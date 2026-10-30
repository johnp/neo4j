/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

/**
 * Iterator that pulls elements lazily from a source and buffers at most one of them.
 *
 * This class is on an allocation-sensitive hot path: one instance sits in front of every
 * distinct, repeat, hash join and concurrent-transaction operator, and every row an operator
 * emits passes through [[produceNext]] and the single-element buffer. The element contract is
 * therefore null-based rather than `Option`-based: [[produceNext]] returns the next element,
 * or `null` when there are no more elements.
 *
 * Do not change [[produceNext]] to return `Option[T]`. Wrapping each row in `Some` allocates an
 * object per emitted row, and because that wrapper escapes into the buffer field the JIT cannot
 * scalar-replace it. That is exactly what caused the allocation storm observed with JFR on
 * `CALL {} IN CONCURRENT TRANSACTIONS` workloads (143 GB of `scala.Some` in 60 s).
 *
 * The pull is lazy by design: [[produceNext]] is only invoked from `innerHasNext`, and `next()`
 * never pulls the element after the one it returns. Keep it that way, otherwise work is done for
 * a row that may never be consumed. Only one element is ever held at a time.
 *
 * @tparam T the element type; must be a reference type whose elements are never `null`, because
 *           `null` is reserved for exhaustion
 */
abstract class PrefetchingIterator[T <: AnyRef] extends ClosingIterator[T] {
  private[this] var buffer: T = _
  private[this] var exhausted: Boolean = false

  /**
   * Produce the next element.
   *
   * @return the next element, or `null` if this iterator is exhausted
   */
  def produceNext(): T

  override def innerHasNext: Boolean = {
    if (buffer == null && !exhausted) {
      buffer = produceNext()
      if (buffer == null) {
        exhausted = true
      }
    }
    buffer != null
  }

  override def next(): T = {
    if (!hasNext)
      throw new NoSuchElementException("next on empty iterator")
    else {
      val current = buffer
      // T is only known to be a reference type, so `null` needs a cast to be assigned back
      buffer = null.asInstanceOf[T]
      current
    }
  }
}
