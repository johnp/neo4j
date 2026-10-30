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

class PrefetchingIteratorTest extends RuntimeUtilTestSuite {

  test("should not pull before hasNext is called") {
    val iterator = new TestPrefetchingIterator("a", "b")

    iterator.pullCount shouldBe 0
    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 1
  }

  test("should not pull the next element from next()") {
    val iterator = new TestPrefetchingIterator("a", "b")

    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 1

    iterator.next() shouldBe "a"
    iterator.pullCount shouldBe 1

    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 2

    iterator.next() shouldBe "b"
    iterator.pullCount shouldBe 2
  }

  test("should be idempotent in hasNext") {
    val iterator = new TestPrefetchingIterator("a", "b")

    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 1

    iterator.hasNext shouldBe true
    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 1

    iterator.next() shouldBe "a"
    iterator.hasNext shouldBe true
    iterator.pullCount shouldBe 2
  }

  test("next without hasNext should return the first element") {
    val iterator = new TestPrefetchingIterator("a", "b")

    iterator.next() shouldBe "a"
    iterator.pullCount shouldBe 1

    iterator.next() shouldBe "b"
    iterator.pullCount shouldBe 2
  }

  test("should report exhaustion and not pull again") {
    val iterator = new TestPrefetchingIterator("a")

    iterator.hasNext shouldBe true
    iterator.next() shouldBe "a"
    iterator.pullCount shouldBe 1

    iterator.hasNext shouldBe false
    iterator.pullCount shouldBe 2

    iterator.hasNext shouldBe false
    iterator.pullCount shouldBe 2
  }

  test("should throw NoSuchElementException when next is called on an exhausted iterator") {
    val iterator = new TestPrefetchingIterator("a")

    iterator.next() shouldBe "a"
    iterator.hasNext shouldBe false

    an[NoSuchElementException] should be thrownBy iterator.next()
    iterator.pullCount shouldBe 2

    an[NoSuchElementException] should be thrownBy iterator.next()
    iterator.pullCount shouldBe 2
  }

  test("empty iterator should be exhausted after a single pull") {
    val iterator = new TestPrefetchingIterator()

    iterator.hasNext shouldBe false
    iterator.pullCount shouldBe 1

    iterator.hasNext shouldBe false
    iterator.pullCount shouldBe 1
  }

  test("should not close before exhaustion") {
    val iterator = new TestPrefetchingIterator("a", "b")

    iterator.hasNext shouldBe true
    iterator.next() shouldBe "a"
    iterator.hasNext shouldBe true
    iterator.next() shouldBe "b"
    iterator.closeCount shouldBe 0

    iterator.hasNext shouldBe false
    iterator.closeCount shouldBe 1
  }

  test("should close exactly once on exhaustion") {
    val iterator = new TestPrefetchingIterator("a")

    iterator.next() shouldBe "a"
    iterator.closeCount shouldBe 0

    iterator.hasNext shouldBe false
    iterator.closeCount shouldBe 1

    iterator.hasNext shouldBe false
    iterator.close()
    iterator.closeCount shouldBe 1
  }

  private class TestPrefetchingIterator(values: String*) extends PrefetchingIterator[String] {
    private var index = 0
    private var pulls = 0
    private var closes = 0

    def pullCount: Int = pulls
    def closeCount: Int = closes

    override def produceNext(): String = {
      pulls += 1
      if (index < values.length) {
        val value = values(index)
        index += 1
        value
      } else {
        null
      }
    }

    override protected[this] def closeMore(): Unit = closes += 1
  }
}
