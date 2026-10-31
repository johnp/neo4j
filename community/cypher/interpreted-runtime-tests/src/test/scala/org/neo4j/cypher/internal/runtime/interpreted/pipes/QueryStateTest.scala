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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class QueryStateTest extends CypherFunSuite {

  private val rowFactory = CommunityCypherRowFactory()

  test("newRowWithArgument should use the initial context") {
    val state = QueryStateHelper.empty
    state.initialContext shouldBe None

    state.setInitialContext(contextWithValue(1L))
    state.initialContext shouldBe Some(contextWithValue(1L))

    val row = state.newRowWithArgument(rowFactory)
    row.getByName("a") shouldBe Values.longValue(1L)
  }

  test("newRow should use the initial context") {
    val state = QueryStateHelper.empty
    state.setInitialContext(contextWithValue(1L))

    state.newRow(rowFactory).getByName("a") shouldBe Values.longValue(1L)
  }

  test("replacing the initial context does not bleed into rows created before") {
    val state = QueryStateHelper.empty

    state.setInitialContext(contextWithValue(1L))
    val first = state.newRowWithArgument(rowFactory)

    // Simulates the next outer row in the same batch
    state.setInitialContext(contextWithValue(2L))
    val second = state.newRowWithArgument(rowFactory)

    first.getByName("a") shouldBe Values.longValue(1L)
    second.getByName("a") shouldBe Values.longValue(2L)
  }

  test("newRowWithArgument applies a pending transform to every copied argument row") {
    val state = QueryStateHelper.empty
    val outer = contextWithValue(1L)
    state.setInitialContext(
      outer,
      (value: AnyValue) => if (value == Values.longValue(1L)) Values.longValue(2L) else value
    )

    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)
    // Every argument consumer must see rebound values, not just the first one
    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)
    outer.getByName("a") shouldBe Values.longValue(1L)
  }

  test("the pending transform is applied once per outer row, not once per argument copy") {
    val state = QueryStateHelper.empty
    val outer = contextWithValue(1L)
    var calls = 0
    state.setInitialContext(
      outer,
      (value: AnyValue) => {
        calls += 1
        if (value == Values.longValue(1L)) Values.longValue(2L) else value
      }
    )

    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)
    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)

    calls shouldBe 1
    outer.getByName("a") shouldBe Values.longValue(1L)
  }

  test("reading the initial context materializes the pending transform without touching the outer row") {
    val state = QueryStateHelper.empty
    val outer = contextWithValue(1L)
    state.setInitialContext(
      outer,
      (value: AnyValue) => if (value == Values.longValue(1L)) Values.longValue(2L) else value
    )

    state.initialContext.get.getByName("a") shouldBe Values.longValue(2L)
    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)
    outer.getByName("a") shouldBe Values.longValue(1L)
  }

  test("newRow applies the pending transform") {
    val state = QueryStateHelper.empty
    state.setInitialContext(
      contextWithValue(1L),
      (value: AnyValue) => if (value == Values.longValue(1L)) Values.longValue(2L) else value
    )

    state.newRow(rowFactory).getByName("a") shouldBe Values.longValue(2L)
  }

  test("a non-owned initial context is always copied") {
    val state = QueryStateHelper.empty
    val shared = contextWithValue(1L)
    state.setInitialContext(shared, null)

    state.newRowWithArgument(rowFactory) should not be theSameInstanceAs(shared)
    state.newRowWithArgument(rowFactory) should not be theSameInstanceAs(shared)
  }

  test("states created before the initial context changes keep their own context") {
    val state = QueryStateHelper.empty
    state.setInitialContext(contextWithValue(1L))

    val derived = state.withInitialContext(contextWithValue(2L))
    state.setInitialContext(contextWithValue(3L))

    derived.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(2L)
    state.newRowWithArgument(rowFactory).getByName("a") shouldBe Values.longValue(3L)
  }

  private def contextWithValue(value: Long): CypherRow =
    CypherRow.from("a" -> Values.longValue(value))
}
