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
package org.neo4j.cypher.internal.runtime.slotted

import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

class SlottedQueryStateTest extends CypherFunSuite {

  private val slots = SlotConfigurationBuilder.empty
    .newLong("a", nullable = false, CTNode)
    .newReference("b", nullable = true, CTAny)
    .build()

  private val rowFactory = SlottedCypherRowFactory(slots, slots.size())

  // The inner pipeline can have more slots than the argument prefix; those must stay untouched.
  private val largerSlots = SlotConfigurationBuilder.empty
    .newLong("a", nullable = false, CTNode)
    .newReference("b", nullable = true, CTAny)
    .newReference("c", nullable = true, CTAny)
    .build()
  private val largerRowFactory = SlottedCypherRowFactory(largerSlots, slots.size())

  test("newRowWithArgument should copy the current initial context") {
    val state = QueryStateHelper.empty
    val outerRow = SlottedRow(slots)
    outerRow.setLongAt(0, 42L)
    outerRow.setRefAt(0, Values.stringValue("x"))
    state.setInitialContext(outerRow)

    val innerRow = state.newRowWithArgument(rowFactory)

    innerRow should not be theSameInstanceAs(outerRow)
    innerRow.getLongAt(0) shouldBe 42L
    innerRow.getRefAt(0) shouldBe Values.stringValue("x")

    // The inner query may mutate its own row, but must never mutate the outer row
    innerRow.setLongAt(0, 21L)
    innerRow.setRefAt(0, Values.stringValue("y"))
    outerRow.getLongAt(0) shouldBe 42L
    outerRow.getRefAt(0) shouldBe Values.stringValue("x")
  }

  test("consecutive rows of a batch should each see their own initial context") {
    val state = QueryStateHelper.empty
    val firstRow = SlottedRow(slots)
    firstRow.setLongAt(0, 1L)
    val secondRow = SlottedRow(slots)
    secondRow.setLongAt(0, 2L)

    state.setInitialContext(firstRow)
    val firstInnerRow = state.newRowWithArgument(rowFactory)

    // Simulates the next outer row in the same batch
    state.setInitialContext(secondRow)
    val secondInnerRow = state.newRowWithArgument(rowFactory)

    firstInnerRow.getLongAt(0) shouldBe 1L
    secondInnerRow.getLongAt(0) shouldBe 2L
  }

  test("newRowWithArgument fuses the pending transform into the argument copy") {
    val state = QueryStateHelper.empty
    val outer = SlottedRow(slots)
    outer.setLongAt(0, 42L)
    outer.setRefAt(0, Values.longValue(1L))
    state.setInitialContext(
      outer,
      (value: AnyValue) => {
        if (value == null) throw new IllegalStateException("transform must only see copied argument slots")
        if (value == Values.longValue(1L)) Values.longValue(2L) else value
      }
    )

    val row = state.newRowWithArgument(largerRowFactory).asInstanceOf[SlottedRow]

    row should not be theSameInstanceAs(outer)
    row.getLongAt(0) shouldBe 42L
    row.getRefAt(0) shouldBe Values.longValue(2L)
    // The extra inner slot is not part of the argument prefix and must not be transformed
    row.getRefAtWithoutCheckingInitialized(1) shouldBe null
    outer.getRefAt(0) shouldBe Values.longValue(1L)
  }

  test("the pending transform is applied once per outer row, not once per argument copy") {
    val state = QueryStateHelper.empty
    val outer = SlottedRow(slots)
    outer.setLongAt(0, 42L)
    outer.setRefAt(0, Values.longValue(1L))
    var calls = 0
    state.setInitialContext(
      outer,
      (value: AnyValue) => {
        calls += 1
        if (value == Values.longValue(1L)) Values.longValue(2L) else value
      }
    )

    val first = state.newRowWithArgument(rowFactory)
    val second = state.newRowWithArgument(rowFactory)

    calls shouldBe 1
    first.getRefAt(0) shouldBe Values.longValue(2L)
    second.getRefAt(0) shouldBe Values.longValue(2L)
    outer.getRefAt(0) shouldBe Values.longValue(1L)
  }

  test("initialContextRow materializes the transform") {
    val state = QueryStateHelper.empty
    val outer = SlottedRow(slots)
    outer.setLongAt(0, 42L)
    outer.setRefAt(0, Values.longValue(1L))
    state.setInitialContext(
      outer,
      (value: AnyValue) => {
        if (value == Values.longValue(1L)) Values.longValue(2L) else value
      }
    )

    state.initialContextRow.getRefAt(0) shouldBe Values.longValue(2L)
    outer.getRefAt(0) shouldBe Values.longValue(1L)
  }

  test("reading the initial context materializes the transform in a fresh row") {
    val state = QueryStateHelper.empty
    val outer = SlottedRow(slots)
    outer.setLongAt(0, 42L)
    outer.setRefAt(0, Values.longValue(1L))
    state.setInitialContext(
      outer,
      (value: AnyValue) => {
        if (value == Values.longValue(1L)) Values.longValue(2L) else value
      }
    )

    val materialized = state.initialContext.get

    materialized should not be theSameInstanceAs(outer)
    materialized.getRefAt(0) shouldBe Values.longValue(2L)
    outer.getRefAt(0) shouldBe Values.longValue(1L)
  }
}
