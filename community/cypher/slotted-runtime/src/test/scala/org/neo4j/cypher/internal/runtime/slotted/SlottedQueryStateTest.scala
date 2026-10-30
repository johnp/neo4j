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
import org.neo4j.values.storable.Values

class SlottedQueryStateTest extends CypherFunSuite {

  private val slots = SlotConfigurationBuilder.empty
    .newLong("a", nullable = false, CTNode)
    .newReference("b", nullable = true, CTAny)
    .build()

  private val rowFactory = SlottedCypherRowFactory(slots, slots.size())

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
}
