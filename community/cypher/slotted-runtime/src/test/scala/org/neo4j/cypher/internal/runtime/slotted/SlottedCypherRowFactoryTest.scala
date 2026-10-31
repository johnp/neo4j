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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values

class SlottedCypherRowFactoryTest extends CypherFunSuite {

  private val slots = SlotConfigurationBuilder.empty
    .newLong("long", nullable = false, CTNode)
    .newReference("ref", nullable = true, CTAny)
    .build()

  private val factory = SlottedCypherRowFactory(slots, slots.size())

  test("copyArgumentOf gives the inner plan its own row and leaves the outer row untouched") {
    val outer = SlottedRow(slots)
    outer.setLongAt(0, 42L)
    outer.setRefAt(0, Values.stringValue("outer"))

    val inner = factory.copyArgumentOf(outer)

    inner should not be theSameInstanceAs(outer)
    inner.getLongAt(0) shouldBe 42L
    inner.getRefAt(0) shouldBe Values.stringValue("outer")

    inner.setLongAt(0, 21L)
    inner.setRefAt(0, Values.stringValue("inner"))

    outer.getLongAt(0) shouldBe 42L
    outer.getRefAt(0) shouldBe Values.stringValue("outer")
  }
}
