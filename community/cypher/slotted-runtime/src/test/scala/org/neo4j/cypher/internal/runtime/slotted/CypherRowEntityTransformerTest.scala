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

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationBuilder
import org.neo4j.cypher.internal.runtime.EntityTransformer
import org.neo4j.cypher.internal.runtime.NoopEntityTransformer
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TransactionPipeWrapper.CypherRowEntityTransformer
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.kernel.impl.util.NodeEntityWrappingNodeValue
import org.neo4j.kernel.impl.util.PathWrappingPathValue
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

class CypherRowEntityTransformerTest extends CypherFunSuite {

  private val slots = SlotConfigurationBuilder.empty
    .newLong("long", nullable = false, CTNode)
    .newReference("ref", nullable = true, CTAny)
    .build()

  private val reboundNode = VirtualValues.node(999L)

  private val reboundPath =
    VirtualValues.pathReference(Array[VirtualNodeValue](VirtualValues.node(1L)), Array.empty[VirtualRelationshipValue])

  private val transformer = new CypherRowEntityTransformer(new RebindToMarkersEntityTransformer)

  test("returns the same row instance if the row contains no entity-wrapping values") {
    val row = SlottedRow(slots)
    row.setLongAt(0, 42L)
    row.setRefAt(0, Values.longValue(42L))

    transformer.copyWithEntityWrappingValuesRebound(row) should be theSameInstanceAs row
  }

  test("copies and rebinds a row containing a node entity") {
    val nodeValue = wrappingNodeValue()
    val row = SlottedRow(slots)
    row.setLongAt(0, 42L)
    row.setRefAt(0, nodeValue)

    val result = transformer.copyWithEntityWrappingValuesRebound(row)

    result should not be theSameInstanceAs(row)
    result.getRefAt(0) shouldBe reboundNode
    result.getLongAt(0) shouldBe 42L
    row.getRefAt(0) shouldBe nodeValue
  }

  test("copies and rebinds a row containing a path") {
    val pathValue = ValueUtils.wrapPath(mock[Path])
    val row = SlottedRow(slots)
    row.setRefAt(0, pathValue)

    val result = transformer.copyWithEntityWrappingValuesRebound(row)

    result should not be theSameInstanceAs(row)
    result.getRefAt(0) shouldBe reboundPath
  }

  test("returns the same row instance for a non-empty map of plain values") {
    val row = SlottedRow(slots)
    row.setRefAt(0, VirtualValues.map(Array("key"), Array[AnyValue](Values.longValue(1L))))

    transformer.copyWithEntityWrappingValuesRebound(row) should be theSameInstanceAs row
  }

  test("copies and rebinds a wrapped entity nested in a map") {
    val row = SlottedRow(slots)
    row.setRefAt(0, VirtualValues.map(Array("key"), Array[AnyValue](wrappingNodeValue())))

    val result = transformer.copyWithEntityWrappingValuesRebound(row)

    result should not be theSameInstanceAs(row)
    result.getRefAt(0) shouldBe VirtualValues.map(Array("key"), Array[AnyValue](reboundNode))
  }

  test("copies and rebinds a wrapped entity nested in a list") {
    val row = SlottedRow(slots)
    row.setRefAt(0, VirtualValues.list(wrappingNodeValue()))

    val result = transformer.copyWithEntityWrappingValuesRebound(row)

    result should not be theSameInstanceAs(row)
    result.getRefAt(0) shouldBe VirtualValues.list(reboundNode)
  }

  test("copies and rebinds a wrapped entity nested two levels deep") {
    val row = SlottedRow(slots)
    row.setRefAt(
      0,
      VirtualValues.map(Array("inner"), Array[AnyValue](VirtualValues.list(wrappingNodeValue())))
    )

    val result = transformer.copyWithEntityWrappingValuesRebound(row)

    result should not be theSameInstanceAs(row)
    result.getRefAt(0) shouldBe
      VirtualValues.map(Array("inner"), Array[AnyValue](VirtualValues.list(reboundNode)))
  }

  test("returns the same row instance for a non-empty integral range") {
    val row = SlottedRow(slots)
    row.setRefAt(0, VirtualValues.range(1L, 10L, 1L))

    transformer.copyWithEntityWrappingValuesRebound(row) should be theSameInstanceAs row
  }

  test("returns the same row instance if the row only contains empty containers") {
    val row = SlottedRow(slots)
    row.setRefAt(0, VirtualValues.EMPTY_MAP)

    transformer.copyWithEntityWrappingValuesRebound(row) should be theSameInstanceAs row
  }

  test("returns the same row instance with a no-op transformer") {
    val noopTransformer = new CypherRowEntityTransformer(new NoopEntityTransformer)
    val row = SlottedRow(slots)
    row.setRefAt(0, wrappingNodeValue())

    noopTransformer.copyWithEntityWrappingValuesRebound(row) should be theSameInstanceAs row
  }

  test("offers no transform when the row contains no entity-wrapping values") {
    val row = SlottedRow(slots)
    row.setRefAt(0, Values.longValue(42L))

    transformer.rebindTransformIfNeeded(row) shouldBe null
  }

  test("offers the rebind transform when the row contains a node entity") {
    val nodeValue = wrappingNodeValue()
    val row = SlottedRow(slots)
    row.setRefAt(0, nodeValue)

    val transform = transformer.rebindTransformIfNeeded(row)

    transform should not be null
    transform(nodeValue) shouldBe reboundNode
  }

  private def wrappingNodeValue(): NodeEntityWrappingNodeValue = {
    val node = mock[Node]
    when(node.getId).thenReturn(-1L)
    ValueUtils.asNodeReference(node) match {
      case nodeValue: NodeEntityWrappingNodeValue => nodeValue
      case other                                  => fail(s"Expected a NodeEntityWrappingNodeValue but got $other")
    }
  }

  private class RebindToMarkersEntityTransformer extends EntityTransformer {

    override def rebindEntityWrappingValue(value: AnyValue): AnyValue = value match {
      case _: NodeEntityWrappingNodeValue => reboundNode
      case _: PathWrappingPathValue       => reboundPath
      case m: MapValue if !m.isEmpty =>
        val builder = new MapValueBuilder(m.size())
        m.foreach((key, v) => builder.add(key, rebindEntityWrappingValue(v)))
        builder.build()
      case l: ListValue if !l.isEmpty =>
        val builder = ListValueBuilder.newListBuilder(l.intSize())
        l.forEach(v => builder.add(rebindEntityWrappingValue(v)))
        builder.build()
      case other => other
    }
  }
}
