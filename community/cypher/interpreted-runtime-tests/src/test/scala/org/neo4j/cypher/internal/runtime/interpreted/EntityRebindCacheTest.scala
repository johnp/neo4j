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
package org.neo4j.cypher.internal.runtime.interpreted

import org.mockito.Mockito.mock
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship

class EntityRebindCacheTest extends CypherFunSuite {

  private def node(id: Long): Node = {
    val entity = mock[Node]
    when(entity.getId).thenReturn(id)
    entity
  }

  private def relationship(id: Long): Relationship = {
    val entity = mock[Relationship]
    when(entity.getId).thenReturn(id)
    entity
  }

  test("reuses a rebound node for the same entity instance") {
    var calls = 0
    val cache = new EntityRebindCache(
      id => {
        calls += 1
        node(id)
      },
      id => relationship(id)
    )

    val entity = node(42L)
    val first = cache.node(entity)
    val second = cache.node(entity)

    first should be theSameInstanceAs second
    calls shouldBe 1
  }

  test("does not reuse across distinct entity instances") {
    var calls = 0
    val cache = new EntityRebindCache(
      id => {
        calls += 1
        node(id)
      },
      id => relationship(id)
    )

    cache.node(node(42L))
    cache.node(node(42L))

    calls shouldBe 2
  }

  test("reuses a rebound relationship for the same entity instance") {
    var calls = 0
    val cache = new EntityRebindCache(
      id => node(id),
      id => {
        calls += 1
        relationship(id)
      }
    )

    val entity = relationship(42L)
    val first = cache.relationship(entity)
    val second = cache.relationship(entity)

    first should be theSameInstanceAs second
    calls shouldBe 1
  }

  test("stops inserting at the cap but keeps serving entries that are already cached") {
    var calls = 0
    val cache = new EntityRebindCache(
      id => {
        calls += 1
        node(id)
      },
      id => relationship(id),
      maxSize = 2
    )

    val first = node(1L)
    val second = node(2L)
    val third = node(3L)

    cache.node(first)
    cache.node(second)
    cache.node(third) // not cached, cap reached
    cache.node(third) // still not cached
    cache.node(first) // still cached

    calls shouldBe 4
  }
}
