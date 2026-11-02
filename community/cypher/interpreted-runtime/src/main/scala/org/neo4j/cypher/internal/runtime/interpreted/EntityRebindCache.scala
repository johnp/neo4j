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

import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship

import java.util

/**
 * Caches transaction-bound entities for the lifetime of one transaction (one batch of
 * `CALL { ... } IN TRANSACTIONS`).
 *
 * The cache is keyed by entity identity. The same entity object is common when a value is copied
 * through several rows, while a procedure may create a fresh wrapper for the same id on every row;
 * identity keys cannot alias two distinct entities that happen to share an id (virtual nodes may
 * share negative ids) and avoid `equals`/`hashCode` work on the hot path.
 *
 * The cache does not evict. Once it reaches `maxSize`, new entities are not cached; entries that
 * are already present keep being served. The bound keeps a large batch from retaining an unbounded
 * number of transaction-bound entities.
 *
 * Keys are assumed to keep their id for the lifetime of the cache. `NodeEntity` is immutable;
 * `RelationshipEntity` exposes a `visit` method that can change its id, but the runtime does not
 * re-visit an entity that is already a cache key. If that ever changes, compare the id on hit.
 */
final private[interpreted] class EntityRebindCache(
  newNodeEntity: Long => Node,
  newRelationshipEntity: Long => Relationship,
  maxSize: Int = EntityRebindCache.DefaultMaxSize
) {

  private[this] val nodeCache = new util.IdentityHashMap[Node, Node]()
  private[this] val relationshipCache = new util.IdentityHashMap[Relationship, Relationship]()

  def node(entity: Node): Node = {
    val cached = nodeCache.get(entity)
    if (cached ne null) {
      cached
    } else {
      val rebound = newNodeEntity(entity.getId)
      if (nodeCache.size() < maxSize) {
        nodeCache.put(entity, rebound)
      }
      rebound
    }
  }

  def relationship(entity: Relationship): Relationship = {
    val cached = relationshipCache.get(entity)
    if (cached ne null) {
      cached
    } else {
      val rebound = newRelationshipEntity(entity.getId)
      if (relationshipCache.size() < maxSize) {
        relationshipCache.put(entity, rebound)
      }
      rebound
    }
  }
}

object EntityRebindCache {
  val DefaultMaxSize = 4096
}
