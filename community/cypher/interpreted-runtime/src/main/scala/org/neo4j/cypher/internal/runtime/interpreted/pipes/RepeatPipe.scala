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

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingCollections.newArrayDeque
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.runtime.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.AcyclicModeConstraint
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.AllReduceAcc
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.TrailModeConstraint
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.TraversalModeConstraint
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.WalkModeConstraint
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RepeatPipe.emptyLists
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.VirtualValue
import org.neo4j.values.virtual.IdentifiedVirtualValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

import scala.annotation.tailrec
import scala.reflect.ClassTag

sealed trait LegacyRepeatState {
  val endNode: Long
  val groupNodes: HeapTrackingArrayList[ListValue]
  val groupRelationships: HeapTrackingArrayList[ListValue]
  val iterations: Int
  val accumulatorValues: Array[AnyValue]
  def close(): Unit
}

case class AcyclicLegacyRepeatState(
  endNode: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  iterations: Int,
  closeGroupsOnClose: Boolean,
  constraint: AcyclicModeConstraint,
  relationshipsSeen: HeapTrackingLongHashSet,
  nodesSeen: HeapTrackingLongHashSet,
  accumulatorValues: Array[AnyValue]
) extends LegacyRepeatState {

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
    relationshipsSeen.close()
    nodesSeen.close()
  }
}

case class TrailLegacyRepeatState(
  endNode: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  iterations: Int,
  closeGroupsOnClose: Boolean,
  constraint: TrailModeConstraint,
  relationshipsSeen: HeapTrackingLongHashSet,
  accumulatorValues: Array[AnyValue]
) extends LegacyRepeatState {

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
    relationshipsSeen.close()
  }
}

case class WalkLegacyRepeatState(
  endNode: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  iterations: Int,
  closeGroupsOnClose: Boolean,
  accumulatorValues: Array[AnyValue]
) extends LegacyRepeatState {

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
  }
}

case class RepeatPipe(
  source: Pipe,
  inner: Pipe,
  repetition: Repetition,
  start: String,
  end: String,
  innerStart: String,
  innerEnd: String,
  groupNodes: Set[VariableGrouping],
  groupRelationships: Set[VariableGrouping],
  uniquenessConstraint: TraversalModeConstraint,
  reverseGroupVariableProjections: Boolean,
  nodeInScope: Boolean,
  accumulatorMappings: Array[AllReduceAcc]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private val groupNodeNames = groupNodes.toArray.sortBy(_.singleton.name)
  private val groupRelationshipNames = groupRelationships.toArray.sortBy(_.singleton.name)
  private val emptyGroupNodes = emptyLists(groupNodes.size)
  private val emptyGroupRelationships = emptyLists(groupRelationships.size)
  private val sortedAccumulators = accumulatorMappings.sortBy(_.previous)
  private val sortedPreviousAccumulatorNames = sortedAccumulators.map(_.previous)

  private def createNewState(
    outerRow: CypherRow,
    startNode: VirtualNodeValue,
    tracker: MemoryTracker,
    state: QueryState
  ): LegacyRepeatState = {
    val initialAccumulatorValues = sortedAccumulators.map(_.initial(outerRow, state))
    uniquenessConstraint match {
      case constraint @ RepeatPipe.TrailModeConstraint(
          _,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups
        ) =>
        val relationshipsSeen =
          createSeen[VirtualRelationshipValue](
            previouslyBoundRelationships,
            previouslyBoundRelationshipGroups,
            outerRow,
            tracker
          )
        TrailLegacyRepeatState(
          startNode.id(),
          emptyGroupNodes,
          emptyGroupRelationships,
          iterations = 1,
          closeGroupsOnClose = false,
          constraint,
          relationshipsSeen,
          initialAccumulatorValues
        )
      case WalkModeConstraint =>
        WalkLegacyRepeatState(
          startNode.id(),
          emptyGroupNodes,
          emptyGroupRelationships,
          iterations = 1,
          closeGroupsOnClose = false,
          initialAccumulatorValues
        )
      case constraint @ AcyclicModeConstraint(
          _,
          _,
          previouslyBoundRelationships,
          previouslyBoundRelationshipGroups,
          previouslyBoundNodes,
          previouslyBoundNodeGroups
        ) =>
        val relationshipsSeen =
          createSeen[VirtualRelationshipValue](
            previouslyBoundRelationships,
            previouslyBoundRelationshipGroups,
            outerRow,
            tracker
          )
        val nodesSeen =
          createSeen[VirtualNodeValue](previouslyBoundNodes, previouslyBoundNodeGroups, outerRow, tracker)

        AcyclicLegacyRepeatState(
          startNode.id(),
          emptyGroupNodes,
          emptyGroupRelationships,
          iterations = 1,
          closeGroupsOnClose = false,
          constraint,
          relationshipsSeen,
          nodesSeen,
          initialAccumulatorValues
        )
    }
  }

  private def createSeen[V <: VirtualValue with IdentifiedVirtualValue : ClassTag](
    previouslyBound: Set[String],
    previouslyBoundGroups: Set[String],
    outerRow: CypherRow,
    tracker: MemoryTracker
  ): HeapTrackingLongHashSet = {
    val seen = HeapTrackingCollections.newLongSet(tracker)
    val ir = previouslyBound.iterator
    while (ir.hasNext) {
      seen.add(castOrFail[V](outerRow.getByName(ir.next())).id())
    }
    val ig = previouslyBoundGroups.iterator
    while (ig.hasNext) {
      val i = castOrFail[ListValue](outerRow.getByName(ig.next())).iterator()
      while (i.hasNext) {
        seen.add(castOrFail[V](i.next()).id())
      }
    }
    seen
  }

  private def maybeCreateNextState(
    repeatState: LegacyRepeatState,
    row: CypherRow,
    innerEndNode: VirtualNodeValue,
    newGroupNodes: HeapTrackingArrayList[ListValue],
    newGroupRels: HeapTrackingArrayList[ListValue],
    tracker: MemoryTracker
  ): Option[LegacyRepeatState] = {
    repeatState match {
      case trailState: TrailLegacyRepeatState =>
        val newSet = HeapTrackingCollections.newLongSet(tracker, trailState.relationshipsSeen)
        val innerRelationshipsArray = trailState.constraint.innerRelationships

        var allRelationshipsUnique = true
        var i = 0
        while (allRelationshipsUnique && i < innerRelationshipsArray.length) {
          val r = innerRelationshipsArray(i)
          allRelationshipsUnique = newSet.add(castOrFail[VirtualRelationshipValue](row.getByName(r)).id())
          i += 1
        }

        if (allRelationshipsUnique) {
          val accumulatorValues = sortedAccumulators.map(acc => row.getByName(acc.next))
          Some(TrailLegacyRepeatState(
            innerEndNode.id(),
            newGroupNodes,
            newGroupRels,
            trailState.iterations + 1,
            closeGroupsOnClose = true,
            trailState.constraint,
            newSet,
            accumulatorValues
          ))
        } else None
      case acyclicState: AcyclicLegacyRepeatState =>
        val newNodes = HeapTrackingCollections.newLongSet(tracker, acyclicState.nodesSeen)
        val innerNodesArray = acyclicState.constraint.innerNodes

        var allNodesUnique = true
        var i = 1
        while (allNodesUnique && i < innerNodesArray.length) {
          val n = innerNodesArray(i)
          allNodesUnique = newNodes.add(
            castOrFail[VirtualNodeValue](row.getByName(n)).id()
          )
          i += 1
        }

        val newRelationships = HeapTrackingCollections.newLongSet(
          tracker,
          acyclicState.relationshipsSeen
        )
        val innerRelationshipsArray = acyclicState.constraint.innerRelationships

        var allRelationshipsUnique = true
        i = 0
        while (i < innerRelationshipsArray.length) {
          val r = innerRelationshipsArray(i)
          allRelationshipsUnique = newRelationships.add(castOrFail[VirtualRelationshipValue](row.getByName(r)).id())
          i += 1
        }

        if (allNodesUnique && allRelationshipsUnique) {
          val accumulatorValues = sortedAccumulators.map(acc => row.getByName(acc.next))
          Some(AcyclicLegacyRepeatState(
            innerEndNode.id(),
            newGroupNodes,
            newGroupRels,
            acyclicState.iterations + 1,
            closeGroupsOnClose = true,
            acyclicState.constraint,
            newRelationships,
            newNodes,
            accumulatorValues
          ))
        } else None
      case walkState: WalkLegacyRepeatState =>
        val accumulatorValues = sortedAccumulators.map(acc => row.getByName(acc.next))
        Some(WalkLegacyRepeatState(
          innerEndNode.id(),
          newGroupNodes,
          newGroupRels,
          walkState.iterations + 1,
          closeGroupsOnClose = true,
          accumulatorValues
        ))
    }
  }

  private def filterRow(row: CypherRow, repeatState: LegacyRepeatState): Boolean =
    repeatState match {
      case acyclicState: AcyclicLegacyRepeatState =>
        val innerRelationshipsArray = acyclicState.constraint.innerRelationships
        var relationshipsAreUnique = true
        var i = 0
        val innerRelationshipsSeen = collection.mutable.Set[Long]()
        while (relationshipsAreUnique && i < innerRelationshipsArray.length) {
          val r = innerRelationshipsArray(i)
          val rel = castOrFail[VirtualRelationshipValue](row.getByName(r)).id()
          relationshipsAreUnique = !acyclicState.relationshipsSeen.contains(rel) && innerRelationshipsSeen.add(rel)
          i += 1
        }
        val innerNodesArray = acyclicState.constraint.innerNodes
        var nodesAreUnique = true
        i = 0
        val innerNodesSeen = collection.mutable.Set[Long]()
        while (nodesAreUnique && i < innerNodesArray.length) {
          val n = innerNodesArray(i)
          val node = castOrFail[VirtualNodeValue](row.getByName(n)).id()
          val uniqueInnerNode = innerNodesSeen.add(
            node
          )
          nodesAreUnique =
            (!acyclicState.nodesSeen.contains(
              node
            )) && uniqueInnerNode || node == acyclicState.endNode && uniqueInnerNode // previous endNode is now startNode
          i += 1
        }
        relationshipsAreUnique && nodesAreUnique
      case trailState: TrailLegacyRepeatState =>
        val innerRelationshipsArray = trailState.constraint.innerRelationships
        var relationshipsAreUnique = true
        var i = 0
        val innerRelationshipsSeen = collection.mutable.Set[Long]()
        while (relationshipsAreUnique && i < innerRelationshipsArray.length) {
          val r = innerRelationshipsArray(i)
          val rel = castOrFail[VirtualRelationshipValue](row.getByName(r)).id()
          relationshipsAreUnique = !trailState.relationshipsSeen.contains(rel) && innerRelationshipsSeen.add(rel)
          i += 1
        }
        relationshipsAreUnique
      case _: WalkLegacyRepeatState =>
        true
    }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap { outerRow =>
      outerRow.getByName(start) match {
        case startNode: VirtualNodeValue =>
          val stack = newArrayDeque[LegacyRepeatState](tracker)
          if (repetition.max.isGreaterThan(0)) {
            stack.push(createNewState(outerRow, startNode, tracker, state))
          }
          new PrefetchingIterator[CypherRow] {
            private var innerResult: ClosingIterator[CypherRow] = ClosingIterator.empty
            private var stackHead: LegacyRepeatState = _
            private var emitFirst = repetition.min == 0

            private def allocateZeroRepetitionResultRowOrNull(): CypherRow = {
              if (emitFirst) {
                emitFirst = false
                val resultRow =
                  outerRow.copyWith(computeNewEntries(emptyGroupNodes, emptyGroupRelationships, startNode, Array.empty))
                if (testEndNode(resultRow, startNode)) {
                  resultRow
                } else {
                  null
                }
              } else {
                null
              }
            }

            override protected[this] def closeMore(): Unit = {
              if (stackHead != null) {
                stackHead.close()
              }
              innerResult.close()
              stack.close()
            }

            @tailrec
            def produceNext(): CypherRow = {
              val firstRowOrNull = allocateZeroRepetitionResultRowOrNull()
              if (firstRowOrNull != null) {
                firstRowOrNull
              } else if (innerResult.hasNext) {
                val row = innerResult.next()
                val innerEndNode = castOrFail[VirtualNodeValue](row.getByName(innerEnd))
                val newGroupNodes = computeGroupVariables(groupNodeNames, stackHead.groupNodes, row, tracker)
                val newGroupRels =
                  computeGroupVariables(groupRelationshipNames, stackHead.groupRelationships, row, tracker)
                if (repetition.max.isGreaterThan(stackHead.iterations)) {
                  maybeCreateNextState(stackHead, row, innerEndNode, newGroupNodes, newGroupRels, tracker)
                    .foreach(stack.push)
                }
                // if iterated long enough emit, otherwise recurse
                if (stackHead.iterations >= repetition.min && testEndNode(row, innerEndNode)) {
                  val resultRow = row.copyWith(computeNewEntries(
                    newGroupNodes,
                    newGroupRels,
                    innerEndNode,
                    stackHead.accumulatorValues
                  ))
                  resultRow
                } else {
                  produceNext()
                }
              } else if (!stack.isEmpty) {
                // close previous state
                if (stackHead != null) {
                  stackHead.close()
                }
                // Run RHS with previous end-node as new innerStartNode
                stackHead = stack.pop()
                outerRow.set(innerStart, VirtualValues.node(stackHead.endNode))

                // Set initial accumulator values
                var i = 0
                while (i < sortedPreviousAccumulatorNames.length) {
                  outerRow.set(sortedPreviousAccumulatorNames(i), stackHead.accumulatorValues(i))
                  i += 1
                }
                val innerState = state.withInitialContext(outerRow)
                innerResult = inner.createResults(innerState).filter(filterRow(_, stackHead))
                produceNext()
              } else {
                null
              }
            }
          }

        case IsNoValue() => ClosingIterator.empty
        case value =>
          throw CypherTypeException.expectedNodeButGot(
            value.prettyPrint(),
            value.getTypeName,
            CypherTypeValueMapper.valueType(value)
          )
      }
    }
  }

  private def computeGroupVariables(
    groupNames: Array[VariableGrouping],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: CypherRow,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupNames.length, tracker)
    var i = 0
    while (i < groupNames.length) {
      res.add(groupVariables.get(i).append(row.getByName(groupNames(i).singleton.name)))
      i += 1
    }
    res
  }

  private def computeNewEntries(
    newGroupNodes: HeapTrackingArrayList[ListValue],
    newGroupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: VirtualNodeValue,
    accumulatorValues: Array[AnyValue]
  ): collection.Seq[(String, AnyValue)] = {
    val newSize = (if (!nodeInScope) 1 else 0) + newGroupNodes.size() + newGroupRels.size() + accumulatorValues.length
    val res = new Array[(String, AnyValue)](newSize)
    var i = 0
    while (i < newGroupNodes.size()) {
      val groupNodes = newGroupNodes.get(i)
      val projectedGroupNodes = if (reverseGroupVariableProjections) groupNodes.reverse() else groupNodes
      res(i) = (groupNodeNames(i).group.name, projectedGroupNodes)
      i += 1
    }

    var j = 0
    while (j < newGroupRels.size()) {
      val groupRels = newGroupRels.get(j)
      val projectedGroupRels = if (reverseGroupVariableProjections) groupRels.reverse() else groupRels
      res(i) = (groupRelationshipNames(j).group.name, projectedGroupRels)
      j += 1
      i += 1
    }

    var l = 0
    while (l < accumulatorValues.length) {
      res(i) = (sortedPreviousAccumulatorNames(l), accumulatorValues(l))
      i += 1
      l += 1
    }

    if (!nodeInScope) {
      res(i) = (end, innerEndNode)
    }
    res
  }

  private def testEndNode(row: CypherRow, endNode: VirtualNodeValue): Boolean = {
    !nodeInScope || {
      row.getByName(end) match {
        case toNode: VirtualNodeValue =>
          endNode.id == toNode.id
        case _ =>
          false
      }
    }
  }
}

object RepeatPipe {

  case class AllReduceAcc(initial: Expression, previous: String, next: String)

  def emptyLists(size: Int): HeapTrackingArrayList[ListValue] = {
    val emptyList = HeapTrackingCollections.newArrayList[ListValue](size, EmptyMemoryTracker.INSTANCE)
    (0 until size).foreach(_ => emptyList.add(EMPTY_LIST))
    emptyList
  }

  sealed trait TraversalModeConstraint

  case class TrailModeConstraint(
    innerRelationships: Array[String],
    previouslyBoundRelationships: Set[String],
    previouslyBoundRelationshipGroups: Set[String]
  ) extends TraversalModeConstraint

  case class AcyclicModeConstraint(
    innerRelationships: Array[String],
    innerNodes: Array[String],
    previouslyBoundRelationships: Set[String],
    previouslyBoundRelationshipGroups: Set[String],
    previouslyBoundNodes: Set[String],
    previouslyBoundNodeGroups: Set[String]
  ) extends TraversalModeConstraint

  case object WalkModeConstraint extends TraversalModeConstraint
}
