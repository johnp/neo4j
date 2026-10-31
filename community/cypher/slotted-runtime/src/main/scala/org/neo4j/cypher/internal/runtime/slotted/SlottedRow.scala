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

import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.macros.AssertMacros3.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.physicalplanning.LongSlot
import org.neo4j.cypher.internal.physicalplanning.RefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotAccessor
import org.neo4j.cypher.internal.physicalplanning.SlotAccessor.nullableNode
import org.neo4j.cypher.internal.physicalplanning.SlotAccessor.nullableRel
import org.neo4j.cypher.internal.physicalplanning.SlotAllocation.LOAD_CSV_METADATA_KEY
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.ApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.CachedPropertySlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.DuplicatedSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.KeyedSlot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.MetaDataSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.OuterNestedApplyPlanSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.PlanningSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.SlotWithKeyAndAliases
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.VariableSlotKey
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.PRIMITIVE_NULL
import org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNonNullLongSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNonNullRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNullableLongSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.NodeNullableRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNonNullRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.OtherNullableRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.RelNonNullLongSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.RelNonNullRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.RelNullableLongSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.RelNullableRefSlot
import org.neo4j.cypher.internal.physicalplanning.SlotType.isNullable
import org.neo4j.cypher.internal.physicalplanning.SlotType.isRefSlot
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.EntityById
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.RuntimeMetadataValue
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker.entityIsNull
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.NotFoundException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.util.VisibleForTesting
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues.node
import org.neo4j.values.virtual.VirtualValues.relationship

object SlottedRow {
  final val INSTANCE_SIZE = shallowSizeOfInstance(classOf[SlottedRow])

  final val DEBUG = false

  def getNodeId(row: ReadableRow, offset: Int, isReference: Boolean): Long =
    if (isReference) {
      row.getRefAt(offset) match {
        case node: VirtualNodeValue => node.id()
        case Values.NO_VALUE        => StatementConstants.NO_SUCH_NODE
        case null =>
          throw CypherTypeException.expectedNodeButGotNull()
        case value: Value =>
          throw CypherTypeException.expectedANodeButGot(
            value.prettyPrint(),
            value.getTypeName,
            CypherTypeValueMapper.valueType(value)
          )
        case other =>
          throw CypherTypeException.expectedANodeButGot(
            String.valueOf(other),
            other.getTypeName,
            CypherTypeValueMapper.valueType(other)
          )
      }
    } else {
      row.getLongAt(offset)
    }

  def getLinenumber(row: ReadableRow, slots: SlotConfiguration): Option[ResourceLinenumber] =
    slots.getMetaDataSlot(LOAD_CSV_METADATA_KEY) match {
      case Some(RefSlot(offset, _, _)) => row.getRefAt(offset) match {
          case RuntimeMetadataValue(l: ResourceLinenumber) => Some(l)
          case Values.NO_VALUE                             => None
          case null                                        => None
          case _ => throw InternalException.internalError(this.getClass.getSimpleName, "Wrong type of linenumber")
        }

      case _ =>
        None
    }
}

trait SlottedCompatible {
  def copyAllToSlottedRow(target: SlottedRow): Unit
  def copyLongsToSlottedRow(target: SlottedRow, fromOffset: Int, toOffset: Int, length: Int): Unit
  def copyRefsToSlottedRow(target: SlottedRow, fromOffset: Int, toOffset: Int, length: Int): Unit
}

/**
 * Execution context which uses a slot configuration to store values in two arrays.
 *
 * @param slots the slot configuration to use.
 */
//noinspection NameBooleanParameters
final case class SlottedRow(slots: SlotConfiguration) extends CypherRow {

  val longs = new Array[Long](slots.numberOfLongs)
  // java.util.Arrays.fill(longs, -2L) // When debugging long slot issues you can uncomment this to check for uninitialized long slots (also in getLongAt below)
  val refs = new Array[AnyValue](slots.numberOfReferences)

  override def toString: String = {
    val iter = this.iterator
    val s: StringBuilder = StringBuilder.newBuilder
    s ++= s"\nSlottedExecutionContext {\n    $slots"
    while (iter.hasNext) {
      val slotValue = iter.next
      s ++= f"\n    ${slotValue._1}%-40s = ${slotValue._2}"
    }
    s ++= "\n}\n"
    s.result
  }

  override def copyAllFrom(input: ReadableRow): Unit = input match {
    case other: SlottedRow        => copyFromSlotted(other, 0, 0, 0, 0)
    case other: SlottedCompatible => other.copyAllToSlottedRow(this)
    case _                        => fail()
  }

  override def copyFrom(input: ReadableRow, nLongs: Int, nRefs: Int): Unit =
    if (nLongs > slots.numberOfLongs || nRefs > slots.numberOfReferences)
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        "A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy."
      )
    else input match {
      case other @ SlottedRow(_) =>
        System.arraycopy(other.longs, 0, longs, 0, nLongs)
        System.arraycopy(other.refs, 0, refs, 0, nRefs)

      case other: SlottedCompatible =>
        other.copyLongsToSlottedRow(this, 0, 0, nLongs)
        other.copyRefsToSlottedRow(this, 0, 0, nRefs)

      case _ => fail()
    }

  override def copyLongsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit = {
    input match {
      case from: SlottedRow        => System.arraycopy(from.longs, fromOffset, longs, toOffset, length)
      case from: SlottedCompatible => from.copyLongsToSlottedRow(this, fromOffset, toOffset, length)
      case _                       => fail()
    }
  }

  override def copyRefsFrom(input: ReadableRow, fromOffset: Int, toOffset: Int, length: Int): Unit = {
    input match {
      case from: SlottedRow        => System.arraycopy(from.refs, fromOffset, refs, toOffset, length)
      case from: SlottedCompatible => from.copyRefsToSlottedRow(this, fromOffset, toOffset, length)
      case _                       => fail()
    }
  }

  def copyMapped(func: AnyValue => AnyValue): CypherRow = {
    val clone = SlottedRow(slots)
    clone.copyAllFrom(this)
    clone.transformRefs(func)
    clone
  }

  override def anyRefValueSatisfies(predicate: AnyValue => Boolean): Boolean = {
    var i = 0
    while (i < refs.length) {
      val value = refs(i)
      if (value != null && predicate(value)) {
        return true
      }
      i += 1
    }
    false
  }

  private def transformRefs(func: AnyValue => AnyValue): Unit = {
    val allSlots = slots.slots
    var i = 0
    while (i < allSlots.length) {
      val slot = allSlots(i)
      if (isRefSlot(slot.slotType)) setRefAt(slot.offset, func(getRefAt(slot.offset)))
      i += 1
    }
  }

  /**
   * Applies `func` to the first `nRefs` reference slots in place. Used when a row was just created
   * by copying an argument prefix; the remaining slots are still uninitialized.
   */
  private[slotted] def transformRefsInPlace(func: AnyValue => AnyValue, nRefs: Int): Unit = {
    var i = 0
    while (i < nRefs) {
      refs(i) = func(refs(i))
      i += 1
    }
  }

  override def copyFromOffset(
    input: ReadableRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit =
    input match {
      case other: SlottedRow =>
        copyFromSlotted(other, sourceLongOffset, sourceRefOffset, targetLongOffset, targetRefOffset)
      case _ => fail()
    }

  private def copyFromSlotted(
    other: SlottedRow,
    sourceLongOffset: Int,
    sourceRefOffset: Int,
    targetLongOffset: Int,
    targetRefOffset: Int
  ): Unit = {
    val otherPipeline = other.slots
    if (
      otherPipeline.numberOfLongs > slots.numberOfLongs ||
      otherPipeline.numberOfReferences > slots.numberOfReferences
    ) {
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        s"""A bug has occurred in the slotted runtime: The target slotted execution context cannot hold the data to copy
           |From : $otherPipeline
           |To :   $slots""".stripMargin
      )
    } else {
      System.arraycopy(
        other.longs,
        sourceLongOffset,
        longs,
        targetLongOffset,
        other.slots.numberOfLongs - sourceLongOffset
      )
      System.arraycopy(
        other.refs,
        sourceRefOffset,
        refs,
        targetRefOffset,
        other.slots.numberOfReferences - sourceRefOffset
      )
    }
  }

  override def setLongAt(offset: Int, value: Long): Unit =
    longs(offset) = value

  override def getLongAt(offset: Int): Long =
    longs(offset)
  // When debugging long slot issues you can uncomment and replace with this to check for uninitialized long slots
  //  {
  //    val value = longs(offset)
  //    if (value == -2L)
  //      throw new InternalException(s"Long value not initialised at offset $offset in $this")
  //    value
  //  }

  override def setRefAt(offset: Int, value: AnyValue): Unit = refs(offset) = value

  override def getRefAt(offset: Int): AnyValue = {
    val value = refs(offset)
    if (SlottedRow.DEBUG && value == null)
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        s"Reference value not initialised at offset $offset in $this"
      )
    value
  }

  override def getByName(name: String): AnyValue =
    SlotAccessor.getValue(
      this,
      slots.getOrElse(VariableSlotKey(name), throw new NotFoundException(s"Unknown variable `$name`"))
    )

  override def numberOfColumns: Int = refs.length + longs.length

  override def containsName(name: String): Boolean = getByName(name) != null

  override def setCachedPropertyAt(offset: Int, value: Value): Unit = refs(offset) = value

  override def invalidateCachedProperties(): Unit = {
    val offsets = slots.cachedPropertyOffsets
    var i = 0
    while (i < offsets.length) {
      setCachedPropertyAt(offsets(i), null)
      i += 1
    }
  }

  override def invalidateCachedNodeProperties(node: Long): Unit =
    SlotAccessor.invalidateCachedNodeProperties(this, slots, node)

  override def invalidateCachedRelationshipProperties(rel: Long): Unit =
    SlotAccessor.invalidateCachedRelProperties(this, slots, rel)

  override def setCachedProperty(key: ASTCachedProperty.RuntimeKey, value: Value): Unit =
    setCachedPropertyAt(slots.cachedPropOffset(key), value)

  override def getCachedPropertyAt(offset: Int): Value = refs(offset).asInstanceOf[Value]

  override def getCachedProperty(key: ASTCachedProperty.RuntimeKey): Value = fail()

  override def estimatedHeapUsage: Long = {
    var usage = SlottedRow.INSTANCE_SIZE + HeapEstimator.sizeOf(longs) + HeapEstimator.shallowSizeOf(
      refs.asInstanceOf[Array[Object]]
    )
    var i = 0
    while (i < refs.length) {
      val ref = refs(i)
      if (ref != null) {
        usage += ref.estimatedHeapUsage()
      }
      i += 1
    }
    usage
  }

  override def deduplicatedEstimatedHeapUsage(previous: CypherRow): Long = {
    if (previous eq null) {
      estimatedHeapUsage
    } else {
      var usage = SlottedRow.INSTANCE_SIZE + HeapEstimator.sizeOf(longs) + HeapEstimator.shallowSizeOf(
        refs.asInstanceOf[Array[Object]]
      )
      var i = 0
      while (i < refs.length) {
        val ref = refs(i)
        if ((ref ne null) && (ref ne previous.getRefAt(i))) {
          usage += ref.estimatedHeapUsage()
        }
        i += 1
      }
      usage
    }
  }

  private def fail(): Nothing =
    throw InternalException.internalError(this.getClass.getSimpleName, "Tried using a slotted context as a map")

  // -----------------------------------------------------------------------------------------------------------
  // Compatibility implementations of the old ExecutionContext API used by Community interpreted runtime pipes
  // -----------------------------------------------------------------------------------------------------------

  override def setLinenumber(line: Option[ResourceLinenumber]): Unit = fail()

  override def getLinenumber: Option[ResourceLinenumber] = {
    SlottedRow.getLinenumber(this, slots)
  }

  // The newWith methods are called from Community pipes. We should already have allocated slots for the given keys,
  // so we just set the values in the existing slots instead of creating a new context like in the MapExecutionContext.
  override def set(newEntries: collection.Seq[(String, AnyValue)]): Unit =
    newEntries.foreach {
      case (k, v) =>
        set(k, v)
    }

  override def set(key: String, value: AnyValue): Unit = SlotAccessor.setValue(this, slots(VariableSlotKey(key)), value)

  override def set(key1: String, value1: AnyValue, key2: String, value2: AnyValue): Unit = {
    set(key1, value1)
    set(key2, value2)
  }

  override def set(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): Unit = {
    set(key1, value1)
    set(key2, value2)
    set(key3, value3)
  }

  override def copyWith(key1: String, value1: AnyValue): CypherRow = {
    // This method should throw like its siblings below as soon as reduce is changed to not use it.
    val newCopy = SlottedRow(slots)
    newCopy.copyAllFrom(this)
    newCopy.set(key1, value1)
    newCopy
  }

  override def copyWith(key1: String, value1: AnyValue, key2: String, value2: AnyValue): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(
    key1: String,
    value1: AnyValue,
    key2: String,
    value2: AnyValue,
    key3: String,
    value3: AnyValue
  ): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  override def copyWith(newEntries: collection.Seq[(String, AnyValue)]): CypherRow = {
    throw new UnsupportedOperationException(
      "Use ExecutionContextFactory.copyWith instead to get the correct slot configuration"
    )
  }

  def isRefInitialized(offset: Int): Boolean = {
    refs(offset) != null
  }

  def getRefAtWithoutCheckingInitialized(offset: Int): AnyValue =
    refs(offset)

  @VisibleForTesting
  private[slotted] def setPrimitiveNode(key: SlotKey, value: Long): Unit = {
    val slot = slots(key)
    val offset = slot.offset
    slot.slotType match {
      case NodeNonNullLongSlot =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setLongAt(offset, value)
      case NodeNullableLongSlot =>
        setLongAt(offset, value)
      case NodeNonNullRefSlot =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setRefAt(offset, node(value))
      case NodeNullableRefSlot =>
        setRefAt(offset, nullableNode(value))
      case OtherNonNullRefSlot if slot.slot.typ.isAssignableFrom(CTNode) =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setRefAt(offset, nullableNode(value))
      case OtherNullableRefSlot if slot.slot.typ.isAssignableFrom(CTNode) =>
        setRefAt(offset, nullableNode(value))
      case _ => throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Do not know how to make a primitive Node setter for slot ${slot.slot}"
        )
    }
  }

  @VisibleForTesting
  private[slotted] def setPrimitiveRel(key: SlotKey, value: Long): Unit = {
    val slot = slots(key)
    val offset = slot.offset
    slot.slotType match {
      case RelNonNullLongSlot =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setLongAt(offset, value)
      case RelNullableLongSlot =>
        setLongAt(offset, value)
      case RelNonNullRefSlot =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setRefAt(offset, relationship(value))
      case RelNullableRefSlot =>
        setRefAt(offset, nullableRel(value))
      case OtherNonNullRefSlot if slot.slot.typ.isAssignableFrom(CTRelationship) =>
        checkOnlyWhenAssertionsAreEnabled(value != PRIMITIVE_NULL)
        setRefAt(offset, nullableRel(value))
      case OtherNullableRefSlot if slot.slot.typ.isAssignableFrom(CTRelationship) =>
        setRefAt(offset, nullableRel(value))
      case _ =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          s"Do not know how to make a primitive Relationship setter for slot ${slot.slot}"
        )
    }
  }

  private def mergeWith(otherRow: SlottedRow, nullCheck: Boolean): Unit = {
    val otherSlots = otherRow.slots.slots
    var i = 0
    while (i < otherSlots.length) {
      val otherSlot = otherSlots(i)
      val key = otherSlot.key
      val otherOffset = otherSlot.offset
      key match {
        case _: VariableSlotKey => otherSlot.slotType match {
            case NodeNonNullLongSlot | NodeNullableLongSlot => setPrimitiveNode(key, otherRow.getLongAt(otherOffset))
            case RelNonNullLongSlot | RelNullableLongSlot   => setPrimitiveRel(key, otherRow.getLongAt(otherOffset))
            case otherType =>
              if (isRefSlot(otherType) && otherRow.isRefInitialized(otherOffset)) {
                checkOnlyWhenAssertionsAreEnabled(!nullCheck || checkCompatibleNullablility(key, otherSlot))
                SlotAccessor.setValue(this, slots(key), otherRow.getRefAtWithoutCheckingInitialized(otherOffset))
              }
          }
        case cachedPropKey: CachedPropertySlotKey =>
          setCachedProperty(cachedPropKey.property, otherRow.getCachedPropertyAt(otherOffset))
        case metadataKey: MetaDataSlotKey =>
          val thisOffset = slots.metaDataOffset(metadataKey)
          if (!isRefInitialized(thisOffset) || (getRefAtWithoutCheckingInitialized(thisOffset) eq NO_VALUE)) {
            setRefAt(thisOffset, otherRow.getRefAt(otherOffset))
          }
        case _: DuplicatedSlotKey => // Ignore
        case _: PlanningSlotKey =>
          throw InternalException.internalError(getClass.getSimpleName, "PlanningSlotKey encountered during execution")
      }
      i += 1
    }
  }

  override def mergeWith(other: ReadableRow, entityById: EntityById, nullCheck: Boolean = true): Unit = other match {
    case slottedOther: SlottedRow => mergeWith(slottedOther, nullCheck)
    case _ =>
      throw InternalException.internalError(this.getClass.getSimpleName, "Well well, isn't this a delicate situation?")
  }

  private def checkCompatibleNullablility(key: SlotKey, otherSlot: KeyedSlot): Boolean = {
    val thisSlot = slots(key)
    // This should be guaranteed by slot allocation or else we could get incorrect results
    if (!isNullable(thisSlot.slotType) && isNullable(otherSlot.slotType))
      throw InternalException.internalError(
        this.getClass.getSimpleName,
        s"Tried to merge slot $otherSlot into $thisSlot but its nullability is incompatible"
      )
    true
  }

  override def createClone(): CypherRow = {
    val clone = SlottedRow(slots)
    clone.copyAllFrom(this)
    clone
  }

  override def isNull(key: String): Boolean = {
    val slot = slots(key)
    slot.slotType match {
      case NodeNullableLongSlot | RelNullableLongSlot =>
        entityIsNull(getLongAt(slot.offset))
      case NodeNullableRefSlot | RelNullableRefSlot =>
        isRefInitialized(slot.offset) && (getRefAtWithoutCheckingInitialized(slot.offset) eq NO_VALUE)
      case _ => false
    }
  }

  private def iterator: Iterator[(String, AnyValue)] = {
    // This method implementation is for debug usage only.
    // Please do not use in production code.
    def prettyKey(key: String, aliases: collection.Set[String]): String = (key +: aliases.toSeq).mkString(",")
    slots.legacyView.keyedSlotsOrdered().iterator.map {
      case SlotWithKeyAndAliases(VariableSlotKey(key), RefSlot(offset, _, _), aliases) =>
        (prettyKey(key, aliases), refs(offset))
      case SlotWithKeyAndAliases(VariableSlotKey(key), LongSlot(offset, _, _), aliases) =>
        (prettyKey(key, aliases), Values.longValue(longs(offset)))
      case SlotWithKeyAndAliases(CachedPropertySlotKey(cachedProperty), slot, _) =>
        (cachedProperty.asCanonicalStringVal, refs(slot.offset))
      case SlotWithKeyAndAliases(MetaDataSlotKey(key, id), slot, _) =>
        (s"MetaData($key, $id)", refs(slot.offset))
      case SlotWithKeyAndAliases(ApplyPlanSlotKey(id), slot, _) =>
        (s"Apply-Plan($id)", Values.longValue(longs(slot.offset)))
      case SlotWithKeyAndAliases(OuterNestedApplyPlanSlotKey(id), slot, _) =>
        (s"Nested-Apply-Plan($id)", Values.longValue(longs(slot.offset)))
      case SlotWithKeyAndAliases(DuplicatedSlotKey(key, id), slot, _) =>
        (
          s"DuplicatedSlot($key, $id)",
          if (slot.isLongSlot) Values.longValue(longs(slot.offset))
          else refs(slot.offset)
        )
    }
  }

  /**
   * Removes slots that have been marked as "discarded" in the slot configuration.
   *
   * Caution!! Only safe to call when the current "pipeline" is done processing this row.
   * In slotted this is before a "break", where rows are copied
   * (see [[SlottedPipelineBreakingPolicy]]).
   */
  // Note, consider adding a test case in SlottedPipelineBreakingPolicyTest
  // if you add calls to this method
  override def compact(): Unit = {
    if (refs.length > 0 && slots.discardedRefSlotOffsets.nonEmpty) {
      val discard = slots.discardedRefSlotOffsets
      var i = 0
      while (i < discard.length) {
        refs(discard(i)) = null
        i += 1
      }
    }
  }
}
