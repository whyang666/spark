/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import java.util.NoSuchElementException

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer

/**
 * :: DeveloperApi ::
 * Performs an sort merge join of two child relations.
 */
@DeveloperApi
case class SortMergeOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression] = None) extends BinaryNode {

  val (streamed, buffered) = joinType match {
    case RightOuter => (right, left)
    case _ => (left, right)
  }

  val (streamedKeys, bufferedKeys) = joinType match {
    case RightOuter => (rightKeys, leftKeys)
    case _ => (leftKeys, rightKeys)
  }

  override def output: Seq[Attribute] = joinType match {
    case Inner =>
      left.output ++ right.output
    case LeftOuter =>
      left.output ++ right.output.map(_.withNullability(true))
    case RightOuter =>
      left.output.map(_.withNullability(true)) ++ right.output
    case FullOuter =>
      left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
    case x =>
      throw new Exception(s"SortMergeJoin should not take $x as the JoinType")
  }

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  // this is to manually construct an ordering that can be used to compare keys from both sides
  private val keyOrdering: RowOrdering = RowOrdering.forSchema(streamedKeys.map(_.dataType))

  override def outputOrdering: Seq[SortOrder] = requiredOrders(streamedKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  @transient protected lazy val streamedKeyGenerator = newProjection(streamedKeys, streamed.output)
  @transient protected lazy val bufferedKeyGenerator = newProjection(bufferedKeys, buffered.output)

  @transient private[this] lazy val streamedNullRow = new GenericRow(streamed.output.length)
  @transient private[this] lazy val bufferedNullRow = new GenericRow(buffered.output.length)

  @transient private[this] lazy val boundCondition =
    condition.map(newPredicate(_, streamed.output ++ buffered.output)).getOrElse((row: Row) => true)

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] =
    keys.map(SortOrder(_, Ascending))

  override def execute(): RDD[Row] = {
    val streamResults = streamed.execute().map(_.copy())
    val bufferResults = buffered.execute().map(_.copy())

    streamResults.zipPartitions(bufferResults) { (streamedIter, bufferedIter) =>
      new Iterator[Row] {
        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow5
        private[this] var streamedElement: Row = _
        private[this] var bufferedElement: Row = _
        private[this] var streamedKey: Row = _
        private[this] var bufferedKey: Row = _
        private[this] var bufferedMatches: CompactBuffer[Row] = _
        private[this] var bufferedPosition: Int = -1
        private[this] var stop: Boolean = false
        private[this] var matchKey: Row = _
        private[this] var continueStreamed: Boolean = _

        // initialize iterator
        initialize()

        override final def hasNext: Boolean = nextMatchingPair()

        override final def next(): Row = {
          if (hasNext) {
            println (streamedElement +"+"+ bufferedElement)
            if (bufferedMatches == null || bufferedMatches.size == 0) {
              if (continueStreamed) {
                val joinedRow = joinType match {
                  case RightOuter => joinRow(bufferedNullRow.copy(), streamedElement)
                  case _ => joinRow(streamedElement, bufferedNullRow.copy())
                }
                fetchStreamed()
                joinedRow
              } else {
                val joinedRow = joinType match {
                  case RightOuter => joinRow(bufferedElement, streamedNullRow.copy())
                  case _ => joinRow(streamedNullRow.copy(), bufferedElement)
                }
                fetchBuffered()
                joinedRow
              }
            } else {
              // we are using the buffered right rows and run down left iterator
              val joinedRow = joinType match {
                case RightOuter => joinRow(bufferedMatches(bufferedPosition), streamedElement)
                case _ => joinRow(streamedElement, bufferedMatches(bufferedPosition))
              }
              bufferedPosition += 1
              if (bufferedPosition >= bufferedMatches.size) {
                bufferedPosition = 0
                fetchStreamed()
                if (streamedElement == null || keyOrdering.compare(streamedKey, matchKey) != 0) {
                  stop = false
                  bufferedMatches = null
                }
              }
              joinedRow
            }
          } else {
            // no more result
            throw new NoSuchElementException
          }
        }

        private def fetchStreamed() = {
          if (streamedIter.hasNext) {
            streamedElement = streamedIter.next()
            streamedKey = streamedKeyGenerator(streamedElement)
          } else {
            streamedElement = null
          }
        }

        private def fetchBuffered() = {
          if (bufferedIter.hasNext) {
            bufferedElement = bufferedIter.next()
            bufferedKey = bufferedKeyGenerator(bufferedElement)
          } else {
            bufferedElement = null
          }
        }

        private def initialize() = {
          fetchStreamed()
          fetchBuffered()
        }

        /**
         * Searches the right iterator for the next rows that have matches in left side, and store
         * them in a buffer.
         *
         * @return true if the search is successful, and false if the right iterator runs out of
         *         tuples.
         */
        private def nextMatchingPair(): Boolean = {
          if (!stop && streamedElement != null) {
            // run both side to get the first match pair
            while (!stop && streamedElement != null && bufferedElement != null) {
              val comparing = keyOrdering.compare(streamedKey, bufferedKey)
              // for inner join, we need to filter those null keys
              stop = comparing == 0 && !streamedKey.anyNull
              if (comparing > 0 || bufferedKey.anyNull) {
                if (joinType == FullOuter) {
                  continueStreamed = false
                  return true
                } else {
                  fetchBuffered()
                }
              } else if (comparing < 0 || streamedKey.anyNull) {
                if (joinType == Inner) {
                  fetchStreamed()
                } else {
                  continueStreamed = true
                  return true
                }
              }
            }
            bufferedMatches = new CompactBuffer[Row]()
            if (stop) {
              stop = false
              // iterate the right side to buffer all rows that matches
              // as the records should be ordered, exit when we meet the first that not match
              while (!stop && bufferedElement != null) {
                if (boundCondition(joinRow(streamedElement, bufferedElement))) {
                  bufferedMatches += bufferedElement
                } else if (joinType == FullOuter) {
                  bufferedMatches += bufferedNullRow.copy()
                }
                fetchBuffered()
                stop = keyOrdering.compare(streamedKey, bufferedKey) != 0
              }
              if (bufferedMatches.size == 0 && joinType != Inner) {
                bufferedMatches += bufferedNullRow.copy()
              }
              if (bufferedMatches.size > 0) {
                bufferedPosition = 0
                matchKey = streamedKey
              }
            }
          }
          // stop is false iff left or right has finished iteration
          if (!stop && (bufferedMatches == null || bufferedMatches.size == 0)) {
            if (streamedElement == null && bufferedElement != null) {
              // streamedElement == null but bufferedElement != null
              if (joinType == FullOuter) {
                continueStreamed = false
                return true
              }
            } else if (streamedElement != null && bufferedElement == null) {
              // bufferedElement == null but streamedElement != null
              if (joinType != Inner) {
                continueStreamed = true
                return true
              }
            }
          }
          bufferedMatches != null && bufferedMatches.size > 0
        }
      }
    }
  }
}
