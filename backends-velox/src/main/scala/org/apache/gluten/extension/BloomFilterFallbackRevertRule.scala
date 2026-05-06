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
package org.apache.gluten.extension

import org.apache.gluten.expression.VeloxBloomFilterMightContain
import org.apache.gluten.expression.aggregate.VeloxBloomFilterAggregate

import org.apache.spark.sql.catalyst.expressions.{BloomFilterMightContain, Expression}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{BaseSubqueryExec, ExecSubqueryExpression, FilterExec, SparkPlan}

/**
 * When a stage containing might_contain falls back to vanilla Spark execution (e.g., because the
 * scan datasource is not supported by Gluten, or ANSI mode forces fallback), the bloom filter data
 * produced by the subquery must also use Spark-compatible format.
 *
 * This rule reverts VeloxBloomFilterAggregate / VeloxBloomFilterMightContain back to their
 * Spark-native counterparts in such cases. It handles two directions of incompatibility:
 *
 * 1. Consumer fallback: FilterExec (not offloaded) contains VeloxBloomFilterMightContain.
 *    Both the consumer expression AND the subquery plan's aggregate must be reverted.
 *
 * 2. Subquery fallback: The subquery's aggregate was not offloaded (e.g., due to ANSI mode)
 *    and still contains VeloxBloomFilterAggregate which produces Velox-format bytes.
 *    When the consumer is also reverted to BloomFilterMightContain (Spark reader),
 *    the subquery must produce Spark-format bytes to match.
 *
 * This rule runs in postTransform (after offload decisions are made) so it can detect which
 * FilterExec nodes were NOT offloaded to native execution.
 *
 * See: https://github.com/apache/gluten/issues/12013
 */
object BloomFilterFallbackRevertRule extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformWithSubqueries {
      case filter: FilterExec if containsVeloxBloomFilter(filter) =>
        // This FilterExec was not offloaded (it would be FilterExecTransformer if offloaded).
        // Revert bloom filter expressions back to Spark native format.
        revertBloomFilterExpressions(filter)
      case other => other
    }
  }

  private def containsVeloxBloomFilter(plan: SparkPlan): Boolean = {
    plan.expressions.exists(containsVeloxBloomFilterExpr)
  }

  private def containsVeloxBloomFilterExpr(expr: Expression): Boolean = {
    expr match {
      case _: VeloxBloomFilterMightContain => true
      case _ => expr.children.exists(containsVeloxBloomFilterExpr)
    }
  }

  private def revertBloomFilterExpressions(filter: FilterExec): FilterExec = {
    val newPlan = filter.transformExpressionsWithPruning(_ => true) {
      case veloxMC: VeloxBloomFilterMightContain =>
        // Revert the consumer expression AND the subquery plan's aggregate.
        val revertedBloomExpr = revertSubqueryInExpr(veloxMC.bloomFilterExpression)
        BloomFilterMightContain(revertedBloomExpr, veloxMC.valueExpression)
    }
    newPlan.asInstanceOf[FilterExec]
  }

  /**
   * Reverts VeloxBloomFilterAggregate back to BloomFilterAggregate in both:
   * - The expression tree itself (in case VeloxBloomFilterAggregate appears directly)
   * - The subquery plan referenced by ExecSubqueryExpression nodes
   *
   * This ensures the subquery produces Spark-format bloom filter data that matches
   * the reverted BloomFilterMightContain consumer.
   */
  private def revertSubqueryInExpr(expr: Expression): Expression = {
    expr.transformUp {
      case sub: ExecSubqueryExpression =>
        // Revert VeloxBloomFilterAggregate in the subquery plan tree
        val newPlan = sub.plan.transformDown {
          case node =>
            node.transformExpressions {
              case veloxAgg: VeloxBloomFilterAggregate =>
                BloomFilterAggregate(
                  veloxAgg.child,
                  veloxAgg.estimatedNumItemsExpression,
                  veloxAgg.numBitsExpression,
                  veloxAgg.mutableAggBufferOffset,
                  veloxAgg.inputAggBufferOffset)
            }
        }.asInstanceOf[BaseSubqueryExec]
        sub.withNewPlan(newPlan)
      case veloxAgg: VeloxBloomFilterAggregate =>
        // Safety: revert any VeloxBloomFilterAggregate that appears directly in the
        // expression tree (outside of subquery plans)
        BloomFilterAggregate(
          veloxAgg.child,
          veloxAgg.estimatedNumItemsExpression,
          veloxAgg.numBitsExpression,
          veloxAgg.mutableAggBufferOffset,
          veloxAgg.inputAggBufferOffset)
    }
  }
}
