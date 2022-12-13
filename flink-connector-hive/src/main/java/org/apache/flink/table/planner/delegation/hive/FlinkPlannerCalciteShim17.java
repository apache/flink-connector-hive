/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.delegation.hive;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.ImmutableBitSet;

import java.lang.reflect.Method;
import java.util.List;

/** Flink Planner 1.17+ Calcite Shim */
public class FlinkPlannerCalciteShim17 implements FlinkPlannerCalciteShim {

  public AggregateCall create(
      SqlAggFunction aggFunction,
      boolean distinct,
      boolean approximate,
      boolean ignoreNulls,
      List<Integer> argList,
      int filterArg,
      ImmutableBitSet distinctKeys,
      RelCollation collation,
      int groupCount,
      RelNode input,
      RelDataType type,
      String name)
      throws Exception {
    Method createMethod =
        org.apache.calcite.rel.core.AggregateCall.class.getMethod(
            "create",
            SqlAggFunction.class,
            boolean.class,
            boolean.class,
            boolean.class,
            List.class,
            int.class,
            ImmutableBitSet.class,
            RelCollation.class,
            int.class,
            RelNode.class,
            RelDataType.class,
            String.class);
    return (AggregateCall)
        createMethod.invoke(
            null,
            aggFunction,
            distinct,
            approximate,
            ignoreNulls,
            argList,
            filterArg,
            distinctKeys,
            collation,
            groupCount,
            input,
            type,
            name);
  }

  public AggregateCall create(
      SqlAggFunction aggFunction,
      boolean distinct,
      boolean approximate,
      boolean ignoreNulls,
      List<Integer> argList,
      int filterArg,
      ImmutableBitSet distinctKeys,
      RelCollation collation,
      RelDataType type,
      String name)
      throws Exception {
    Method createMethod =
        org.apache.calcite.rel.core.AggregateCall.class.getMethod(
            "create",
            SqlAggFunction.class,
            boolean.class,
            boolean.class,
            boolean.class,
            List.class,
            int.class,
            ImmutableBitSet.class,
            RelCollation.class,
            RelDataType.class,
            String.class);
    return (AggregateCall)
        createMethod.invoke(
            null,
            aggFunction,
            distinct,
            approximate,
            ignoreNulls,
            argList,
            filterArg,
            distinctKeys,
            collation,
            type,
            name);
  }
}
