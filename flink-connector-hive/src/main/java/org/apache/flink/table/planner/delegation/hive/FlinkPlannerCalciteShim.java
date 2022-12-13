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

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Flink planner calcite shim proxy calcite rel method signature change in flink table planner*/
public interface FlinkPlannerCalciteShim extends Serializable {

  String FLINK_1_16 = "1.16";

  String FLINK_1_17 = "1.17";

  String FLINK_1_15 = "1.15";

  Pattern VERSION_PATTERN = Pattern.compile("(?!\\.)(\\d+(\\.\\d+)+)(?:[-.][A-Z]+)?(?![\\d.])$");

  ConcurrentHashMap<String, FlinkPlannerCalciteShim> shims = new ConcurrentHashMap<>();

  AggregateCall create(
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
      throws Exception;

  AggregateCall create(
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
      throws Exception;

  static String getFLinkPlannerVersion() {
    return org.apache.calcite.rel.RelNode.class.getPackage().getImplementationVersion();
  }

  static boolean isVersionNoLess(String version, String target) {
    Matcher m = VERSION_PATTERN.matcher(version);

    if (m.find()) {
      String[] versionArray = m.group(1).split("\\.");
      String[] targetVersionArray = target.split("\\.");

      int i = 0;
      while (i < versionArray.length && i < targetVersionArray.length) {
        int VNo = Integer.valueOf(versionArray[i]);
        int TNo = Integer.valueOf(targetVersionArray[i]);
        if (VNo == TNo) {
          i++;
        } else {
          return VNo > TNo;
        }
      }
      return versionArray.length >= targetVersionArray.length;
    } else {
      throw new RuntimeException("flink planner version is invalid");
    }
  }

  static FlinkPlannerCalciteShim loadShim(String version) {
    return shims.computeIfAbsent(
        version,
        (v) -> {
          if (v.startsWith(FLINK_1_16)) {
            return new FlinkPlannerCalciteShim16();
          } else if (isVersionNoLess(version, FLINK_1_17)) {
            return new FlinkPlannerCalciteShim17();
          } else {
            /** flink-connector-hive can build with 1.16+ only */
            throw new RuntimeException("unsupported flink planner exception");
          }
        });
  }
}
