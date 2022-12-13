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

import org.apache.flink.util.FlinkException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/** Tests for Flink Planner Calcite Shim Helper*/
public class FlinkPlannerCalciteShimTest {

    @Test
    public void testFlinkPlannerCalciteShimLoader() throws FlinkException {
        FlinkPlannerCalciteShim shim16 = FlinkPlannerCalciteShim.loadShim(FlinkPlannerCalciteShim.FLINK_1_16);
        Assert.assertTrue("calcite shim should match flink 1.16", shim16 instanceof FlinkPlannerCalciteShim16);

        FlinkPlannerCalciteShim shim16snapshot = FlinkPlannerCalciteShim.loadShim(
                FlinkPlannerCalciteShim.FLINK_1_16 + "-SNAPSHOT");
        Assert.assertTrue("calcite shim should match flink 1.16-SNAPSHOT",
                shim16snapshot instanceof FlinkPlannerCalciteShim16);

        FlinkPlannerCalciteShim shim17 = FlinkPlannerCalciteShim.loadShim(FlinkPlannerCalciteShim.FLINK_1_17);
        Assert.assertTrue("calcite shim should match flink 1.17", shim17 instanceof FlinkPlannerCalciteShim17);

        FlinkPlannerCalciteShim shim17snapshot = FlinkPlannerCalciteShim.loadShim(
                FlinkPlannerCalciteShim.FLINK_1_17 + "-SNAPSHOT");
        Assert.assertTrue("calcite shim should match flink 1.17-SNAPSHOT",
                shim17snapshot instanceof FlinkPlannerCalciteShim17);
    }

    @Test
    public void testFlinkVersionCompareCheck() {
        Assert.assertTrue("input version should be greater than target version",
                FlinkPlannerCalciteShim.isVersionNoLess(
                        FlinkPlannerCalciteShim.FLINK_1_16, FlinkPlannerCalciteShim.FLINK_1_15));

        Assert.assertTrue("input version should be greater than target version",
                FlinkPlannerCalciteShim.isVersionNoLess(
                        FlinkPlannerCalciteShim.FLINK_1_17 + "-SNAPSHOT", FlinkPlannerCalciteShim.FLINK_1_15));

        Assert.assertTrue("input version should be greater than target version",
                FlinkPlannerCalciteShim.isVersionNoLess(
                        "1.18-SNAPSHOT", FlinkPlannerCalciteShim.FLINK_1_15));

        Assert.assertTrue("only support flink 1.16+",
                FlinkPlannerCalciteShim.isVersionNoLess("1.16-SNAPSHOT", FlinkPlannerCalciteShim.FLINK_1_16));

        Assert.assertTrue("input version should be greater than target version",
                FlinkPlannerCalciteShim.isVersionNoLess(
                        "2.0-FOOBAR", FlinkPlannerCalciteShim.FLINK_1_15));
    }

    @Test
    public void testFlinkPlannerVersionCheck() {
        String version = FlinkPlannerCalciteShim.getFLinkPlannerVersion();
        Assert.assertTrue("only support flink 1.16+",
                FlinkPlannerCalciteShim.isVersionNoLess(version, FlinkPlannerCalciteShim.FLINK_1_16));
    }
}
