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

package org.apache.flink.table.catalog.hive.client;

import org.apache.flink.table.HiveVersionTestUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeTrue;

/** Unit tests for Hive 4 shim methods. Only runs when Hive 4 jars are on the classpath. */
public class HiveShimV400Test {

    @Test
    public void testGetGenericWindowingEvaluator() throws Exception {
        assumeTrue(HiveVersionTestUtil.HIVE_400_OR_LATER);
        HiveShim shim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
        GenericUDAFEvaluator evaluator =
                shim.getGenericWindowingEvaluator(
                        "lead",
                        Collections.singletonList(
                                PrimitiveObjectInspectorFactory.javaIntObjectInspector),
                        false,
                        false);
        assertThat(evaluator).isNotNull();
    }

    @Test
    public void testInitializeSerDe() throws Exception {
        assumeTrue(HiveVersionTestUtil.HIVE_400_OR_LATER);
        HiveShim shim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
        LazySimpleSerDe serde = new LazySimpleSerDe();
        Properties props = new Properties();
        props.setProperty("columns", "col1");
        props.setProperty("columns.types", "string");
        props.setProperty("serialization.format", "1");
        // Should not throw — verifies HiveShimV400.initializeSerDe calls
        // AbstractSerDe.initialize(conf, tableProps, partProps) instead of the
        // removed SerDeUtils.initializeSerDe()
        shim.initializeSerDe(serde, new Configuration(), props, null);
    }

    @Test
    public void testShimLoaderReturnsV400() {
        assumeTrue(HiveVersionTestUtil.HIVE_400_OR_LATER);
        HiveShim shim = HiveShimLoader.loadHiveShim(HiveShimLoader.getHiveVersion());
        assertThat(shim.getClass().getName()).contains("HiveShimV400");
    }
}
