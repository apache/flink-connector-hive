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

package org.apache.flink.connectors.hive;

import org.apache.flink.packaging.PackagingTestUtils;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.test.resources.ResourceTestUtils;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Arrays;

class PackagingITCase {

    @Test
    void testPackaging() throws Exception {
        final Path jar = ResourceTestUtils.getResource(".*/flink-sql-connector-hive-2[^/]*\\.jar");

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar,
                Arrays.asList(
                        "META-INF/",
                        "au/com/bytecode/opencsv/",
                        "avro/",
                        "com/fasterxml/jackson/",
                        "com/facebook/fb303/",
                        "google/protobuf/",
                        "io/airlift/",
                        "javaewah/",
                        "javax/jdo/",
                        "javax/realtime/",
                        "javolution/",
                        "jodd/",
                        "package.jdo",
                        "parquet/",
                        "codegen/",
                        "hive-log4j2.properties",
                        "parquet-logging.properties",
                        "tez-container-log4j2.properties",
                        "hive-exec-log4j2.properties",
                        "org-apache-calcite-jdbc.properties",
                        "parquet.thrift",
                        "shaded/parquet/",
                        "org/antlr/runtime/",
                        "org/json/",
                        "org/slf4j/",
                        "org/codehaus/jackson/",
                        "org/joda/",
                        "org/apache/avro/",
                        "org/apache/commons/",
                        "org/apache/hadoop/",
                        "org/apache/hive/",
                        "org/apache/orc/",
                        "org/apache/thrift/",
                        "org/apache/tez/",
                        "org/apache/flink/api/",
                        "org/apache/flink/connectors/base/",
                        "org/apache/flink/connectors/hive/",
                        "org/apache/flink/formats/hadoop/bulk/",
                        "org/apache/flink/hadoopcompatibility/",
                        "org/apache/flink/streaming/api/functions/sink/filesystem/",
                        "org/apache/flink/table/",
                        "org/apache/flink/orc/",
                        "org/apache/flink/parquet/",
                        "org/apache/flink/hive/"));
        PackagingTestUtils.assertJarContainsServiceEntry(jar, Factory.class);
    }
}
