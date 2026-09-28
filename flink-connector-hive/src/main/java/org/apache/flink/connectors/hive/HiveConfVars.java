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

import org.apache.hadoop.hive.conf.HiveConf;

import java.util.HashMap;
import java.util.Map;

/**
 * Compatibility shim for {@link HiveConf.ConfVars} enum constants that were renamed in Hive 4.0
 * (HIVE-27925). Hive 4 adopted snake_case naming (e.g. {@code METASTOREURIS} became {@code
 * METASTORE_URIS}). This class resolves the correct enum constant at class-load time, allowing the
 * connector to work across Hive 2.3, 3.1, and 4.x without compile-time dependency on a specific
 * version.
 *
 * <p>Also provides compatibility wrappers for Hive API methods whose signatures changed in Hive 4.
 */
public class HiveConfVars {

    private static final Map<String, String> LEGACY_NAMES = createLegacyNames();

    public static final HiveConf.ConfVars COMPRESS_INTERMEDIATE_CODEC =
            resolveConfVar("COMPRESS_INTERMEDIATE_CODEC");
    public static final HiveConf.ConfVars COMPRESS_INTERMEDIATE_TYPE =
            resolveConfVar("COMPRESS_INTERMEDIATE_TYPE");
    public static final HiveConf.ConfVars COMPRESS_RESULT = resolveConfVar("COMPRESS_RESULT");
    public static final HiveConf.ConfVars DEFAULT_PARTITION_NAME =
            resolveConfVar("DEFAULT_PARTITION_NAME");
    public static final HiveConf.ConfVars DOWNLOADED_RESOURCES_DIR =
            resolveConfVar("DOWNLOADED_RESOURCES_DIR");
    public static final HiveConf.ConfVars DYNAMIC_PARTITIONING_MODE =
            resolveConfVar("DYNAMIC_PARTITIONING_MODE");
    public static final HiveConf.ConfVars HIVE_CHECK_FILEFORMAT =
            resolveConfVar("HIVE_CHECK_FILEFORMAT");
    public static final HiveConf.ConfVars HIVE_CONF_VALIDATION =
            resolveConfVar("HIVE_CONF_VALIDATION");
    public static final HiveConf.ConfVars HIVE_DEFAULT_FILEFORMAT =
            resolveConfVar("HIVE_DEFAULT_FILEFORMAT");
    public static final HiveConf.ConfVars HIVE_DEFAULT_RCFILE_SERDE =
            resolveConfVar("HIVE_DEFAULT_RCFILE_SERDE");
    public static final HiveConf.ConfVars HIVE_DEFAULT_SERDE = resolveConfVar("HIVE_DEFAULT_SERDE");
    public static final HiveConf.ConfVars HIVE_GROUPBY_SKEW = resolveConfVar("HIVE_GROUPBY_SKEW");
    public static final HiveConf.ConfVars HIVE_MAPRED_MODE = resolveConfVar("HIVE_MAPRED_MODE");
    public static final HiveConf.ConfVars HIVE_QUERY_ID = resolveConfVar("HIVE_QUERY_ID");
    public static final HiveConf.ConfVars HIVE_SCRIPT_ENV_BLACKLIST =
            resolveConfVar("HIVE_SCRIPT_ENV_BLACKLIST");
    public static final HiveConf.ConfVars HIVE_SCRIPT_ESCAPE = resolveConfVar("HIVE_SCRIPT_ESCAPE");
    public static final HiveConf.ConfVars HIVE_SCRIPT_ID_ENV_VAR =
            resolveConfVar("HIVE_SCRIPT_ID_ENV_VAR");
    public static final HiveConf.ConfVars HIVE_SCRIPT_TRUNCATE_ENV =
            resolveConfVar("HIVE_SCRIPT_TRUNCATE_ENV");
    public static final HiveConf.ConfVars MAPRED_MAX_SPLIT_SIZE =
            resolveConfVar("MAPRED_MAX_SPLIT_SIZE");
    public static final HiveConf.ConfVars METASTORE_URIS = resolveConfVar("METASTORE_URIS");
    public static final HiveConf.ConfVars METASTORE_WAREHOUSE =
            resolveConfVar("METASTORE_WAREHOUSE");
    public static final HiveConf.ConfVars SCRATCH_DIR = resolveConfVar("SCRATCH_DIR");
    public static final HiveConf.ConfVars SCRIPT_WRAPPER = resolveConfVar("SCRIPT_WRAPPER");

    // These were not renamed in Hive 4 but are included for completeness so all
    // ConfVars references go through this class.
    public static final HiveConf.ConfVars DEFAULT_ZOOKEEPER_PARTITION_NAME =
            resolveConfVar("DEFAULT_ZOOKEEPER_PARTITION_NAME");
    public static final HiveConf.ConfVars HIVE_AUTHORIZATION_ENABLED =
            resolveConfVar("HIVE_AUTHORIZATION_ENABLED");
    public static final HiveConf.ConfVars HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME =
            resolveConfVar("HIVE_AUTOGEN_COLUMNALIAS_PREFIX_INCLUDEFUNCNAME");
    public static final HiveConf.ConfVars HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL =
            resolveConfVar("HIVE_AUTOGEN_COLUMNALIAS_PREFIX_LABEL");
    public static final HiveConf.ConfVars HIVE_EXECUTION_ENGINE =
            resolveConfVar("HIVE_EXECUTION_ENGINE");
    public static final HiveConf.ConfVars HIVE_GROUPBY_ORDERBY_POSITION_ALIAS =
            resolveConfVar("HIVE_GROUPBY_ORDERBY_POSITION_ALIAS");
    public static final HiveConf.ConfVars HIVE_QUOTEDID_SUPPORT =
            resolveConfVar("HIVE_QUOTEDID_SUPPORT");
    public static final HiveConf.ConfVars HIVE_SAMPLE_RANDOM_NUM =
            resolveConfVar("HIVE_SAMPLE_RANDOM_NUM");
    public static final HiveConf.ConfVars HIVE_STATS_AUTOGATHER =
            resolveConfVar("HIVE_STATS_AUTOGATHER");
    public static final HiveConf.ConfVars HIVE_STATS_COLLECT_SCANCOLS =
            resolveConfVar("HIVE_STATS_COLLECT_SCANCOLS");
    public static final HiveConf.ConfVars HIVE_TYPE_CHECK_ON_INSERT =
            resolveConfVar("HIVE_TYPE_CHECK_ON_INSERT");
    public static final HiveConf.ConfVars METASTORE_INT_ARCHIVED =
            resolveConfVar("METASTORE_INT_ARCHIVED");
    public static final HiveConf.ConfVars METASTORE_INT_EXTRACTED =
            resolveConfVar("METASTORE_INT_EXTRACTED");
    public static final HiveConf.ConfVars METASTORE_INT_ORIGINAL =
            resolveConfVar("METASTORE_INT_ORIGINAL");
    public static final HiveConf.ConfVars NEW_TABLE_DEFAULT_PARA =
            resolveConfVar("NEW_TABLE_DEFAULT_PARA");

    // Additional constants used in test code
    public static final HiveConf.ConfVars HADOOP_BIN = resolveConfVar("HADOOP_BIN");
    public static final HiveConf.ConfVars HIVE_AUTHORIZATION_MANAGER =
            resolveConfVar("HIVE_AUTHORIZATION_MANAGER");
    public static final HiveConf.ConfVars HIVE_CBO_ENABLED = resolveConfVar("HIVE_CBO_ENABLED");
    public static final HiveConf.ConfVars HIVE_CONVERT_JOIN = resolveConfVar("HIVE_CONVERT_JOIN");
    public static final HiveConf.ConfVars HIVE_COUNTERS_PULL_INTERVAL =
            resolveConfVar("HIVE_COUNTERS_PULL_INTERVAL");
    public static final HiveConf.ConfVars HIVE_HISTORY_FILE_LOC =
            resolveConfVar("HIVE_HISTORY_FILE_LOC");
    public static final HiveConf.ConfVars HIVE_INFER_BUCKET_SORT =
            resolveConfVar("HIVE_INFER_BUCKET_SORT");
    public static final HiveConf.ConfVars HIVE_IN_TEST = resolveConfVar("HIVE_IN_TEST");
    public static final HiveConf.ConfVars HIVE_METADATA_ONLY_QUERIES =
            resolveConfVar("HIVE_METADATA_ONLY_QUERIES");
    public static final HiveConf.ConfVars HIVE_OPT_INDEX_FILTER =
            resolveConfVar("HIVE_OPT_INDEX_FILTER");
    public static final HiveConf.ConfVars HIVE_RPC_QUERY_PLAN =
            resolveConfVar("HIVE_RPC_QUERY_PLAN");
    public static final HiveConf.ConfVars HIVE_SERVER2_LOGGING_OPERATION_ENABLED =
            resolveConfVar("HIVE_SERVER2_LOGGING_OPERATION_ENABLED");
    public static final HiveConf.ConfVars HIVE_SKEW_JOIN = resolveConfVar("HIVE_SKEW_JOIN");
    public static final HiveConf.ConfVars HIVE_SUPPORT_CONCURRENCY =
            resolveConfVar("HIVE_SUPPORT_CONCURRENCY");
    public static final HiveConf.ConfVars HIVE_TXN_MANAGER = resolveConfVar("HIVE_TXN_MANAGER");
    public static final HiveConf.ConfVars LOCAL_SCRATCH_DIR = resolveConfVar("LOCAL_SCRATCH_DIR");
    public static final HiveConf.ConfVars METASTORE_CONNECT_URL_KEY =
            resolveConfVar("METASTORE_CONNECT_URL_KEY");
    public static final HiveConf.ConfVars METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES =
            resolveConfVar("METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES");
    public static final HiveConf.ConfVars METASTORE_USE_THRIFT_SASL =
            resolveConfVar("METASTORE_USE_THRIFT_SASL");
    public static final HiveConf.ConfVars METASTORE_VALIDATE_COLUMNS =
            resolveConfVar("METASTORE_VALIDATE_COLUMNS");
    public static final HiveConf.ConfVars METASTORE_VALIDATE_CONSTRAINTS =
            resolveConfVar("METASTORE_VALIDATE_CONSTRAINTS");
    public static final HiveConf.ConfVars METASTORE_VALIDATE_TABLES =
            resolveConfVar("METASTORE_VALIDATE_TABLES");

    private HiveConfVars() {}

    private static Map<String, String> createLegacyNames() {
        Map<String, String> m = new HashMap<>();
        // Names that already had underscores in Hive 2.3/3.1
        m.put("HIVE_SCRIPT_ENV_BLACKLIST", "HIVESCRIPT_ENV_BLACKLIST");
        return m;
    }

    /**
     * Resolves a {@link HiveConf.ConfVars} enum constant by its snake_case name (Hive 4+). If not
     * found, falls back to the legacy name (Hive 2.3/3.1) by first checking an explicit mapping,
     * then stripping all underscores.
     */
    private static HiveConf.ConfVars resolveConfVar(String snakeCaseName) {
        try {
            return HiveConf.ConfVars.valueOf(snakeCaseName);
        } catch (IllegalArgumentException e) {
            String legacy = LEGACY_NAMES.get(snakeCaseName);
            if (legacy != null) {
                return HiveConf.ConfVars.valueOf(legacy);
            }
            return HiveConf.ConfVars.valueOf(snakeCaseName.replace("_", ""));
        }
    }
}
