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

import org.apache.flink.connectors.hive.FlinkHiveException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreUtils;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Shim for Hive version 4.0.0+. This class compiles against Hive 4 jars so all API calls are direct
 * — no reflection needed.
 *
 * <p>Handles API changes introduced in Hive 4:
 *
 * <ul>
 *   <li>{@code HiveFileFormatUtils.getRecordWriter()} removed — use {@code getHiveRecordWriter()}
 *   <li>{@code StructTypeInfo} return types changed from {@code ArrayList} to {@code List}
 *   <li>{@code SerDeUtils.initializeSerDe()} removed — call {@code AbstractSerDe.initialize()}
 *       directly with 3 args
 *   <li>{@code IMetaStoreClient} column statistics methods require engine parameter
 *   <li>{@code PrincipalDesc} relocated to {@code ql.ddl.privilege} package
 * </ul>
 */
public class HiveShimV400 extends HiveShimV313 {

    /**
     * Hive 4 removed {@code HiveFileFormatUtils.getRecordWriter()} which pre-Hive-4 shims call.
     * This override adapts the old parameter style to {@code getHiveRecordWriter()} by constructing
     * {@code TableDesc} and {@code FileSinkDesc} from the provided values.
     */
    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Class outputFormatClz,
            Class<? extends Writable> outValClz,
            boolean isCompressed,
            Properties tableProps,
            Path outPath) {
        try {
            TableDesc tableDesc = new TableDesc();
            tableDesc.setOutputFileFormatClass(outputFormatClz);
            tableDesc.setProperties(tableProps);
            FileSinkDesc fileSinkDesc = new FileSinkDesc(outPath, tableDesc, isCompressed);
            return HiveFileFormatUtils.getHiveRecordWriter(
                    jobConf, tableDesc, outValClz, fileSinkDesc, outPath, Reporter.NULL);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to create Hive RecordWriter", e);
        }
    }

    @Override
    public List<String> getStructFieldNames(StructTypeInfo structTypeInfo) {
        return new ArrayList<>(structTypeInfo.getAllStructFieldNames());
    }

    @Override
    public List<TypeInfo> getStructFieldTypeInfos(StructTypeInfo structTypeInfo) {
        return new ArrayList<>(structTypeInfo.getAllStructFieldTypeInfos());
    }

    @Override
    public void initializeSerDe(
            Deserializer deserializer,
            Configuration conf,
            Properties tableProperties,
            Properties partitionProperties)
            throws SerDeException {
        ((AbstractSerDe) deserializer).initialize(conf, tableProperties, partitionProperties);
    }

    @Override
    public List<ColumnStatisticsObj> getTableColumnStatistics(
            IMetaStoreClient client,
            String databaseName,
            String tableName,
            List<String> columnNames)
            throws TException {
        return client.getTableColumnStatistics(databaseName, tableName, columnNames, "hive");
    }

    @Override
    public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            IMetaStoreClient client,
            String dbName,
            String tableName,
            List<String> partNames,
            List<String> colNames)
            throws TException {
        return client.getPartitionColumnStatistics(dbName, tableName, partNames, colNames, "hive");
    }

    @Override
    public Object createPrincipalDesc(String principalName, PrincipalType principalType) {
        return new PrincipalDesc(principalName, principalType);
    }

    @Override
    public void walkExpressionTree(Node expression, Dispatcher dispatcher)
            throws SemanticException {
        SemanticDispatcher semanticDispatcher =
                (nd, stack, nodeOutputs) -> {
                    try {
                        return dispatcher.dispatch(nd, stack, nodeOutputs);
                    } catch (SemanticException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new SemanticException(e.getMessage(), e);
                    }
                };
        PreOrderWalker walker = new PreOrderWalker(semanticDispatcher);
        walker.startWalking(Collections.singleton(expression), null);
    }

    /**
     * Hive 4 changed {@code HiveMetaStoreUtils.getDeserializer()} from 2 args to 4 args and {@code
     * getFieldsFromDeserializer()} from 2 args to 3 args. Override to call the Hive 4 signatures
     * directly.
     */
    @Override
    public List<FieldSchema> getFieldsFromDeserializer(
            Configuration conf, Table table, boolean skipConfError) {
        try {
            Deserializer deserializer =
                    HiveMetaStoreUtils.getDeserializer(conf, table, null, skipConfError);
            return HiveMetaStoreUtils.getFieldsFromDeserializer(
                    table.getTableName(), deserializer, conf);
        } catch (Exception e) {
            throw new FlinkHiveException("Failed to get table schema from deserializer", e);
        }
    }

    /**
     * Hive 4 added a 5th boolean parameter ({@code respectNulls}) to {@code
     * FunctionRegistry.getGenericWindowingEvaluator}.
     */
    @Override
    public GenericUDAFEvaluator getGenericWindowingEvaluator(
            String functionName,
            List<ObjectInspector> argumentOIs,
            boolean isDistinct,
            boolean isAllColumns)
            throws SemanticException {
        return FunctionRegistry.getGenericWindowingEvaluator(
                functionName, argumentOIs, isDistinct, isAllColumns, false);
    }

    /**
     * Hive 4 added an 11th boolean parameter to {@code Hive.loadTable()} compared to Hive 3.1's
     * 10-arg version.
     */
    @Override
    public void loadTable(
            Hive hive, Path loadPath, String tableName, boolean replace, boolean isSrcLocal) {
        try {
            hive.loadTable(
                    loadPath,
                    tableName,
                    getLoadFileType(replace),
                    isSrcLocal,
                    false, // isSkewedStoreAsSubdir
                    false, // isAcid
                    false, // hasFollowingStatsTask
                    null, // writeId
                    0, // stmtId
                    replace,
                    false); // isInsertOverwrite
        } catch (HiveException e) {
            throw new FlinkHiveException("Failed to load table", e);
        }
    }

    private static LoadTableDesc.LoadFileType getLoadFileType(boolean replace) {
        return replace
                ? LoadTableDesc.LoadFileType.REPLACE_ALL
                : LoadTableDesc.LoadFileType.KEEP_EXISTING;
    }
}
