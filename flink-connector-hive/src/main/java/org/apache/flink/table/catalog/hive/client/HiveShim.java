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

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.legacy.api.constraints.UniqueConstraint;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.thrift.TException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

/** A shim layer to support different versions of Hive. */
public interface HiveShim extends Serializable {

    /**
     * Create a Hive Metastore client based on the given HiveConf object.
     *
     * @param hiveConf HiveConf instance
     * @return an IMetaStoreClient instance
     */
    IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf);

    /**
     * Get a list of views in the given database from the given Hive Metastore client.
     *
     * @param client Hive Metastore client
     * @param databaseName the name of the database
     * @return A list of names of the views
     * @throws UnknownDBException if the database doesn't exist
     * @throws TException for any other generic exceptions caused by Thrift
     */
    List<String> getViews(IMetaStoreClient client, String databaseName)
            throws UnknownDBException, TException;

    /**
     * Alters a Hive table.
     *
     * @param client the Hive metastore client
     * @param databaseName the name of the database to which the table belongs
     * @param tableName the name of the table to be altered
     * @param table the new Hive table
     */
    void alterTable(IMetaStoreClient client, String databaseName, String tableName, Table table)
            throws InvalidOperationException, MetaException, TException;

    void alterPartition(
            IMetaStoreClient client, String databaseName, String tableName, Partition partition)
            throws InvalidOperationException, MetaException, TException;

    /** Creates SimpleGenericUDAFParameterInfo. */
    SimpleGenericUDAFParameterInfo createUDAFParameterInfo(
            ObjectInspector[] params, boolean isWindowing, boolean distinct, boolean allColumns);

    /**
     * Get the class of Hive's MetaStoreUtils because its package name was changed in Hive 3.1.0.
     *
     * @return MetaStoreUtils class
     */
    Class<?> getMetaStoreUtilsClass();

    /**
     * Get the class of Hive's HiveMetaStoreUtils as it was split from MetaStoreUtils class in Hive
     * 3.1.0.
     *
     * @return HiveMetaStoreUtils class
     */
    Class<?> getHiveMetaStoreUtilsClass();

    /**
     * Hive Date data type class was changed in Hive 3.1.0.
     *
     * @return Hive's Date class
     */
    Class<?> getDateDataTypeClass();

    /** Gets writable class for Date type. */
    Class<?> getDateWritableClass();

    /**
     * Hive Timestamp data type class was changed in Hive 3.1.0.
     *
     * @return Hive's Timestamp class
     */
    Class<?> getTimestampDataTypeClass();

    /** Gets writable class for Timestamp type. */
    Class<?> getTimestampWritableClass();

    /**
     * Generate Hive ColumnStatisticsData from Flink CatalogColumnStatisticsDataDate for DATE
     * columns.
     */
    ColumnStatisticsData toHiveDateColStats(CatalogColumnStatisticsDataDate flinkDateColStats);

    /** Whether a Hive ColumnStatisticsData is for DATE columns. */
    boolean isDateStats(ColumnStatisticsData colStatsData);

    /**
     * Generate Flink CatalogColumnStatisticsDataDate from Hive ColumnStatisticsData for DATE
     * columns.
     */
    CatalogColumnStatisticsDataDate toFlinkDateColStats(ColumnStatisticsData hiveDateColStats);

    /** Get Hive's FileSinkOperator.RecordWriter. */
    FileSinkOperator.RecordWriter getHiveRecordWriter(
            JobConf jobConf,
            Class outputFormatClz,
            Class<? extends Writable> outValClz,
            boolean isCompressed,
            Properties tableProps,
            Path outPath);

    /** For a given OutputFormat class, get the corresponding {@link HiveOutputFormat} class. */
    Class getHiveOutputFormatClass(Class outputFormatClz);

    /** Get Hive table schema from deserializer. */
    List<FieldSchema> getFieldsFromDeserializer(
            Configuration conf, Table table, boolean skipConfError);

    /** List names of all built-in functions. */
    Set<String> listBuiltInFunctions();

    /** Get a Hive built-in function by name. */
    Optional<FunctionInfo> getBuiltInFunctionInfo(String name);

    /** Get the set of columns that have NOT NULL constraints. */
    Set<String> getNotNullColumns(
            IMetaStoreClient client, Configuration conf, String dbName, String tableName);

    /**
     * Get the primary key of a Hive table and convert it to a UniqueConstraint. Return empty if the
     * table doesn't have a primary key, or the constraint doesn't satisfy the desired trait, e.g.
     * RELY.
     */
    Optional<UniqueConstraint> getPrimaryKey(
            IMetaStoreClient client, String dbName, String tableName, byte requiredTrait);

    /** Converts a Flink timestamp instance to what's expected by Hive. */
    @Nullable
    Object toHiveTimestamp(@Nullable Object flinkTimestamp);

    /**
     * Converts a hive timestamp instance to LocalDateTime which is expected by DataFormatConverter.
     */
    LocalDateTime toFlinkTimestamp(Object hiveTimestamp);

    /** Converts a Flink date instance to what's expected by Hive. */
    @Nullable
    Object toHiveDate(@Nullable Object flinkDate);

    /** Converts a hive date instance to LocalDate which is expected by DataFormatConverter. */
    LocalDate toFlinkDate(Object hiveDate);

    /** Converts a Hive primitive java object to corresponding Writable object. */
    @Nullable
    Writable hivePrimitiveToWritable(@Nullable Object value);

    /** Creates a table with PK and NOT NULL constraints. */
    void createTableWithConstraints(
            IMetaStoreClient client,
            Table table,
            Configuration conf,
            UniqueConstraint pk,
            List<Byte> pkTraits,
            List<String> notNullCols,
            List<Byte> nnTraits);

    /** Create orc {@link BulkWriter.Factory} for different hive versions. */
    BulkWriter.Factory<RowData> createOrcBulkWriterFactory(
            Configuration conf, String schema, LogicalType[] fieldTypes);

    /** Checks whether a hive table is a materialized view. */
    default boolean isMaterializedView(org.apache.hadoop.hive.ql.metadata.Table table) {
        return false;
    }

    default PrimitiveTypeInfo getIntervalYearMonthTypeInfo() {
        throw new UnsupportedOperationException(
                "INTERVAL YEAR MONTH type not supported until 1.2.0");
    }

    default PrimitiveTypeInfo getIntervalDayTimeTypeInfo() {
        throw new UnsupportedOperationException("INTERVAL DAY TIME type not supported until 1.2.0");
    }

    default boolean isIntervalYearMonthType(
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory) {
        return false;
    }

    default boolean isIntervalDayTimeType(
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory) {
        return false;
    }

    void registerTemporaryFunction(String funcName, Class funcClass);

    void loadPartition(
            Hive hive,
            Path loadPath,
            String tableName,
            Map<String, String> partSpec,
            boolean isSkewedStoreAsSubdir,
            boolean replace,
            boolean isSrcLocal);

    void loadTable(Hive hive, Path loadPath, String tableName, boolean replace, boolean isSrcLocal);

    /** Get struct field names from a StructTypeInfo, compatible across Hive versions. */
    List<String> getStructFieldNames(StructTypeInfo structTypeInfo);

    /** Get struct field type infos from a StructTypeInfo, compatible across Hive versions. */
    List<TypeInfo> getStructFieldTypeInfos(StructTypeInfo structTypeInfo);

    /**
     * Initialize a Deserializer with the given configuration and properties, compatible across Hive
     * versions.
     */
    void initializeSerDe(
            Deserializer deserializer,
            Configuration conf,
            Properties tableProperties,
            Properties partitionProperties)
            throws SerDeException;

    /** Get column statistics for a table, compatible across Hive versions. */
    List<ColumnStatisticsObj> getTableColumnStatistics(
            IMetaStoreClient client,
            String databaseName,
            String tableName,
            List<String> columnNames)
            throws TException;

    /** Get column statistics for partitions, compatible across Hive versions. */
    Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
            IMetaStoreClient client,
            String dbName,
            String tableName,
            List<String> partNames,
            List<String> colNames)
            throws TException;

    /**
     * Create a PrincipalDesc instance. The class moved from {@code
     * org.apache.hadoop.hive.ql.plan.PrincipalDesc} to {@code
     * org.apache.hadoop.hive.ql.ddl.privilege.PrincipalDesc} in Hive 4.
     */
    Object createPrincipalDesc(String principalName, PrincipalType principalType);

    /**
     * Walk an expression tree using PreOrderWalker. Hive 4 changed PreOrderWalker to require
     * SemanticDispatcher instead of Dispatcher.
     */
    void walkExpressionTree(Node expression, Dispatcher dispatcher) throws SemanticException;

    /**
     * Get a GenericUDAFEvaluator for windowing functions (LEAD/LAG). Hive 4 added a 5th boolean
     * parameter ({@code respectNulls}).
     */
    default GenericUDAFEvaluator getGenericWindowingEvaluator(
            String functionName,
            List<ObjectInspector> argumentOIs,
            boolean isDistinct,
            boolean isAllColumns)
            throws SemanticException {
        return org.apache.hadoop.hive.ql.exec.FunctionRegistry.getGenericWindowingEvaluator(
                functionName, argumentOIs, isDistinct, isAllColumns);
    }
}
