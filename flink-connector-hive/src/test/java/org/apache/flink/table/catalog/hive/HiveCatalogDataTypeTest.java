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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.catalog.hive.util.Constants.IDENTIFIER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for data type mappings in HiveCatalog. */
class HiveCatalogDataTypeTest {

    private static HiveCatalog catalog;

    protected final String db1 = "db1";
    protected final String db2 = "db2";

    protected final String t1 = "t1";
    protected final String t2 = "t2";
    protected final ObjectPath path1 = new ObjectPath(db1, t1);
    protected final ObjectPath path2 = new ObjectPath(db2, t2);
    protected final ObjectPath path3 = new ObjectPath(db1, t2);

    @BeforeAll
    static void init() {
        catalog = HiveTestUtils.createHiveCatalog();
        catalog.open();
    }

    @AfterEach
    void cleanup() throws Exception {
        if (catalog.tableExists(path1)) {
            catalog.dropTable(path1, true);
        }
        if (catalog.tableExists(path2)) {
            catalog.dropTable(path2, true);
        }
        if (catalog.tableExists(path3)) {
            catalog.dropTable(path3, true);
        }
        if (catalog.functionExists(path1)) {
            catalog.dropFunction(path1, true);
        }
        if (catalog.databaseExists(db1)) {
            catalog.dropDatabase(db1, true);
        }
        if (catalog.databaseExists(db2)) {
            catalog.dropDatabase(db2, true);
        }
    }

    @AfterAll
    static void closeup() {
        if (catalog != null) {
            catalog.close();
        }
    }

    @Test
    void testDataTypes() throws Exception {
        DataType[] types =
                new DataType[] {
                    DataTypes.TINYINT(),
                    DataTypes.SMALLINT(),
                    DataTypes.INT(),
                    DataTypes.BIGINT(),
                    DataTypes.FLOAT(),
                    DataTypes.DOUBLE(),
                    DataTypes.BOOLEAN(),
                    DataTypes.STRING(),
                    DataTypes.BYTES(),
                    DataTypes.DATE(),
                    DataTypes.TIMESTAMP(9),
                    DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH),
                    DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH),
                    DataTypes.DECIMAL(5, 3)
                };

        verifyDataTypes(types);
    }

    @Test
    void testNonSupportedBinaryDataTypes() throws Exception {
        DataType[] types = new DataType[] {DataTypes.BINARY(BinaryType.MAX_LENGTH)};

        CatalogTable table = createCatalogTable(types);

        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.createTable(path1, table, false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testNonSupportedVarBinaryDataTypes() throws Exception {
        DataType[] types = new DataType[] {DataTypes.VARBINARY(20)};

        CatalogTable table = createCatalogTable(types);

        catalog.createDatabase(db1, createDb(), false);

        assertThatThrownBy(() -> catalog.createTable(path1, table, false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testCharTypeLength() throws Exception {
        DataType[] types = new DataType[] {DataTypes.CHAR(HiveChar.MAX_CHAR_LENGTH + 1)};

        assertThatThrownBy(() -> verifyDataTypes(types)).isInstanceOf(CatalogException.class);
    }

    @Test
    void testVarCharTypeLength() throws Exception {
        DataType[] types = new DataType[] {DataTypes.VARCHAR(HiveVarchar.MAX_VARCHAR_LENGTH + 1)};

        assertThatThrownBy(() -> verifyDataTypes(types)).isInstanceOf(CatalogException.class);
    }

    @Test
    void testComplexDataTypes() throws Exception {
        DataType[] types =
                new DataType[] {
                    DataTypes.ARRAY(DataTypes.DOUBLE()),
                    DataTypes.MAP(DataTypes.FLOAT(), DataTypes.BIGINT()),
                    DataTypes.ROW(
                            DataTypes.FIELD("0", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("1", DataTypes.BOOLEAN()),
                            DataTypes.FIELD("2", DataTypes.DATE())),

                    // nested complex types
                    DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                    DataTypes.MAP(
                            DataTypes.STRING(),
                            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                    DataTypes.ROW(
                            DataTypes.FIELD("3", DataTypes.ARRAY(DataTypes.DECIMAL(5, 3))),
                            DataTypes.FIELD(
                                    "4", DataTypes.MAP(DataTypes.TINYINT(), DataTypes.SMALLINT())),
                            DataTypes.FIELD(
                                    "5",
                                    DataTypes.ROW(DataTypes.FIELD("3", DataTypes.TIMESTAMP(9)))))
                };

        verifyDataTypes(types);
    }

    private CatalogTable createCatalogTable(DataType[] types) {
        String[] colNames = new String[types.length];

        List<Column> columns = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++) {
            columns.add(
                    Column.physical(
                            String.format("%s_%d", types[i].toString().toLowerCase(), i),
                            types[i]));
        }

        ResolvedSchema resolvedSchema = ResolvedSchema.of(columns);
        Schema schema = Schema.newBuilder().fromResolvedSchema(resolvedSchema).build();

        return new ResolvedCatalogTable(
                CatalogTable.of(
                        schema,
                        "",
                        new ArrayList<>(),
                        new HashMap<String, String>() {
                            {
                                put("is_streaming", "false");
                                put(FactoryUtil.CONNECTOR.key(), IDENTIFIER);
                            }
                        }),
                resolvedSchema);
    }

    private void verifyDataTypes(DataType[] types) throws Exception {
        CatalogTable table = createCatalogTable(types);

        catalog.createDatabase(db1, createDb(), false);
        catalog.createTable(path1, table, false);

        assertThat(catalog.getTable(path1).getUnresolvedSchema())
                .isEqualTo(table.getUnresolvedSchema());
    }

    private static CatalogDatabase createDb() {
        return new CatalogDatabaseImpl(
                new HashMap<String, String>() {
                    {
                        put("k1", "v1");
                    }
                },
                "");
    }
}
