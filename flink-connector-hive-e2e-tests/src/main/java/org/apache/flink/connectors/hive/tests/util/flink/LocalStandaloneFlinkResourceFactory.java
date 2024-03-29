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

package org.apache.flink.connectors.hive.tests.util.flink;

import org.apache.flink.test.parameters.ParameterProperty;
import org.apache.flink.test.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/** A {@link FlinkResourceFactory} for the {@link LocalStandaloneFlinkResource}. */
public final class LocalStandaloneFlinkResourceFactory implements FlinkResourceFactory {
    private static final Logger LOG =
            LoggerFactory.getLogger(LocalStandaloneFlinkResourceFactory.class);

    private static final ParameterProperty<Path> DISTRIBUTION_LOG_BACKUP_DIRECTORY =
            new ParameterProperty<>("logBackupDir", Paths::get);

    @Override
    public FlinkResource create(FlinkResourceSetup setup) {
        Optional<Path> logBackupDirectory = DISTRIBUTION_LOG_BACKUP_DIRECTORY.get();
        if (!logBackupDirectory.isPresent()) {
            LOG.warn(
                    "Property {} not set, logs will not be backed up in case of test failures.",
                    DISTRIBUTION_LOG_BACKUP_DIRECTORY.getPropertyName());
        }
        return new LocalStandaloneFlinkResource(
                FileUtils.findFlinkDist(), logBackupDirectory.orElse(null), setup);
    }
}
