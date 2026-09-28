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

import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;
import java.util.Stack;

/**
 * A rule dispatcher compatible with both Hive 2.x/3.x and 4.x. Hive 4 changed {@code
 * DefaultRuleDispatcher} to require {@code SemanticNodeProcessor}/{@code SemanticRule} which don't
 * exist in earlier versions. This dispatcher uses the base {@code NodeProcessor}/{@code Rule}
 * interfaces directly.
 */
public class HiveParserRuleDispatcher implements Dispatcher {

    private final NodeProcessor defaultProcessor;
    private final Map<Rule, NodeProcessor> procRules;
    private final NodeProcessorCtx procCtx;

    public HiveParserRuleDispatcher(
            NodeProcessor defaultProcessor,
            Map<Rule, NodeProcessor> procRules,
            NodeProcessorCtx procCtx) {
        this.defaultProcessor = defaultProcessor;
        this.procRules = procRules;
        this.procCtx = procCtx;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> ndStack, Object... nodeOutputs)
            throws SemanticException {
        Rule bestRule = null;
        int bestCost = Integer.MAX_VALUE;
        for (Rule rule : procRules.keySet()) {
            int cost = rule.cost(ndStack);
            if (cost >= 0 && cost <= bestCost) {
                bestCost = cost;
                bestRule = rule;
            }
        }
        try {
            if (bestRule == null) {
                return defaultProcessor.process(nd, ndStack, procCtx, nodeOutputs);
            } else {
                return procRules.get(bestRule).process(nd, ndStack, procCtx, nodeOutputs);
            }
        } catch (SemanticException e) {
            throw e;
        } catch (Exception e) {
            throw new SemanticException(e.getMessage(), e);
        }
    }
}
