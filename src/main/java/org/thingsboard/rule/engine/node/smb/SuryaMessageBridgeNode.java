/**
 * Copyright © 2018-2025 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.thingsboard.rule.engine.node.smb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.StatefulRedisConnection;

import org.thingsboard.common.util.JacksonUtil;
import org.thingsboard.rule.engine.api.*;
import org.thingsboard.server.common.msg.TbMsg;
import org.thingsboard.server.common.msg.TbMsgMetaData;
import org.thingsboard.server.common.data.plugin.ComponentType;

import java.util.Collections;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RuleNode(type = ComponentType.EXTERNAL, name = "Surya Message Bridge", configClazz = SuryaMessageBridgeConfig.class, nodeDescription = "Sends telemetry to Redis Stream", nodeDetails = "Wraps and sends incoming telemetry to a Redis Stream.")
public class SuryaMessageBridgeNode implements TbNode {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisCommands<String, String> syncCommands;
    private ObjectMapper mapper;
    private String redisURI;
    private String streamKey;
    private String maxLen;
    private static final Logger log = LoggerFactory.getLogger(SuryaMessageBridgeNode.class);

    @Override
    public void init(TbContext ctx, TbNodeConfiguration config) throws TbNodeException {
        log.info("Initializing SMB");

        SuryaMessageBridgeConfig conf;
        try {
            conf = JacksonUtil.convertValue(config.getData(), SuryaMessageBridgeConfig.class);
            // You can now access config.getRedisHost() etc.
        } catch (Exception e) {
            throw new TbNodeException(e);
        }

        try {
            this.redisURI = conf.getRedisURI();
            log.info("Initializing SuryaMessageBridgeNode with Redis URI: {}", this.redisURI);
            if (this.redisURI == null || this.redisURI.isEmpty()) {
                throw new TbNodeException("Redis URI cannot be null or empty");
            }
            log.info("Connecting to Redis at {}", this.redisURI);

            this.redisClient = RedisClient.create(this.redisURI);
            this.connection = this.redisClient.connect();
        } catch (RedisConnectionException e) {
            log.error("Redis connection failed: {}", e.getMessage(), e);
            throw new RuntimeException("Cannot start SuryaMessageBridgeNode without Redis");
        }

        this.streamKey = conf.getStreamKey();
        this.mapper = new ObjectMapper();
        this.syncCommands = connection.sync();
        this.maxLen = conf.getMaxlen();
    }

    @Override
    public void onMsg(TbContext ctx, TbMsg msg) {
        log.debug("Surya Node received msg: {}", msg.getData());
        log.debug("Surya Node received metadata: {}", msg.getMetaData());

        try {
            TbMsgMetaData metadata = msg.getMetaData();

            // Build metadata object
            Map<String, String> metadataMap = Map.of(
                    "deviceType", metadata.getValue("deviceType"),
                    "deviceName", metadata.getValue("deviceName"));

            // Parse telemetry data from msg.getData()
            JsonNode dataNode = mapper.readTree(msg.getData());

            // Build main JSON object
            ObjectNode mainObject = mapper.createObjectNode();
            mainObject.set("metadata", mapper.valueToTree(metadataMap));
            mainObject.set("data", dataNode);

            // Convert to JSON string
            String json = mapper.writeValueAsString(mainObject);

            log.debug("Surya: JSON data {}", json);

            // Send to Redis stream
            // XADD with MAXLEN
        syncCommands.xadd(
            streamKey,
            XAddArgs.Builder.maxlen(Long.parseLong(maxLen)).approximateTrimming(true), // keep stream short
            Collections.singletonMap("data", json)
        );

            ctx.ack(msg);
        } catch (Exception e) {
            ctx.tellFailure(msg, e);
        }
    }

    @Override
    public void destroy() {
        connection.close();
        redisClient.shutdown();
    }
}
