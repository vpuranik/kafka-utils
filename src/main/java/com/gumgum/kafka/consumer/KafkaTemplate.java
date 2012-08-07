/**
* Copyright 2012 GumGum Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.gumgum.kafka.consumer;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import com.google.common.collect.ImmutableMap;

/**
 * Spring style template for boiler plate kafka code.
 * @author Vaibhav Puranik
 *
 */
public class KafkaTemplate {
    private static final Log LOGGER = LogFactory.getLog("com.gumgum.kafka.KafkaTemplate");
    private GenericObjectPool<ConsumerConnector> kafkaConsumerPool;

    public KafkaTemplate() {
    }

    public void executeWithBatch(String topic, int batchSize, MessageCallback action) {
        ConsumerConnector consumerConnector = null;
        try {
            consumerConnector = this.kafkaConsumerPool.borrowObject();
            Map<String, List<KafkaStream<Message>>> topicMessageStreams = consumerConnector.createMessageStreams(ImmutableMap.of(topic, 1));
            List<KafkaStream<Message>> streams = topicMessageStreams.get(topic);
            int counter = 0;
            for (KafkaStream<Message> stream : streams) {
                for (MessageAndMetadata<Message> message: stream) {
                    try {
                        String strMessage = convertToUtf8String(message.message().payload());
                        if (isBlank(strMessage)) {
                            continue;
                        }
                        action.processMessage(strMessage);
                        counter++;
                        if (counter % batchSize == 0) {
                            break;
                        }
                    } catch (Exception e) {
                        LOGGER.error(e);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (consumerConnector != null) {
                try {
                    //consumerConnector.commitOffsets();
                    this.kafkaConsumerPool.returnObject(consumerConnector);
                } catch (Exception e) {
                    LOGGER.error(e);
                }
            }
        }
    }

    public void shutdown() {
        for (int i = 0; i < this.kafkaConsumerPool.getMaxActive(); i++) {
            try {
                this.kafkaConsumerPool.borrowObject().shutdown();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String convertToUtf8String(ByteBuffer buffer) throws Exception {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, "UTF-8");
    }

    public GenericObjectPool<ConsumerConnector> getKafkaConsumerPool() {
        return kafkaConsumerPool;
    }

    public void setKafkaConsumerPool(GenericObjectPool<ConsumerConnector> kafkaConsumerPool) {
        this.kafkaConsumerPool = kafkaConsumerPool;
    }
}
