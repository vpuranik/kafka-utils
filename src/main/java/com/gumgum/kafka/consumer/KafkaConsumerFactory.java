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

import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

/**
 * Connection Factory for Kafka Consumers.
 *
 * @author Vaibhav Puranik
 */
@ManagedResource(objectName = "GumGum:name=kafkaConsumerFactory", description = "Initialize Consumer Configuration")
public class KafkaConsumerFactory  extends BasePoolableObjectFactory<ConsumerConnector> {
    private ConsumerConfig consumerConfig;
    private String zkConnectionString;
    private String groupName;
    private Integer fetchSize;

    public KafkaConsumerFactory(String zkConnectionString, String groupName, Integer fetchSize) {
        this.zkConnectionString = zkConnectionString;
        this.groupName = groupName;
        this.fetchSize = fetchSize;
        initializeConsumerConfig();
    }

    @Override
    public ConsumerConnector makeObject() throws Exception {
        if (this.consumerConfig == null) {
            this.initializeConsumerConfig();
        }
        return Consumer.createJavaConsumerConnector(consumerConfig);
    }


    @ManagedOperation(description = "Initialize Consumer Configuration")
    public void initializeConsumerConfig() {
        Properties props = new Properties();
        props.put("zk.connect", zkConnectionString);
        props.put("zk.connectiontimeout.ms", "1000000");
        props.put("groupid", groupName);
        props.put("fetch.size", fetchSize.toString());
        this.consumerConfig = new ConsumerConfig(props);
    }
}
