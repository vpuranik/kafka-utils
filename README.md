Apache Kafka is a high throughput distributed messaging system. This project defines some utilities for connecting to kafka from java. It defines Spring style templates for producers and consumers.

The consumer template uses generic object pool. If you want to use spring's dependency injection, here is how you will the classes defined in this project:

    <bean id="kafkaConsumerFactory" class="com.gumgum.kafka.consumer.KafkaConsumerFactory">
        <constructor-arg value="${kafka.zookeeper.connection}" />
        <constructor-arg value="${adevents.consumer.group.name}"/>
        <constructor-arg value="${adevents.fetch.size}"/>
    </bean>

    <bean id="kafkaConsumerPool" class="org.apache.commons.pool.impl.GenericObjectPool">
        <!-- The ObjectPool uses a ConnectionFactory to build new connections -->
        <constructor-arg ref="kafkaConsumerFactory" />
        <property name="maxActive" value="3" />
    </bean>

    <bean id="kafkaTemplate" class="com.gumgum.kafka.consumer.KafkaTemplate">
        <property name="kafkaConsumerPool" ref="kafkaConsumerPool"/>
    </bean>


