/*
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
package io.github.aload0.presto.eventlistener.producer;

import com.facebook.presto.spi.eventlistener.QueryContext;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.QueryMetadata;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestPrestoEventProducer
{
    @Test
    public void testProducer()
    {
        try (TestingBroker broker = new TestingBroker()) {
            PrestoEventProducer producer = PrestoEventProducer.create(createTestingConfig(broker));
            producer.queryCreated(createQueryCreatedEvent());

            List<ConsumerRecord<String, String>> consumed = consume(broker);
            assertEquals(1, consumed.size());
            ConsumerRecord<String, String> record = consumed.get(0);
            System.out.printf(
                    "Record: offset = %d, key = %s, value = %s\n",
                    record.offset(),
                    record.key(),
                    record.value());
        }
        catch (Exception e) {
            fail("Failed", e);
        }
    }

    private Map<String, String> createTestingConfig(TestingBroker broker)
    {
        Map<String, String> map = new HashMap<>();
        map.put(PrestoEventProducer.NAME + ".coordinator", "localhost:8080");
        map.put(PrestoEventProducer.NAME + ".topic", broker.getTopic());
        map.put(PrestoEventProducer.NAME + ".kafka.bootstrap.servers", broker.getAddress());
        return map;
    }

    private List<ConsumerRecord<String, String>> consume(TestingBroker broker)
    {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", broker.getAddress());
        consumerProps.setProperty("group.id", "group0");
        consumerProps.setProperty("client.id", "consumer0");
        consumerProps.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // to make sure the consumer starts from the beginning of the topic
        consumerProps.put("auto.offset.reset", "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(broker.getTopic()));
        ConsumerRecords<String, String> records = consumer.poll(5000);
        List<ConsumerRecord<String, String>> result = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            result.add(record);
        }
        return result;
    }

    private QueryCreatedEvent createQueryCreatedEvent()
    {
        Instant time = Instant.parse("2007-12-03T10:15:30.00Z");
        QueryMetadata metadata = new QueryMetadata(
                "20071203_101530_00008_tests",
                Optional.empty(),
                "select 1",
                "QUEUE",
                URI.create("http://localhost:8080/"),
                Optional.empty(),
                Optional.empty());
        QueryContext context = new QueryContext(
                "root",
                Optional.empty(),
                Optional.of("localhost"),
                Optional.of("StatementClientV1/25af413"),
                Optional.empty(),
                Collections.emptySet(),
                Optional.empty(),
                Optional.of("hive"),
                Optional.empty(),
                Optional.empty(),
                Collections.emptyMap(),
                "127.0.0.1",
                "0.198",
                "production");
        return new QueryCreatedEvent(time, context, metadata);
    }

    static class TestingBroker
            implements AutoCloseable
    {
        private final String topic = "test";
        private final String address;
        private final EmbeddedZookeeper zkServer;
        private final ZkClient zkClient;
        private final KafkaServer broker;

        TestingBroker()
        {
            this.zkServer = new EmbeddedZookeeper();
            String zkAddress = "127.0.0.1:" + zkServer.port();
            this.zkClient = new ZkClient(zkAddress, 30000, 30000, ZKStringSerializer$.MODULE$);
            ZkUtils zkUtils = ZkUtils.apply(zkClient, false);

            Properties brokerProps = TestUtils.createBrokerConfig(
                    0,
                    zkAddress,
                    true,
                    true,
                    0,
                    scala.Option.apply(null),
                    scala.Option.apply(null),
                    scala.Option.apply(null),
                    true,
                    false,
                    0,
                    false,
                    0,
                    false,
                    1,
                    scala.Option.apply(null),
                    1);
            KafkaConfig config = new KafkaConfig(brokerProps);
            broker = TestUtils.createServer(config, new MockTime());
            address = "127.0.0.1:" + TestUtils.boundPort(broker, SecurityProtocol.PLAINTEXT);

            AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        }

        @Override
        public void close() throws Exception
        {
            broker.shutdown();
            zkClient.close();
            zkServer.shutdown();
        }

        String getTopic()
        {
            return topic;
        }

        String getAddress()
        {
            return address;
        }
    }
}
