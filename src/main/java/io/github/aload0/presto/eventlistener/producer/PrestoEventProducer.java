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

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.QueryCompletedEvent;
import com.facebook.presto.spi.eventlistener.QueryCreatedEvent;
import com.facebook.presto.spi.eventlistener.SplitCompletedEvent;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

class PrestoEventProducer
        implements EventListener
{
    private static final Logger LOG = LoggerFactory.getLogger(PrestoEventProducer.class);

    static PrestoEventProducer create(Map<String, String> config)
    {
        return new PrestoEventProducer(config);
    }

    static final String NAME = "presto-event-producer";

    private static String requireNotBlank(String s, String message)
    {
        if (s == null || s.trim().length() == 0) {
            throw new IllegalArgumentException(message);
        }
        return s;
    }

    private static ObjectMapper createObjectMapper()
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.registerModule(new Jdk8Module());
        return mapper;
    }

    private final String coordinator;
    private final String topic;
    private final Producer<String, String> producer;
    private final ObjectMapper mapper;

    private PrestoEventProducer(Map<String, String> config)
    {
        this.coordinator = requireNotBlank(config.get(NAME + ".coordinator"), "No coordinator specified");
        this.topic = requireNotBlank(config.get(NAME + ".topic"), "No topic specified");
        this.producer = KafkaProducerFactory.create(config);
        this.mapper = createObjectMapper();
    }

    private void send(PrestoEventHolder event)
    {
        try {
            String value = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, value));
        }
        catch (Exception e) {
            LOG.error("Sending event {} error", e);
        }
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        send(new PrestoEventHolder(queryCreatedEvent, PrestoEventHolder.Type.QUERY_CREATED, coordinator));
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        send(new PrestoEventHolder(queryCompletedEvent, PrestoEventHolder.Type.QUERY_COMPLETED, coordinator));
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        send(new PrestoEventHolder(splitCompletedEvent, PrestoEventHolder.Type.SPLIT_COMPLETED, coordinator));
    }

    static class PrestoEventHolder
    {
        enum Type
        {
            QUERY_CREATED,
            QUERY_COMPLETED,
            SPLIT_COMPLETED
        }

        @JsonProperty
        private final Instant time;
        @JsonProperty
        private final Object event;
        @JsonProperty
        private final Type type;
        @JsonProperty
        private final String coordinator;

        PrestoEventHolder(Object event, Type type, String coordinator)
        {
            this.time = Instant.now();
            this.event = event;
            this.type = type;
            this.coordinator = coordinator;
        }
    }

    static class KafkaProducerFactory
    {
        static KafkaProducer<String, String> create(Map<String, String> config)
        {
            return new KafkaProducerFactory(config).create();
        }

        private final String prefix;
        private final Map<String, String> config;
        private final Properties properties;

        private KafkaProducerFactory(Map<String, String> config)
        {
            this.prefix = NAME + ".kafka.";
            this.config = config;
            this.properties = new Properties();

            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        }

        private void exchange(String key, String defaultValue)
        {
            properties.put(key, config.getOrDefault(prefix + key, defaultValue));
        }

        private KafkaProducer<String, String> create()
        {
            requireNotBlank(config.get(prefix + "bootstrap.servers"), "Bootstrap servers not provided");
            exchange("bootstrap.servers", "");
            exchange("acks", "1");
            exchange("retries", "3");
            exchange("batch.size", "16384");
            exchange("linger.ms", "1");
            exchange("buffer.memory", "33554432");
            return new KafkaProducer<>(properties);
        }
    }
}