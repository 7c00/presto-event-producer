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

import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;

import java.util.Collections;
import java.util.Map;

public class PrestoEventProducerPlugin
        implements Plugin
{
    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return Collections.singletonList(new EventListenerFactory()
        {
            @Override
            public String getName()
            {
                return PrestoEventProducer.NAME;
            }

            @Override
            public EventListener create(Map<String, String> config)
            {
                return PrestoEventProducer.create(config);
            }
        });
    }
}
