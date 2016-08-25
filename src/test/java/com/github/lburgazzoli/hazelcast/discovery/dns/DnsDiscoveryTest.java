/*
 * Copyright 2015 Luca Burgazzoli
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
package com.github.lburgazzoli.hazelcast.discovery.dns;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class DnsDiscoveryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DnsDiscoveryTest.class);

    @Test
    public void discoveryProviderTest() throws Exception {
        Map<String, Comparable> properties = new HashMap<>();
        properties.put("serviceName", "_xmpp-server._tcp.gmail.com");

        DiscoveryNode local = new SimpleDiscoveryNode(new Address("127.0.0.1", 1010));
        DiscoveryStrategyFactory factory = new DnsDiscoveryStrategyFactory();
        DiscoveryStrategy provider = factory.newDiscoveryStrategy(local, null, properties);

        provider.start();

        Iterable<DiscoveryNode> nodes = provider.discoverNodes();
        Assert.assertNotNull(nodes);
        Assert.assertTrue("Empty DiscoveryNode list", nodes.iterator().hasNext());

        for(DiscoveryNode node : nodes) {
            LOGGER.info("Node -> {}", node.getPublicAddress());
        }
    }

    @Test
    public void hazelcastConfigurationTest() throws Exception {
        Config config = loadConfig("test-hazelcast-discovery-dns.xml");
        DiscoveryConfig discovery = config.getNetworkConfig().getJoin().getDiscoveryConfig();
        Collection<DiscoveryStrategyConfig> discoveryConfs = discovery.getDiscoveryStrategyConfigs();

        Assert.assertFalse("No DiscoveryStrategy configured", discoveryConfs.isEmpty());
        Assert.assertEquals(1, discoveryConfs.size());

        DiscoveryStrategyConfig discoveryConf = discoveryConfs.iterator().next();

        Assert.assertEquals("_hz._tcp.localdomain", discoveryConf.getProperties().get(DnsDiscovery.PROPERTY_SERVICE_NAME.key()));
        Assert.assertEquals(DnsDiscoveryStrategy.class.getName(), discoveryConf.getClassName());
    }

    // *************************************************************************
    //
    // *************************************************************************

    private Config loadConfig(String fileName) throws IOException {

        try(InputStream in = DnsDiscoveryTest.class.getClassLoader().getResourceAsStream(fileName)) {
            Config config = new XmlConfigBuilder(in).build();

            InterfacesConfig interfaces = config.getNetworkConfig().getInterfaces();
            interfaces.clear();
            interfaces.setEnabled(true);
            interfaces.addInterface("127.0.0.1");

            return config;
        }
    }
}
