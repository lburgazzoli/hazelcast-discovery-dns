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

import com.hazelcast.config.NetworkConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;

import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;

import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

public class DnsDiscoveryStrategy extends AbstractDiscoveryStrategy {
    private static final ILogger LOGGER;
    private static final String[] ATTRIBUTE_IDS;
    private static final Hashtable<String, String> ENV;

    static {
        LOGGER = Logger.getLogger(DnsDiscoveryStrategy.class);
        ATTRIBUTE_IDS = new String[] { "SRV" };

        ENV = new Hashtable<>();
        ENV.put("java.naming.factory.initial", "com.sun.jndi.dns.DnsContextFactory");
        ENV.put("java.naming.provider.url", "dns:");
    }

    private final String serviceName;

    public DnsDiscoveryStrategy(ILogger logger, Map<String, Comparable> properties) {
        super(logger, properties);
        
        this.serviceName = (String) getOrNull(DnsDiscovery.PROPERTY_SERVICE_NAME);
        if(this.serviceName == null) {
            throw new RuntimeException("Property 'serviceName' is missing in the DNS provider");
        }
    }

    // *************************************************************************
    // DiscoveryStrategy
    // *************************************************************************

    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        List<DiscoveryNode> servers = Collections.emptyList();

        try {
            DirContext ctx = new InitialDirContext(ENV);
            NamingEnumeration<?> resolved = ctx.getAttributes(serviceName, ATTRIBUTE_IDS).get("srv").getAll();
            
            if (resolved.hasMore()) {
                servers = new LinkedList<>();

                while (resolved.hasMore()) {
                    Address address = srvRecordToAddress((String)resolved.next());

                    if (LOGGER.isFinestEnabled()) {
                        LOGGER.finest("Found node ip-address is: " + address);
                    }
                    
                    servers.add(new SimpleDiscoveryNode(address));
                }
            } else {
                LOGGER.warning("Could not find any service for serviceName '" + serviceName + "'");
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not resolve services via DNS", e);
        }

        return servers;
    }

    @Override
    public void destroy() {
    }


    // *************************************************************************
    // DiscoveryStrategy
    // *************************************************************************

    private Address srvRecordToAddress(String record) throws Exception {
        String[] items = record.split(" ");
        String host = items[3].trim();
        String port = items[2].trim();

        return new Address(
            items[3].trim(), 
            port.length() > 0 
                ? Integer.parseInt(port) 
                : NetworkConfig.DEFAULT_PORT
        );
    }
}
