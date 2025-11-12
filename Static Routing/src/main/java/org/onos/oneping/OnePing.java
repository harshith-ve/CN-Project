/*
 * Copyright 2015 Open Networking Foundation
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
package org.onos.oneping;

import org.onlab.packet.Ethernet;
import org.onlab.packet.IPv4;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.Ip4Prefix;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.PortNumber;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.host.HostService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyGraph;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.net.topology.TopologyVertex;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;


@Component(immediate = true)
public class OnePing {

    private static final Logger log = LoggerFactory.getLogger(OnePing.class);

    private static final int PACKET_PROCESSOR_PRIORITY = 500;
    private static final int FLOW_PRIORITY = 50_000;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    private final PacketProcessor packetProcessor = new DynamicPacketProcessor();

    private final ConcurrentMap<FlowKey, PathState> activePaths = new ConcurrentHashMap<>();

    private final TopologyListener topologyListener = new InternalTopologyListener();
    private ApplicationId appId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.onosproject.oneping");
        packetService.addProcessor(packetProcessor, PACKET_PROCESSOR_PRIORITY);
        packetService.requestPackets(DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4).build(), PacketPriority.REACTIVE, appId);
        topologyService.addListener(topologyListener);
        log.info("Dynamic routing application started");
    }

    @Deactivate
    protected void deactivate() {
        packetService.removeProcessor(packetProcessor);
        packetService.cancelPackets(DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4).build(), PacketPriority.REACTIVE, appId);
        topologyService.removeListener(topologyListener);

        flowRuleService.removeFlowRulesById(appId);
        activePaths.clear();
        log.info("Dynamic routing application stopped");
    }

    private void resetPaths() {
        flowRuleService.removeFlowRulesById(appId);
        activePaths.clear();
    }

    private PathState installPath(FlowKey key, Host srcHost, Host dstHost,
                                  List<Link> pathLinks, Ip4Address srcIp, Ip4Address dstIp) {
        Map<DeviceId, PortNumber> forwarding = buildForwardingMap(srcHost, dstHost, pathLinks);
        FlowRule[] rules = buildFlowRules(forwarding, srcIp, dstIp);
        PathState candidate = new PathState(forwarding);

        PathState installed = activePaths.putIfAbsent(key, candidate);
        if (installed != null) {
            return installed;
        }

        flowRuleService.applyFlowRules(rules);
        log.debug("Installed dynamic path {} -> {} ({})", srcIp, dstIp, formatPath(pathLinks));
        return candidate;
    }

    private Map<DeviceId, PortNumber> buildForwardingMap(Host srcHost, Host dstHost, List<Link> pathLinks) {
        LinkedHashMap<DeviceId, PortNumber> forwarding = new LinkedHashMap<>();
        if (pathLinks == null || pathLinks.isEmpty()) {
            forwarding.put(srcHost.location().deviceId(), dstHost.location().port());
            forwarding.put(dstHost.location().deviceId(), dstHost.location().port());
            return forwarding;
        }

        forwarding.put(srcHost.location().deviceId(), pathLinks.get(0).src().port());
        for (Link link : pathLinks) {
            forwarding.put(link.src().deviceId(), link.src().port());
        }
        forwarding.put(dstHost.location().deviceId(), dstHost.location().port());
        return forwarding;
    }

    private FlowRule[] buildFlowRules(Map<DeviceId, PortNumber> forwarding,
                                      Ip4Address srcIp, Ip4Address dstIp) {
        List<FlowRule> rules = new ArrayList<>();
        forwarding.forEach((deviceId, outPort) -> {
            TrafficSelector selector = DefaultTrafficSelector.builder()
                    .matchEthType(Ethernet.TYPE_IPV4)
                    .matchIPSrc(Ip4Prefix.valueOf(srcIp, 32))
                    .matchIPDst(Ip4Prefix.valueOf(dstIp, 32))
                    .build();

            TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                    .setOutput(outPort)
                    .build();

            FlowRule rule = DefaultFlowRule.builder()
                    .forDevice(deviceId)
                    .withSelector(selector)
                    .withTreatment(treatment)
                    .withPriority(FLOW_PRIORITY)
                    .makePermanent()
                    .fromApp(appId)
                    .build();
            rules.add(rule);
        });
        return rules.toArray(new FlowRule[0]);
    }

    private List<Link> computePath(Host srcHost, Host dstHost) {
        if (srcHost == null || dstHost == null) {
            return null;
        }
        DeviceId srcDevice = srcHost.location().deviceId();
        DeviceId dstDevice = dstHost.location().deviceId();
        if (Objects.equals(srcDevice, dstDevice)) {
            return Collections.emptyList();
        }

        Topology topology = topologyService.currentTopology();
        TopologyGraph graph = topologyService.getGraph(topology);
        TopologyVertex srcVertex = findVertex(graph, srcDevice);
        TopologyVertex dstVertex = findVertex(graph, dstDevice);
        if (srcVertex == null || dstVertex == null) {
            return null;
        }

        Map<TopologyVertex, Integer> distance = new HashMap<>();
        Map<TopologyVertex, TopologyEdge> previous = new HashMap<>();
        ArrayDeque<TopologyVertex> queue = new ArrayDeque<>();

        distance.put(srcVertex, 0);
        queue.add(srcVertex);

        while (!queue.isEmpty()) {
            TopologyVertex current = queue.poll();
            if (current.equals(dstVertex)) {
                break;
            }

            for (TopologyEdge edge : graph.getEdgesFrom(current)) {
                TopologyVertex neighbour = edge.dst();
                if (distance.containsKey(neighbour)) {
                    continue;
                }
                distance.put(neighbour, distance.get(current) + 1);
                previous.put(neighbour, edge);
                queue.add(neighbour);
            }
        }

        if (!distance.containsKey(dstVertex)) {
            return null;
        }

        List<Link> links = new ArrayList<>();
        TopologyVertex walk = dstVertex;
        while (!walk.equals(srcVertex)) {
            TopologyEdge edge = previous.get(walk);
            if (edge == null) {
                return null;
            }
            links.add(0, edge.link());
            walk = edge.src();
        }

        return links;
    }

    private TopologyVertex findVertex(TopologyGraph graph, DeviceId deviceId) {
        for (TopologyVertex vertex : graph.getVertexes()) {
            if (vertex.deviceId().equals(deviceId)) {
                return vertex;
            }
        }
        return null;
    }

    private void forwardInitialPacket(PacketContext context, Map<DeviceId, PortNumber> forwarding) {
        InboundPacket packet = context.inPacket();
        DeviceId deviceId = packet.receivedFrom().deviceId();
        PortNumber outPort = forwarding.get(deviceId);
        if (outPort == null) {
            return;
        }
        context.treatmentBuilder().setOutput(outPort);
        context.send();
    }

    private String formatPath(List<Link> pathLinks) {
        if (pathLinks == null || pathLinks.isEmpty()) {
            return "[direct]";
        }
        return pathLinks.stream()
                .map(link -> String.format("%s/%s->%s/%s",
                        link.src().deviceId(), link.src().port(),
                        link.dst().deviceId(), link.dst().port()))
                .collect(Collectors.joining(" | "));
    }

    private final class InternalTopologyListener implements TopologyListener {
        @Override
        public void event(TopologyEvent event) {
            if (event == null) {
                return;
            }
            if (event.type() == TopologyEvent.Type.TOPOLOGY_CHANGED) {
                log.info("Topology change detected, clearing dynamic routes");
                resetPaths();
            }
        }
    }

    private final class DynamicPacketProcessor implements PacketProcessor {
        @Override
        public void process(PacketContext context) {
            if (context.isHandled()) {
                return;
            }

            InboundPacket inbound = context.inPacket();
            Ethernet eth = inbound.parsed();
            if (eth == null || eth.getEtherType() != Ethernet.TYPE_IPV4) {
                return;
            }
            if (!(eth.getPayload() instanceof IPv4)) {
                return;
            }

            IPv4 ipv4 = (IPv4) eth.getPayload();
            Ip4Address srcIp = Ip4Address.valueOf(ipv4.getSourceAddress());
            Ip4Address dstIp = Ip4Address.valueOf(ipv4.getDestinationAddress());

            HostId srcHostId = HostId.hostId(eth.getSourceMAC());
            HostId dstHostId = HostId.hostId(eth.getDestinationMAC());
            Host srcHost = hostService.getHost(srcHostId);
            Host dstHost = hostService.getHost(dstHostId);
            if (srcHost == null || dstHost == null) {
                log.debug("Hosts unavailable for flow {} -> {}", srcHostId, dstHostId);
                return;
            }

            FlowKey key = new FlowKey(srcIp, dstIp);
            PathState state = activePaths.get(key);
            if (state != null) {
                forwardInitialPacket(context, state.forwarding);
                return;
            }

            List<Link> pathLinks = computePath(srcHost, dstHost);
            if (pathLinks == null && !Objects.equals(srcHost.location().deviceId(), dstHost.location().deviceId())) {
                log.debug("No viable path between {} and {}", srcHostId, dstHostId);
                return;
            }

            PathState newState = installPath(key, srcHost, dstHost, pathLinks, srcIp, dstIp);
            if (newState != null) {
                forwardInitialPacket(context, newState.forwarding);
            }
        }
    }

    private static final class FlowKey {
        private final Ip4Address srcIp;
        private final Ip4Address dstIp;

        FlowKey(Ip4Address srcIp, Ip4Address dstIp) {
            this.srcIp = srcIp;
            this.dstIp = dstIp;
        }

        @Override
        public int hashCode() {
            return Objects.hash(srcIp, dstIp);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            FlowKey that = (FlowKey) obj;
            return Objects.equals(this.srcIp, that.srcIp) && Objects.equals(this.dstIp, that.dstIp);
        }
    }

    private static final class PathState {
        private final Map<DeviceId, PortNumber> forwarding;

        PathState(Map<DeviceId, PortNumber> forwarding) {
            this.forwarding = forwarding;
        }
    }
}

