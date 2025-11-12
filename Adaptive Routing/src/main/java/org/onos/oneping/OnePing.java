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
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.LinkKey;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.Topology;
import org.onosproject.net.topology.TopologyEdge;
import org.onosproject.net.topology.TopologyGraph;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.net.topology.TopologyVertex;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Component(immediate = true)
public class OnePing {

    private static final Logger log = LoggerFactory.getLogger(OnePing.class);

    private static final int PACKET_PROCESSOR_PRIORITY = 500;
    private static final int FLOW_PRIORITY = 50_000;
    private static final int FLOW_HARD_TIMEOUT_SEC = 60;
    private static final long FLOW_STALE_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long STATS_POLL_INTERVAL_MS = 100;           // faster feedback for high-throughput links
    private static final long REROUTE_COOLDOWN_MS = 150;               // quick successive adjustments allowed

    private static final double DEFAULT_LINK_CAPACITY_BPS = 25_000_000D; // align with core 25 Mbps
    private static final double UTILIZATION_THRESHOLD = 0.70D;           // shift flows before queues build up
    private static final double ALPHA = 0.35D;                           // base hop cost
    private static final double BETA = 25.0D;                            // congestion penalty

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LinkService linkService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    private final PacketProcessor packetProcessor = new AdaptivePacketProcessor();

    private final ConcurrentMap<LinkKey, Double> linkUtilization = new ConcurrentHashMap<>();
    private final ConcurrentMap<ConnectPoint, PortStatsSnapshot> previousPortStats = new ConcurrentHashMap<>();
    private final ConcurrentMap<FlowKey, PathState> activePaths = new ConcurrentHashMap<>();

    private ScheduledExecutorService statsExecutor;
    private ApplicationId appId;

    @Activate
    protected void activate() {
        appId = coreService.registerApplication("org.onosproject.oneping");
        packetService.addProcessor(packetProcessor, PACKET_PROCESSOR_PRIORITY);
        packetService.requestPackets(DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4).build(), PacketPriority.REACTIVE, appId);

    statsExecutor = Executors.newSingleThreadScheduledExecutor(namedThreadFactory("oneping-monitor"));
    statsExecutor.scheduleAtFixedRate(this::refreshLinkUtilization, 0,
                      STATS_POLL_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("Adaptive routing application started");
    }

    @Deactivate
    protected void deactivate() {
        packetService.removeProcessor(packetProcessor);
        packetService.cancelPackets(DefaultTrafficSelector.builder()
                .matchEthType(Ethernet.TYPE_IPV4).build(), PacketPriority.REACTIVE, appId);

        if (statsExecutor != null) {
            statsExecutor.shutdownNow();
        }

        flowRuleService.removeFlowRulesById(appId);
        activePaths.clear();
        linkUtilization.clear();
        previousPortStats.clear();
        log.info("Adaptive routing application stopped");
    }

    private void refreshLinkUtilization() {
        try {
            long now = System.currentTimeMillis();
            for (Device device : deviceService.getAvailableDevices()) {
                List<PortStatistics> stats = deviceService.getPortStatistics(device.id());
                if (stats == null) {
                    continue;
                }
                stats.stream().filter(stat -> stat != null && !stat.portNumber().isLogical())
                        .forEach(stat -> updateUtilization(device.id(), stat, now));
            }
            reRouteCongestedFlows(now);
        } catch (Exception e) {
            log.warn("Failed to refresh link utilization", e);
        }
    }

    private void updateUtilization(DeviceId deviceId, PortStatistics stats, long now) {
        ConnectPoint cp = new ConnectPoint(deviceId, stats.portNumber());
        long totalBytes = stats.bytesReceived() + stats.bytesSent();
        PortStatsSnapshot previous = previousPortStats.put(cp, new PortStatsSnapshot(totalBytes, now));
        if (previous == null || totalBytes < previous.bytes || now <= previous.timestamp) {
            return;
        }

        long deltaBytes = totalBytes - previous.bytes;
        long deltaTime = now - previous.timestamp;
        if (deltaBytes == 0 || deltaTime <= 0) {
            return;
        }

        double rateBps = (deltaBytes * 8.0D * 1000.0D) / deltaTime;
        Set<Link> links = linkService.getLinks(cp);
        if (links == null || links.isEmpty()) {
            return;
        }

        for (Link link : links) {
            if (!link.src().equals(cp) || link.state() != Link.State.ACTIVE) {
                continue;
            }
            double capacity = extractCapacity(link);
            double utilization = capacity > 0 ? Math.min(1.0D, rateBps / capacity) : 0.0D;
            linkUtilization.put(LinkKey.linkKey(link.src(), link.dst()), utilization);
        }
    }

    private double extractCapacity(Link link) {
        String annotatedCapacity = link.annotations() != null ? link.annotations().value("bandwidth") : null;
        if (annotatedCapacity != null) {
            try {
                return Double.parseDouble(annotatedCapacity);
            } catch (NumberFormatException e) {
                log.debug("Unable to parse bandwidth annotation {} for link {}", annotatedCapacity, link, e);
            }
        }
        return DEFAULT_LINK_CAPACITY_BPS;
    }

    private void reRouteCongestedFlows(long now) {
        activePaths.forEach((key, state) -> {
            if (state.isStale(now, FLOW_STALE_TIMEOUT_MS)) {
                if (activePaths.remove(key, state)) {
                    removeFlowRulesSafe(state.flowRules);
                }
                return;
            }

            double maxUtil = maxUtilization(state.linkKeys);
            if (state.linkKeys.isEmpty() || maxUtil < UTILIZATION_THRESHOLD ||
                    now - state.lastRerouteMillis < REROUTE_COOLDOWN_MS) {
                return;
            }

            Host srcHost = hostService.getHost(state.srcHostId);
            Host dstHost = hostService.getHost(state.dstHostId);
            if (srcHost == null || dstHost == null) {
                return;
            }

            List<Link> newPath = computePath(srcHost, dstHost);
            if (newPath == null) {
                return;
            }

            List<LinkKey> newKeys = buildLinkKeys(newPath);
            if (state.hasSameLinks(newKeys) && maxUtil >= UTILIZATION_THRESHOLD) {
                Set<LinkKey> avoidLinks = congestedLinks(state.linkKeys);
                if (!avoidLinks.isEmpty()) {
                    List<Link> alternate = computePath(srcHost, dstHost, avoidLinks);
                    if (alternate != null) {
                        List<LinkKey> altKeys = buildLinkKeys(alternate);
                        if (!state.hasSameLinks(altKeys)) {
                            newPath = alternate;
                            newKeys = altKeys;
                        }
                    }
                }
            }

            if (state.hasSameLinks(newKeys)) {
                state.refreshInstall(now);
                return;
            }

            long start = System.currentTimeMillis();
            PathState newState = installOrUpdatePath(key, srcHost, dstHost, newPath, state.srcIp, state.dstIp);
            if (newState != null) {
                newState.markReroute(now);
                long duration = System.currentTimeMillis() - start;
                String formattedUtil = String.format("%.1f", maxUtil * 100.0D);
                log.info("Adaptive reroute {} -> {} | max util {}% | duration {} ms | path {}",
                        state.srcIp, state.dstIp, formattedUtil, duration, formatPath(newPath));
            }
        });
    }

    private double maxUtilization(List<LinkKey> linkKeys) {
        double max = 0.0D;
        for (LinkKey key : linkKeys) {
            max = Math.max(max, linkUtilization.getOrDefault(key, 0.0D));
        }
        return max;
    }

    private PathState installOrUpdatePath(FlowKey key, Host srcHost, Host dstHost,
                                          List<Link> pathLinks, Ip4Address srcIp, Ip4Address dstIp) {
        long now = System.currentTimeMillis();
        Map<DeviceId, PortNumber> forwarding = buildForwardingMap(srcHost, dstHost, pathLinks);
        FlowRule[] rules = buildFlowRules(forwarding, srcIp, dstIp);
        List<LinkKey> linkKeys = buildLinkKeys(pathLinks);
        PathState draftState = new PathState(srcHost.id(), dstHost.id(), srcIp, dstIp,
                forwarding, rules, linkKeys, now);

        PathState[] resultHolder = new PathState[1];
        activePaths.compute(key, (k, existing) -> {
            if (existing != null && existing.hasSameLinks(linkKeys)) {
                existing.refreshInstall(now);
                resultHolder[0] = existing;
                return existing;
            }

            flowRuleService.applyFlowRules(rules);
            if (existing != null) {
                removeFlowRulesSafe(existing.flowRules);
            }
            resultHolder[0] = draftState;
            return draftState;
        });
        return resultHolder[0];
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
                    .makeTemporary(FLOW_HARD_TIMEOUT_SEC)
                    .fromApp(appId)
                    .build();
            rules.add(rule);
        });
        return rules.toArray(new FlowRule[0]);
    }

    private List<LinkKey> buildLinkKeys(List<Link> links) {
        if (links == null || links.isEmpty()) {
            return Collections.emptyList();
        }
        return links.stream()
                .map(link -> LinkKey.linkKey(link.src(), link.dst()))
                .collect(Collectors.toList());
    }

    private List<Link> computePath(Host srcHost, Host dstHost) {
        return computePath(srcHost, dstHost, Collections.emptySet());
    }

    private List<Link> computePath(Host srcHost, Host dstHost, Set<LinkKey> avoidLinks) {
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

        Map<TopologyVertex, Double> distance = new HashMap<>();
        Map<TopologyVertex, TopologyEdge> previous = new HashMap<>();
        PriorityQueue<VertexCost> queue = new PriorityQueue<>(Comparator.comparingDouble(vc -> vc.cost));

        distance.put(srcVertex, 0.0D);
        queue.add(new VertexCost(srcVertex, 0.0D));

        while (!queue.isEmpty()) {
            VertexCost current = queue.poll();
            if (current.cost > distance.getOrDefault(current.vertex, Double.POSITIVE_INFINITY)) {
                continue;
            }
            if (current.vertex.equals(dstVertex)) {
                break;
            }

            for (TopologyEdge edge : graph.getEdgesFrom(current.vertex)) {
                Link link = edge.link();
                LinkKey linkKey = LinkKey.linkKey(link.src(), link.dst());
                if (avoidLinks.contains(linkKey)) {
                    continue;
                }
                TopologyVertex neighbour = edge.dst();
                double weight = linkWeight(link);
                double newCost = current.cost + weight;
                if (newCost + 1e-9 < distance.getOrDefault(neighbour, Double.POSITIVE_INFINITY)) {
                    distance.put(neighbour, newCost);
                    previous.put(neighbour, edge);
                    queue.add(new VertexCost(neighbour, newCost));
                }
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

    private double linkWeight(Link link) {
        double utilization = linkUtilization.getOrDefault(LinkKey.linkKey(link.src(), link.dst()), 0.0D);
        return ALPHA + (BETA * utilization);
    }

    private Set<LinkKey> congestedLinks(List<LinkKey> linkKeys) {
        Set<LinkKey> congested = new HashSet<>();
        for (LinkKey key : linkKeys) {
            double util = linkUtilization.getOrDefault(key, 0.0D);
            if (util >= UTILIZATION_THRESHOLD) {
                congested.add(key);
            }
        }
        return congested;
    }

    private TopologyVertex findVertex(TopologyGraph graph, DeviceId deviceId) {
        for (TopologyVertex vertex : graph.getVertexes()) {
            if (vertex.deviceId().equals(deviceId)) {
                return vertex;
            }
        }
        return null;
    }

    private void removeFlowRulesSafe(FlowRule[] flowRules) {
        if (flowRules != null && flowRules.length > 0) {
            flowRuleService.removeFlowRules(flowRules);
        }
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

    private ThreadFactory namedThreadFactory(String name) {
        return runnable -> {
            Thread thread = new Thread(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private final class AdaptivePacketProcessor implements PacketProcessor {
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
                state.refreshInstall(System.currentTimeMillis());
                forwardInitialPacket(context, state.forwarding);
                return;
            }

            List<Link> pathLinks = computePath(srcHost, dstHost);
            if (pathLinks == null && !Objects.equals(srcHost.location().deviceId(), dstHost.location().deviceId())) {
                log.debug("No viable path between {} and {}", srcHostId, dstHostId);
                return;
            }

            PathState newState = installOrUpdatePath(key, srcHost, dstHost, pathLinks, srcIp, dstIp);
            if (newState != null) {
                forwardInitialPacket(context, newState.forwarding);
            }
        }
    }

    private static final class VertexCost {
        private final TopologyVertex vertex;
        private final double cost;

        VertexCost(TopologyVertex vertex, double cost) {
            this.vertex = vertex;
            this.cost = cost;
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
        private final HostId srcHostId;
        private final HostId dstHostId;
        private final Ip4Address srcIp;
        private final Ip4Address dstIp;
        private final Map<DeviceId, PortNumber> forwarding;
        private final FlowRule[] flowRules;
        private final List<LinkKey> linkKeys;
        private volatile long lastInstallMillis;
        private volatile long lastRerouteMillis;

      PathState(HostId srcHostId, HostId dstHostId,
            Ip4Address srcIp, Ip4Address dstIp,
                  Map<DeviceId, PortNumber> forwarding,
                  FlowRule[] flowRules, List<LinkKey> linkKeys,
                  long timestamp) {
            this.srcHostId = srcHostId;
            this.dstHostId = dstHostId;
            this.srcIp = srcIp;
            this.dstIp = dstIp;
            this.forwarding = forwarding;
            this.flowRules = flowRules;
            this.linkKeys = linkKeys;
            this.lastInstallMillis = timestamp;
            this.lastRerouteMillis = timestamp;
        }

        boolean hasSameLinks(List<LinkKey> other) {
            return linkKeys.equals(other);
        }

        void refreshInstall(long now) {
            this.lastInstallMillis = now;
        }

        void markReroute(long now) {
            this.lastRerouteMillis = now;
        }

        boolean isStale(long now, long timeoutMs) {
            return now - lastInstallMillis > timeoutMs;
        }
    }

    private static final class PortStatsSnapshot {
        private final long bytes;
        private final long timestamp;

        PortStatsSnapshot(long bytes, long timestamp) {
            this.bytes = bytes;
            this.timestamp = timestamp;
        }
    }
}

