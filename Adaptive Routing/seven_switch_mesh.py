#!/usr/bin/env python3
"""
Seven-switch, two-host Mininet topology matching the provided ONOS demo layout.

Hosts: h1 (10.0.0.1) attached to s1, h2 (10.0.0.2) attached to s7.
Switch fabric:
    - Spine ring across the top: s4 -- s5 -- s6
    - Edge hubs: s1 (left) and s7 (right)
    - Diagonal connectors: s1-s2-s7 and s1-s3-s7

Topology sketch (hosts as circles, switches as boxes):

            (h1)
                |
            [s1]------[s4]------[s5]------[s6]
             |  \                |         |
            [s2] \               |         |
             |    \             [s7]-------(h2)
            [s3]   -------------/

Defaults favor balanced throughput across all available paths: the core ring is spacious while the
diagonals still have slightly higher latency so the controller can spread flows without incurring
loss.

Usage:
    sudo python3 seven_switch_mesh.py --controller 127.0.0.1
        [--access-bandwidth 50]
        [--core-bandwidth 25 --core-delay 10 --core-queue 40]
        [--diagonal-bandwidth 12 --diagonal-delay 35 --diagonal-queue 20]
"""

import argparse
from functools import partial

from mininet.cli import CLI
from mininet.link import TCLink
from mininet.log import setLogLevel, info
from mininet.net import Mininet
from mininet.node import OVSSwitch, RemoteController
from mininet.topo import Topo


class SevenSwitchMeshTopo(Topo):
    def build(
        self,
        access_bw,
        core_bw,
        core_delay,
        core_queue,
        diagonal_bw,
        diagonal_delay,
        diagonal_queue,
    ):
        h1 = self.addHost("h1", ip="10.0.0.1/24")
        h2 = self.addHost("h2", ip="10.0.0.2/24")

        s1 = self.addSwitch("s1")
        s2 = self.addSwitch("s2")
        s3 = self.addSwitch("s3")
        s4 = self.addSwitch("s4")
        s5 = self.addSwitch("s5")
        s6 = self.addSwitch("s6")
        s7 = self.addSwitch("s7")

        access_link = {"bw": access_bw}
        core_link = {
            "bw": core_bw,
            "delay": f"{core_delay}ms",
            "max_queue_size": core_queue,
            "use_htb": True,
        }
        diagonal_link = {
            "bw": diagonal_bw,
            "delay": f"{diagonal_delay}ms",
            "max_queue_size": diagonal_queue,
            "use_htb": True,
        }

        self.addLink(h1, s1, **access_link)
        self.addLink(h2, s7, **access_link)

        self.addLink(s1, s4, **core_link)
        self.addLink(s4, s5, **core_link)
        self.addLink(s5, s6, **core_link)
        self.addLink(s6, s7, **core_link)

        self.addLink(s1, s2, **diagonal_link)
        self.addLink(s1, s3, **diagonal_link)
        self.addLink(s7, s2, **diagonal_link)
        self.addLink(s7, s3, **diagonal_link)


def parse_args():
    parser = argparse.ArgumentParser(description="Seven-switch Mininet topology for ONOS")
    parser.add_argument("--controller", default="127.0.0.1", help="ONOS controller IP")
    parser.add_argument("--port", type=int, default=6653, help="OpenFlow port")
    parser.add_argument("--access-bandwidth", type=int, default=50, help="Access link bandwidth (Mbps)")
    parser.add_argument("--core-bandwidth", type=int, default=25, help="Core ring bandwidth (Mbps)")
    parser.add_argument("--core-delay", type=int, default=10, help="Core ring delay (ms)")
    parser.add_argument("--core-queue", type=int, default=40, help="Core ring queue size (packets)")
    parser.add_argument("--diagonal-bandwidth", type=int, default=12, help="Diagonal shortcut bandwidth (Mbps)")
    parser.add_argument("--diagonal-delay", type=int, default=35, help="Diagonal shortcut delay (ms)")
    parser.add_argument("--diagonal-queue", type=int, default=20, help="Diagonal shortcut queue size (packets)")
    return parser.parse_args()


def main():
    args = parse_args()

    topo = SevenSwitchMeshTopo(
        access_bw=args.access_bandwidth,
        core_bw=args.core_bandwidth,
        core_delay=args.core_delay,
        core_queue=args.core_queue,
        diagonal_bw=args.diagonal_bandwidth,
        diagonal_delay=args.diagonal_delay,
        diagonal_queue=args.diagonal_queue,
    )

    switch_cls = partial(OVSSwitch, protocols="OpenFlow13")

    net = Mininet(
        topo=topo,
        controller=None,
        switch=switch_cls,
        link=TCLink,
        autoSetMacs=True,
        autoStaticArp=True,
    )

    net.addController(
        "onos",
        controller=RemoteController,
        ip=args.controller,
        port=args.port,
    )

    info("*** Starting seven-switch mesh: controller %s:%s\n" % (args.controller, args.port))
    info("    access bw=%s Mbps | core bw=%s Mbps delay=%sms queue=%s | diagonal bw=%s Mbps delay=%sms queue=%s\n"
         % (
             args.access_bandwidth,
             args.core_bandwidth,
             args.core_delay,
             args.core_queue,
             args.diagonal_bandwidth,
             args.diagonal_delay,
             args.diagonal_queue,
         ))

    net.start()
    info("*** Topology ready. Use the Mininet CLI; type 'exit' to stop.\n")
    CLI(net)
    info("*** Stopping network\n")
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    main()
