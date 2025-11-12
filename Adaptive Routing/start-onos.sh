#!/bin/bash

docker run -it --rm \
  -e ONOS_APPS=gui2,openflow,lldpprovider,hostprovider \
  -p 8101:8101 -p 8181:8181 -p 6653:6653 \
  onosproject/onos:2.2.x