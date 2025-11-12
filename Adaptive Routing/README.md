## Prerequisites
- Java 11
- Maven 3.x
- ONOS 2.2.2 (Docker image supported)
- Mininet for the demo topology

## Build
Compile the project and assemble the ONOS application archive (`.oar`):

```bash
mvn clean package
```

## Launch the Demo Environment

### 1. Start ONOS
Run ONOS (foreground process) using the provided helper script:

```bash
./start-onos.sh
```

### 2. Clear Existing Network Configuration
In a separate terminal, reset the ONOS network configuration:

```bash
curl -u onos:rocks -X DELETE http://localhost:8181/onos/v1/network/configuration
```

### 3. Install the Application
With ONOS running, load the `oneping` application via the `onos-app` tool:

```bash
tools/onos-app localhost install! target/oneping-2.0.0.oar
```

### 4. Start the Topology
Launch the sample mesh topology in Mininet:

```bash
sudo python3 seven_switch_mesh.py
```

### 5. Run Connectivity Checks
From the Mininet CLI:

```text
mininet> pingall
mininet> xterm h1 h2
```

This opens separate terminals for hosts `h1` and `h2`.

### 6. Generate Traffic
- On `h2`, start a UDP server:

	```bash
	iperf -s -u &
	```

- On `h1`, generate UDP traffic:

	```bash
	iperf -c 10.0.0.2 -u -b 10M -t 30 -i 1
	```

You should now observe traffic flowing across the dynamic route installed by the application.