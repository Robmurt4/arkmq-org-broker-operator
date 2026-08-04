---
title: "Partitioned Pub/Sub with AMQP Federation"
description: "Deploy a 2-node federated broker pair with role-based connection routing to partition consumers across broker pods"
draft: false
images: []
menu:
  docs:
    parent: "tutorials"
weight: 120
toc: true
---

This tutorial walks through deploying a publish/subscribe topology where:

- A **producer** publishes messages to the `COMMANDS` topic on any broker.
- **Consumers are partitioned** across broker pods by their JAAS role — `c1`/`c2` always land on broker-0, `c3`/`c4` always land on broker-1.
- **AMQP federation** links the two brokers so that a message published to either broker is delivered to consumers on both.

> **_NOTE:_** This tutorial requires a running Kubernetes cluster. All client workloads (producer and consumers) run as Deployments inside the cluster, using the broker image's built-in `artemis` CLI.

---

### Prerequisites

- A running Kubernetes cluster (for example [Minikube](https://minikube.sigs.k8s.io/docs/start/) or [CRC](https://www.redhat.com/fr/blog/codeready-containers))
- The arkmq-org operator deployed in the `default` namespace
- `kubectl` configured to point at the cluster

#### Start Minikube

```bash
minikube start --profile pub-sub-tutorial --memory=4096 --cpus=2
minikube profile pub-sub-tutorial
```

#### Create and switch to the tutorial namespace

```bash
kubectl create namespace pub-sub-tutorial
kubectl config set-context --current --namespace=pub-sub-tutorial
```

#### Deploy the operator

From the root of the operator repository:

```bash
./deploy/install_opr.sh
```

Wait for the operator to be ready:

```bash
kubectl rollout status deployment/arkmq-org-broker-controller-manager --timeout=300s
```

---

### Step 1 — Deploy the JAAS authentication Secret

The broker uses JAAS `PropertiesLoginModule` for authentication. This Secret
provides three files that are mounted into every broker pod:

- **`login.config`** — chains two login modules: the operator's built-in one (so the operator can connect to the management console) and an app-specific one that reads the files below.
- **`users.properties`** — defines the users and their passwords.
- **`roles.properties`** — maps users to roles. The `shard-consumers-broker-N` roles are what the connection router uses to decide which broker pod a consumer should land on.

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: pub-sub-jaas-config
  namespace: pub-sub-tutorial
stringData:
  login.config: |
    activemq {
      // ensure the operator can connect to the mgmt console by referencing the existing properties config
      org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule sufficient
        org.apache.activemq.jaas.properties.user="artemis-users.properties"
        org.apache.activemq.jaas.properties.role="artemis-roles.properties"
        baseDir="/home/jboss/amq-broker/etc";

      // app specific users and roles
      org.apache.activemq.artemis.spi.core.security.jaas.PropertiesLoginModule sufficient
        reload=true
        debug=true
        org.apache.activemq.jaas.properties.user="users.properties"
        org.apache.activemq.jaas.properties.role="roles.properties";
    };
  users.properties: |
    control-plane=passwd
    p=passwd
    c1=passwd
    c2=passwd
    c3=passwd
    c4=passwd
  roles.properties: |
    # rbac
    control-plane=control-plane,control-plane-0,control-plane-1
    consumers=c1,c2,c3,c4
    producers=p

    # shard roles for connectionRouter partitioning
    shard-consumers-broker-0=c1,c2
    shard-consumers-broker-1=c3,c4
    shard-producers=p
EOF
```

---

### Step 2 — Deploy the logging ConfigMap

This ConfigMap sets `TRACE` level logging on the JAAS and configuration packages, making it easy to see authentication decisions and broker property loading in the pod logs.

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-logging-config
  namespace: pub-sub-tutorial
data:
  logging.properties: |
    appender.stdout.name = STDOUT
    appender.stdout.type = Console
    rootLogger = info, STDOUT
    logger.activemq.name=org.apache.activemq.artemis.core.config.impl.ConfigurationImpl
    logger.activemq.level=TRACE
    logger.jaas.name=org.apache.activemq.artemis.spi.core.security.jaas
    logger.jaas.level=TRACE
    logger.rest.name=org.apache.activemq.artemis.core
    logger.rest.level=INFO
EOF
```

---

### Step 3 — Deploy the broker

This deploys a 2-pod `ActiveMQArtemis` broker with:

- **No persistence** and **no native Artemis cluster** — the two pods are independent brokers linked only by AMQP federation.
- **AMQP federation** — each broker connects outbound to the other pod's headless service DNS name. Messages published to either broker are forwarded to the other, so all consumers receive every message regardless of which broker they are on.
- **Connection router** — inspects the connecting client's JAAS role. It strips the `shard-` prefix (via a regex), then matches the result against the pod's own ordinal. A client whose role doesn't match is **rejected**, causing it to retry until it lands on the correct pod.
- **Metrics plugin** — exposes Prometheus metrics at `http://<pod-ip>:8161/metrics`, used for verification.

> **How the federation URIs work:** Each broker pod has a `CR_NAME` environment variable populated from its own `ActiveMQArtemis` label via the Kubernetes Downward API. The federation target URI is built using this value: `tcp://${CR_NAME}-ss-<other-ordinal>.${CR_NAME}-hdls-svc:61616`. The `broker-0.` and `broker-1.` property prefixes ensure each pod only creates the outbound connection to the *other* pod.

```bash
kubectl apply -f - <<EOF
apiVersion: broker.arkmq.org/v1beta2
kind: BrokerCluster
metadata:
  name: pub-sub-broker
  namespace: pub-sub-tutorial
spec:
  deploymentPlan:
    size: 2
    persistenceEnabled: false
    clustered: false
    enableMetricsPlugin: true
    extraMounts:
      secrets:
        - pub-sub-jaas-config
      configMaps:
        - my-logging-config
  console:
    expose: true
  acceptors:
    - name: tcp
      port: 61616
      expose: true
  env:
    - name: CR_NAME
      valueFrom:
        fieldRef:
          fieldPath: "metadata.labels['ActiveMQArtemis']"
  brokerProperties:
    # address config: MULTICAST = pub/sub topic behaviour
    - addressConfigurations.COMMANDS.routingTypes=MULTICAST

    # rbac
    - securityRoles.COMMANDS.producers.send=true
    - securityRoles.COMMANDS.consumers.consume=true
    - securityRoles.COMMANDS.consumers.createNonDurableQueue=true
    - securityRoles.COMMANDS.consumers.deleteNonDurableQueue=true

    # control-plane rbac (used by federation links)
    - securityRoles.COMMANDS.control-plane.createDurableQueue=true
    - securityRoles.COMMANDS.control-plane.consume=true
    - securityRoles.COMMANDS.control-plane.send=true

    # federation internal address permissions
    - "securityRoles.\"$ACTIVEMQ_ARTEMIS_FEDERATION.#\".control-plane.createNonDurableQueue=true"
    - "securityRoles.\"$ACTIVEMQ_ARTEMIS_FEDERATION.#\".control-plane.createAddress=true"
    - "securityRoles.\"$ACTIVEMQ_ARTEMIS_FEDERATION.#\".control-plane.consume=true"
    - "securityRoles.\"$ACTIVEMQ_ARTEMIS_FEDERATION.#\".control-plane.send=true"

    # AMQP federation: broker-0 connects to broker-1, and vice versa
    - broker-0.AMQPConnections.target.uri=tcp://${CR_NAME}-ss-1.${CR_NAME}-hdls-svc:61616
    - broker-1.AMQPConnections.target.uri=tcp://${CR_NAME}-ss-0.${CR_NAME}-hdls-svc:61616

    # federation connection settings
    - AMQPConnections.target.retryInterval=1000
    - AMQPConnections.target.user=control-plane
    - AMQPConnections.target.password=passwd
    - AMQPConnections.target.autostart=true

    # federate the COMMANDS address across both brokers
    - AMQPConnections.target.federations.peerN.localAddressPolicies.forCommands.includes.justCommands.addressMatch=COMMANDS

    # connection router: partition consumers by shard role
    - connectionRouters.partitionOnRole.keyType=ROLE_NAME
    - connectionRouters.partitionOnRole.localTargetFilter=NULL|producers|consumers-broker-${STATEFUL_SET_ORDINAL}
    - "connectionRouters.partitionOnRole.keyFilter=(?<=^shard-).*"

    # wire the router to the tcp acceptor
    - acceptorConfigurations.tcp.params.router=partitionOnRole
EOF
```

Wait for both broker pods to be ready:

```bash
kubectl wait BrokerCluster pub-sub-broker \
  --for=condition=Ready \
  --namespace=pub-sub-tutorial \
  --timeout=240s
```

Verify both pods are running:

```bash
kubectl get pods -n pub-sub-tutorial -l ActiveMQArtemis=pub-sub-broker
```

Expected output:
```
NAME                  READY   STATUS    RESTARTS   AGE
pub-sub-broker-ss-0   1/1     Running   0          ...
pub-sub-broker-ss-1   1/1     Running   0          ...
```

---

### Step 4 — Deploy the load-balanced Service

The operator creates per-pod and headless services automatically, but the producer and consumers need a single **load-balanced** entry point so that their initial connection attempt is randomly distributed between the two pods. The connection router will then redirect them to the correct pod based on their role.

```bash
kubectl apply -f - <<EOF
apiVersion: v1
kind: Service
metadata:
  name: pub-sub-broker
  namespace: pub-sub-tutorial
spec:
  selector:
    ActiveMQArtemis: pub-sub-broker
  ports:
    - port: 62616
      targetPort: 61616
EOF
```

---

### Step 5 — Deploy the consumers

Each consumer runs the `artemis perf consumer` command in a retry loop. The loop is needed because the connection router rejects connections that arrive on the wrong broker pod — the consumer retries until it lands on the correct one.

- **consumer1** uses user `c1`, who has the role `shard-consumers-broker-0` → routed to `pub-sub-broker-ss-0`.
- **consumer3** uses user `c3`, who has the role `shard-consumers-broker-1` → routed to `pub-sub-broker-ss-1`.

```bash
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer1
  namespace: pub-sub-tutorial
  labels:
    app: consumer1
spec:
  replicas: 1
  selector:
    matchLabels:
      app: consumer1
  template:
    metadata:
      labels:
        app: consumer1
    spec:
      containers:
        - name: consumer1
          image: quay.io/arkmq-org/arkmq-org-broker-kubernetes:artemis.2.54.0
          command:
            - /bin/sh
            - -c
            - |
              until /opt/amq/bin/artemis perf consumer \
                --password passwd --user c1 \
                --url tcp://pub-sub-broker:62616 \
                --silent --message-count 10 \
                topic://COMMANDS; do
                echo retry; sleep 1
              done
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: consumer3
  namespace: pub-sub-tutorial
  labels:
    app: consumer3
spec:
  replicas: 1
  selector:
    matchLabels:
      app: consumer3
  template:
    metadata:
      labels:
        app: consumer3
    spec:
      containers:
        - name: consumer3
          image: quay.io/arkmq-org/arkmq-org-broker-kubernetes:artemis.2.54.0
          command:
            - /bin/sh
            - -c
            - |
              until /opt/amq/bin/artemis perf consumer \
                --password passwd --user c3 \
                --url tcp://pub-sub-broker:62616 \
                --silent --message-count 10 \
                topic://COMMANDS; do
                echo retry; sleep 1
              done
EOF
```

---

### Step 6 — Deploy the producer

The producer publishes to `topic://COMMANDS` at a rate of 2 messages per second. User `p` has the `producers` role which matches `NULL|producers|...` in the router's `localTargetFilter` — so it is accepted on any broker pod.

```bash
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: producer
  namespace: pub-sub-tutorial
  labels:
    app: producer
spec:
  replicas: 1
  selector:
    matchLabels:
      app: producer
  template:
    metadata:
      labels:
        app: producer
    spec:
      containers:
        - name: producer
          image: quay.io/arkmq-org/arkmq-org-broker-kubernetes:artemis.2.54.0
          command:
            - /opt/amq/bin/artemis
            - perf
            - producer
            - --user
            - p
            - --password
            - passwd
            - --url
            - tcp://pub-sub-broker:62616
            - --rate
            - "2"
            - --silent
            - topic://COMMANDS
EOF
```

---

### Step 7 — Verify messages are flowing on both brokers

The Prometheus metrics plugin exposes a `artemis_routed_message_count` gauge per address. Once both brokers show a non-zero value for `COMMANDS`, the topology is confirmed working: federation has propagated messages from the publishing broker to the consuming broker.

First, get the IP addresses of each broker pod:

```bash
kubectl get pod pub-sub-broker-ss-0 -n pub-sub-tutorial -o jsonpath='{.status.podIP}'
kubectl get pod pub-sub-broker-ss-1 -n pub-sub-tutorial -o jsonpath='{.status.podIP}'
```

Then scrape the metrics endpoint on each pod (replace `<pod-ip-0>` and `<pod-ip-1>` with the values returned above):

```bash
# broker-0
curl -s http://<pod-ip-0>:8161/metrics | grep 'artemis_routed_message_count.*COMMANDS'

# broker-1
curl -s http://<pod-ip-1>:8161/metrics | grep 'artemis_routed_message_count.*COMMANDS'
```

Both commands should return a line like the following with a value **greater than `0.0`**:

```
artemis_routed_message_count{address="COMMANDS",broker="amq-broker",} 12.0
```

If either value is `0.0`, wait a few seconds and retry — the federation mesh and consumer connections may still be establishing.

You can also watch the consumer pod logs to confirm they are receiving messages:

```bash
kubectl logs -n pub-sub-tutorial -l app=consumer1 --follow
kubectl logs -n pub-sub-tutorial -l app=consumer3 --follow
```

---

### Cleanup

Delete all resources created by this tutorial:

```bash
kubectl delete deployment producer consumer1 consumer3 -n pub-sub-tutorial
kubectl delete activemqartemis pub-sub-broker -n pub-sub-tutorial
kubectl delete secret pub-sub-jaas-config -n pub-sub-tutorial
kubectl delete configmap my-logging-config -n pub-sub-tutorial
kubectl delete service pub-sub-broker -n pub-sub-tutorial
kubectl delete namespace pub-sub-tutorial
```

Or, to remove the entire Minikube cluster:

```bash
minikube delete --profile pub-sub-tutorial
```

---

### Further reading

- [BrokerProperties reference](../help/operator.md#configuring-brokerproperties) — how to configure the broker without an init container.
- [Extra mounts](../getting-started/quick-start.md#using-a-operator-extramounts) — how to mount Secrets and ConfigMaps into broker pods.
- [Scale up and scale down](scaleup_and_scaledown.md) — adding and removing broker pods from a deployment.
- [Connection Routers](https://activemq.apache.org/components/artemis/documentation/latest/connection-routers.html) — upstream Artemis documentation for the `connectionRouters` configuration.
- [AMQP Federation](https://activemq.apache.org/components/artemis/documentation/latest/amqp-broker-connections.html) — upstream Artemis documentation for `AMQPConnections` and federation policies.
