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

### How it works

The diagram below shows the key components and how they interact:

```
  [producer (user: p)]
        │
        │  connect to tcp://pub-sub-broker:62616 (load-balanced Service)
        ▼
 ┌──────────────────────────────────────────────────────────────┐
 │              Load-balanced Service (port 62616)              │
 └──────────────┬───────────────────────────────┬──────────────┘
                │                               │
                ▼                               ▼
     ┌─────────────────────┐         ┌─────────────────────┐
     │  pub-sub-broker-ss-0│◄───────►│  pub-sub-broker-ss-1│
     │   (broker-0)        │  AMQP   │   (broker-1)        │
     │                     │ federat.│                     │
     └────────┬────────────┘         └──────────┬──────────┘
              │                                 │
     [consumer1 (user: c1)]          [consumer3 (user: c3)]
     role: shard-consumers-broker-0  role: shard-consumers-broker-1
```

**Connection routing:** when a client connects to the load-balanced Service, the broker's connection router inspects the client's JAAS role. It strips the `shard-` prefix (via a regex `keyFilter`) and matches the result against `NULL|producers|consumers-broker-<ordinal>`. If the role does not match the broker's own ordinal, the broker **refuses the AMQP connection**, causing the client to retry — eventually landing on the correct pod.

**AMQP federation:** each broker opens an outbound AMQP connection to the *other* pod's headless DNS name. A federation policy mirrors the `COMMANDS` address, so every message produced on either broker is forwarded to the other broker's consumers.

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

> **Kubernetes ingress domain:** the broker CR sets `console.expose: true`, which causes the operator to create an Ingress for the management console. On vanilla Kubernetes (non-OpenShift), you should also set `spec.ingressDomain` in the broker CR to match your cluster's ingress domain (e.g. `192.168.49.2.nip.io` for Minikube). If you omit it, the console Ingress will be created but unreachable — the broker itself will still work normally. See the [Minikube ingress domain example](../getting-started/quick-start.md) for details.

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

**Role design explained:**

| Role | Members | Purpose |
|------|---------|---------|
| `producers` | `p` | Allowed to send to `COMMANDS`. Matches the `NULL\|producers\|...` router filter, so user `p` is accepted on any broker. |
| `consumers` | `c1,c2,c3,c4` | Allowed to consume from `COMMANDS`. |
| `control-plane` | — | Used by the AMQP federation links. Has permission to create and consume from the internal federation addresses. |
| `shard-consumers-broker-0` | `c1,c2` | The router's `keyFilter` strips `shard-` to get `consumers-broker-0`, which matches only broker-0's filter. |
| `shard-consumers-broker-1` | `c3,c4` | Same mechanism — these users land on broker-1. |

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
- **Connection router** — inspects the connecting client's JAAS role. It strips the `shard-` prefix (via a regex), then matches the result against the pod's own ordinal. A client whose role doesn't match is **refused**, causing it to retry until it lands on the correct pod.
- **Metrics plugin** — exposes Prometheus metrics at `http://<pod-hostname>:8161/metrics` (bound to the headless service DNS name), used for verification.

> **How the federation URIs work:** the `broker-0.` and `broker-1.` property prefixes route each property to the matching pod's `broker.properties` file. The URI values are hardcoded to the StatefulSet headless DNS names (`<cr-name>-ss-<ordinal>.<cr-name>-hdls-svc`). `${CR_NAME}` and `${STATEFUL_SET_ORDINAL}` are **not** expanded inside `.properties` file values — the operator expands `${STATEFUL_SET_ORDINAL}` only in the `-Dbroker.properties=` JVM path (to select the right per-pod directory), not in property values themselves.

> **How `retryInterval=1000` helps:** the federation links reconnect every 1 second if the target pod is not yet ready. Without this, the default interval is longer and the federation mesh takes more time to establish after startup.

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
    - securityRoles.COMMANDS.control-plane.deleteDurableQueue=true
    - securityRoles.COMMANDS.control-plane.consume=true
    - securityRoles.COMMANDS.control-plane.send=true

    # federation internal address permissions
    - 'securityRoles."\$ACTIVEMQ_ARTEMIS_FEDERATION.#".control-plane.createNonDurableQueue=true'
    - 'securityRoles."\$ACTIVEMQ_ARTEMIS_FEDERATION.#".control-plane.createAddress=true'
    - 'securityRoles."\$ACTIVEMQ_ARTEMIS_FEDERATION.#".control-plane.consume=true'
    - 'securityRoles."\$ACTIVEMQ_ARTEMIS_FEDERATION.#".control-plane.send=true'

    # AMQP federation: broker-0 connects to broker-1, and vice versa
    # URIs are hardcoded to the StatefulSet headless DNS names
    - broker-0.AMQPConnections.target.uri=tcp://pub-sub-broker-ss-1.pub-sub-broker-hdls-svc:61616
    - broker-1.AMQPConnections.target.uri=tcp://pub-sub-broker-ss-0.pub-sub-broker-hdls-svc:61616

    # speed up mesh formation (reconnect every 1s while the peer pod is starting)
    - AMQPConnections.target.retryInterval=1000

    # federation connection credentials
    - AMQPConnections.target.user=control-plane
    - AMQPConnections.target.password=passwd
    - AMQPConnections.target.autostart=true

    # federate the COMMANDS address across both brokers
    - AMQPConnections.target.federations.peerN.localAddressPolicies.forCommands.includes.justCommands.addressMatch=COMMANDS

    # connection router: partition consumers by shard role
    # localTargetFilter uses a regex matching either ordinal so it survives live reloads,
    # while the per-pod broker-N. override narrows it to only the matching ordinal
    - connectionRouters.partitionOnRole.keyType=ROLE_NAME
    - connectionRouters.partitionOnRole.localTargetFilter=NULL|producers|consumers-broker-.*
    - connectionRouters.partitionOnRole.keyFilter=(?<=^shard-).*
    - acceptorConfigurations.tcp.params.router=partitionOnRole

    # broker-0 accepts producers and consumers-broker-0 only
    - broker-0.connectionRouters.partitionOnRole.localTargetFilter=NULL|producers|consumers-broker-0

    # broker-1 accepts producers and consumers-broker-1 only
    - broker-1.connectionRouters.partitionOnRole.localTargetFilter=NULL|producers|consumers-broker-1
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

**How the `localTargetFilter` and `keyFilter` work together:**

Each broker evaluates these two properties when a client connects:

1. `keyType=ROLE_NAME` — extract the client's JAAS roles.
2. `keyFilter=(?<=^shard-).*` — keep only roles that start with `shard-`, and strip that prefix. For user `c1`, whose roles include `shard-consumers-broker-0`, the extracted key becomes `consumers-broker-0`.
3. `localTargetFilter=NULL|producers|consumers-broker-${STATEFUL_SET_ORDINAL}` — the broker accepts the connection if the key matches this pattern. On broker-0 (ordinal `0`), the pattern expands to `NULL|producers|consumers-broker-0`. User `c1`'s key `consumers-broker-0` matches → **accepted**. User `c3`'s key `consumers-broker-1` does not match → **connection refused**. The client retries until it reaches broker-1.

`NULL` in the filter means "no matching shard role" — this catches the producer (user `p`) who has no `shard-` prefixed role, so it is accepted on any broker pod.

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

> A different port (`62616`) is used on the Service to avoid conflicts with the per-pod Services that the operator creates on the same cluster IP range. The broker's acceptor remains on `61616` inside the pod.

---

### Step 5 — Deploy the consumers

Each consumer runs the `artemis perf consumer` command in a retry loop. The loop is needed because the connection router **refuses** connections that arrive on the wrong broker pod with an AMQP error — the consumer exits, sleeps 1 second, and retries until it lands on the correct one.

- **consumer1** uses user `c1`, whose role `shard-consumers-broker-0` routes it to `pub-sub-broker-ss-0`.
- **consumer3** uses user `c3`, whose role `shard-consumers-broker-1` routes it to `pub-sub-broker-ss-1`.

Each consumer waits to receive exactly **10 messages** before exiting, then the retry loop re-launches it. This is intentional — it demonstrates that messages continue flowing even as consumers reconnect.

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

The producer publishes to `topic://COMMANDS` at a rate of 2 messages per second. User `p` has the `producers` role, which matches `NULL` in the router's `localTargetFilter` (because `p` has no `shard-` prefixed role). This means the producer is **accepted on any broker pod**.

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

The Prometheus metrics plugin exposes an `artemis_routed_message_count` gauge per address. Once both brokers show a non-zero value for `COMMANDS`, the topology is confirmed working: federation has propagated messages from the publishing broker to the consuming broker.

The broker's web server binds to the pod's headless service DNS name rather than `localhost` or the pod IP, so `curl` must be run from inside each pod using `kubectl exec`:

```bash
# broker-0
kubectl exec -n pub-sub-tutorial pub-sub-broker-ss-0 -c pub-sub-broker-container -- \
  curl -s http://pub-sub-broker-ss-0.pub-sub-broker-hdls-svc.pub-sub-tutorial.svc.cluster.local:8161/metrics/ \
  | grep 'artemis_routed_message_count.*COMMANDS'

# broker-1
kubectl exec -n pub-sub-tutorial pub-sub-broker-ss-1 -c pub-sub-broker-container -- \
  curl -s http://pub-sub-broker-ss-1.pub-sub-broker-hdls-svc.pub-sub-tutorial.svc.cluster.local:8161/metrics/ \
  | grep 'artemis_routed_message_count.*COMMANDS'
```

Both commands should return a line like the following with a value **greater than `0.0`**:

```
artemis_routed_message_count{address="COMMANDS",broker="amq-broker",} 12.0
```

The test verifies this exact condition: `artemis_routed_message_count{address="COMMANDS",broker="amq-broker",}` must be present and must not end in `} 0.0`.

If either value is `0.0`, wait a few seconds and retry — the federation mesh and consumer connections may still be establishing. The federation links retry every 1 second (`retryInterval=1000`), so both brokers should show non-zero counts within 10–20 seconds of all three deployments becoming ready.

You can also watch the consumer pod logs to confirm they are receiving messages (you will see repeated `retry` lines until each consumer lands on the correct broker, followed by silence once it is receiving):

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
