package org.example;

import akka.actor.*;
import org.example.Messages.*;

import java.util.concurrent.ThreadLocalRandom;
import java.time.Duration;
import java.util.*;

public class NodeActor extends AbstractActor {
    private final int nodeId;
    private final RingManager ringManager;
    private final Map<Integer, ValueResponse> localStorage = new HashMap<>();

    private final Map<Integer, List<ValueResponse>> pendingGets = new HashMap<>();
    private final Map<Integer, ActorRef> pendingClients = new HashMap<>();

    private final Map<Integer, List<Integer>> pendingWriteVersions = new HashMap<>();
    private final Map<Integer, UpdateRequest> pendingWriteRequests = new HashMap<>();
    private final Map<Integer, ActorRef> pendingWriteClients = new HashMap<>();

    private final Map<Integer, Cancellable> getTimeouts = new HashMap<>();
    private final Map<Integer, Cancellable> updateTimeouts = new HashMap<>();

    //parametri importati da Config.java
    int W = Config.W;
    int R = Config.R;
    int TIMEOUT = Config.T;
    int MIN_DELAY_MS = Config.MIN_DELAY_MS;
    int MAX_DELAY_MS = Config.MAX_DELAY_MS;


    public NodeActor(int id, RingManager ringManager) {
        this.nodeId = id;
        this.ringManager = ringManager;
    }
    
    private void delayedTell(ActorRef target, Object message) {
    int delay = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS);
    getContext().getSystem().scheduler().scheduleOnce(
        Duration.ofMillis(delay),
        () -> target.tell(message, getSelf()),
        getContext().getSystem().dispatcher()
    );
}


    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(UpdateRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Received UPDATE for key=" + msg.key + " value=" + msg.value);
                List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);
                if (responsible.contains(getSelf())) {
                    for (ActorRef replica : responsible) {
                        delayedTell(replica, new VersionRequest(msg.key));
                    }
                    pendingWriteVersions.put(msg.key, new ArrayList<>());
                    pendingWriteRequests.put(msg.key, msg);
                    pendingWriteClients.put(msg.key, getSender());

                    Cancellable timeout = getContext().getSystem().scheduler().scheduleOnce(
                        Duration.ofMillis(TIMEOUT),
                        () -> {
                            if (pendingWriteClients.containsKey(msg.key)) {
                                pendingWriteClients.get(msg.key).tell("Update failed: quorum not reached", getSelf());
                                pendingWriteClients.remove(msg.key);
                                pendingWriteVersions.remove(msg.key);
                                pendingWriteRequests.remove(msg.key);
                                updateTimeouts.remove(msg.key);
                            }
                        },
                        getContext().getSystem().dispatcher()
                    );
                    updateTimeouts.put(msg.key, timeout);
                } else {
                    ActorRef coordinator = responsible.get(0);
                    System.out.println("[Node " + nodeId + "] NOT responsible for key=" + msg.key + ", forwarding UPDATE to " + coordinator.path().name());
                    delayedTell(coordinator, msg);
                }
            })
            .match(VersionRequest.class, msg -> {
                int version = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0)).version;
                delayedTell(getSender(), new VersionResponse(msg.key, version));
            })
            .match(VersionResponse.class, msg -> {
                List<Integer> versions = pendingWriteVersions.get(msg.key);
                if (versions != null) {
                    versions.add(msg.version);
                    if (versions.size() >= W) {
                        int newVersion = Collections.max(versions) + 1;
                        UpdateRequest original = pendingWriteRequests.remove(msg.key);
                        ActorRef client = pendingWriteClients.remove(msg.key);
                        pendingWriteVersions.remove(msg.key);
                        Cancellable timeout = updateTimeouts.remove(msg.key);
                        if (timeout != null) timeout.cancel();

                        List<ActorRef> replicas = ringManager.getResponsibleNodes(msg.key);
                        for (ActorRef r : replicas) {
                            delayedTell(r, new UpdateInternal(msg.key, original.value, newVersion));

                        }
                        client.tell("Update committed with version " + newVersion, getSelf());
                    }
                }
            })
            .match(UpdateInternal.class, msg -> {
                ValueResponse existing = localStorage.get(msg.key);
                if (existing == null || msg.version > existing.version) {
                    localStorage.put(msg.key, new ValueResponse(msg.key, msg.value, msg.version));
                    System.out.println("[Node " + nodeId + "] Stored key=" + msg.key + " value=" + msg.value + " v=" + msg.version);
                } else {
                    System.out.println("[Node " + nodeId + "] Ignored outdated update for key=" + msg.key + " v=" + msg.version);
                }
            })

            .match(GetRequest.class, msg -> {
                List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);
                pendingGets.put(msg.key, new ArrayList<>());
                pendingClients.put(msg.key, getSender());
                for (ActorRef replica : responsible) {
                    delayedTell(replica, new InternalGet(msg.key));
                }

                Cancellable timeout = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.ofMillis(TIMEOUT),
                    () -> {
                        if (pendingClients.containsKey(msg.key)) {
                            pendingClients.get(msg.key).tell("GET failed: quorum not reached", getSelf());
                            pendingClients.remove(msg.key);
                            pendingGets.remove(msg.key);
                            getTimeouts.remove(msg.key);
                        }
                    },
                    getContext().getSystem().dispatcher()
                );
                getTimeouts.put(msg.key, timeout);
            })
            .match(InternalGet.class, msg -> {
                ValueResponse val = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0));
                delayedTell(getSender(), val);

            })
            .match(ValueResponse.class, msg -> {
                List<ValueResponse> responses = pendingGets.get(msg.key);
                if (responses != null) {
                    responses.add(msg);
                    System.out.println("[Node " + nodeId + "] Received version " + msg.version + " for key=" + msg.key);
                    if (responses.size() >= R) {
                        ValueResponse latest = responses.stream()
                            .max(Comparator.comparingInt(v -> v.version))
                            .orElse(new ValueResponse(msg.key, "", 0));
                        ActorRef client = pendingClients.remove(msg.key);
                        if (client != null) {
                            client.tell("GET key=" + msg.key + " -> value=" + latest.value + " [v=" + latest.version + "]", getSelf());
                        }
                        pendingGets.remove(msg.key);
                        Cancellable timeout = getTimeouts.remove(msg.key);
                        if (timeout != null) timeout.cancel();
                    }
                }
            })
            .match(JoinRequest.class, msg -> {
                delayedTell(msg.newNode, new NodeListResponse(ringManager.getNodeMap()));
            })
            .match(NodeListResponse.class, msg -> {
                for (Map.Entry<Integer, ActorRef> entry : msg.nodes.entrySet()) {
                    ringManager.addNode(entry.getKey(), entry.getValue());
                }
                ringManager.addNode(nodeId, getSelf());
                ActorRef successor = ringManager.getClockwiseSuccessor(nodeId);
                delayedTell(successor, new TransferKeysRequest(nodeId));

            })
            .match(TransferKeysRequest.class, msg -> {
                ringManager.addNode(msg.newNodeId, getSender());
                Map<Integer, ValueResponse> toTransfer = new HashMap<>();
                for (Map.Entry<Integer, ValueResponse> entry : localStorage.entrySet()) {
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(entry.getKey());
                    ActorRef newPrimary = newResponsible.isEmpty() ? null : newResponsible.get(0);
                    ActorRef targetNode = ringManager.getNodeMap().get(msg.newNodeId);
                    if (targetNode != null && targetNode.equals(newPrimary)) {
                        toTransfer.put(entry.getKey(), entry.getValue());
                        localStorage.remove(entry.getKey());
                        System.out.println("[Node " + nodeId + "] Transferred & removed key=" + entry.getKey() + " to node " + msg.newNodeId);
                    }
                }
                delayedTell(getSender(), new TransferKeysResponse(toTransfer));

            })
            .match(TransferKeysResponse.class, msg -> {
                for (Map.Entry<Integer, ValueResponse> entry : msg.data.entrySet()) {
                    int key = entry.getKey();
                    List<ActorRef> responsible = ringManager.getResponsibleNodes(key);
                    if (responsible.contains(getSelf())) {
                        localStorage.put(key, entry.getValue());
                        System.out.println("[Node " + nodeId + "] Recovered key=" + key);
                    } else {
                        localStorage.remove(key);
                    }
                }
            })
            .match(RecoverRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling RECOVERY");

                Map<Integer, ActorRef> currentNodes = ringManager.getNodeMap();
                for (Map.Entry<Integer, ActorRef> entry : currentNodes.entrySet()) {
                    if (!entry.getKey().equals(nodeId)) {
                        ringManager.addNode(entry.getKey(), entry.getValue());
                    }
                }
                ringManager.addNode(nodeId, getSelf());

                ActorRef successor = ringManager.getClockwiseSuccessor(nodeId);
                delayedTell(successor, new TransferKeysRequest(nodeId));

            })
            .match(LeaveRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling LEAVE");
                ActorRef successor = ringManager.getClockwiseSuccessor(nodeId);
                for (Map.Entry<Integer, ValueResponse> entry : localStorage.entrySet()) {
                    List<ActorRef> responsible = ringManager.getResponsibleNodes(entry.getKey());
                    ActorRef newPrimary = responsible.isEmpty() ? null : responsible.get(0);
                    if (newPrimary != null && !newPrimary.equals(getSelf())) {
                        delayedTell(successor, new UpdateInternal(entry.getKey(), entry.getValue().value, entry.getValue().version));
                        System.out.println("[Node " + nodeId + "] Transferred key=" + entry.getKey() + " to " + successor.path().name());
                    }
                }
                for (ActorRef peer : ringManager.getNodeMap().values()) {
                    if (!peer.equals(getSelf())) {
                        delayedTell(peer, new LeaveNotification(nodeId));
                    }
                }
                localStorage.clear();
                ringManager.removeNode(nodeId);
                getSender().tell(new LeaveAck(nodeId), getSelf());
                getContext().stop(getSelf());
            })
            .match(LeaveNotification.class, msg -> {
                ringManager.removeNode(msg.nodeId);
                System.out.println("[Node " + nodeId + "] Removed node " + msg.nodeId + " from ring.");
            })
            //degub 
            .matchEquals("print_storage", msg -> {
                System.out.println("[Node " + nodeId + "] keys in localStorage: " + localStorage.keySet());
            })
            //fine debug

            .build();
    }
}
