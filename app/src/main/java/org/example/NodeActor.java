package org.example;

import akka.actor.*;
import org.example.Messages.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class NodeActor extends AbstractActor {
    private final int nodeId;
    private final RingManager ringManager;
    private final Map<Integer, ValueResponse> localStorage;

    private final Map<Integer, List<ValueResponse>> pendingGets = new HashMap<>();
    private final Map<Integer, ActorRef> pendingClients = new HashMap<>();
    private final Map<Integer, Cancellable> getTimeouts = new HashMap<>();

    // CONTEXT PER GESTIRE WRITE SERIALIZZATE
    private static class WriteCtx {
        final UpdateRequest req;
        final ActorRef client;
        final List<Integer> versions = new ArrayList<>();
        Cancellable timeout;

        WriteCtx(UpdateRequest req, ActorRef client) {
            this.req = req;
            this.client = client;
        }
    }

    private final Map<Integer, Deque<WriteCtx>> writeQueues = new HashMap<>();
    private final Map<Integer, WriteCtx> inFlightWrite = new HashMap<>();

    int W = Config.W;
    int R = Config.R;
    int TIMEOUT = Config.T;
    int MIN_DELAY_MS = Config.MIN_DELAY_MS;
    int MAX_DELAY_MS = Config.MAX_DELAY_MS;

    public NodeActor(int id, RingManager ringManager) {
        this.nodeId = id;
        this.ringManager = ringManager;
        this.localStorage = PersistentStorage.getStorage(id);
    }

    private void delayedTell(ActorRef target, Object message) {
        int delay = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS);
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.ofMillis(delay),
            () -> target.tell(message, getSelf()),
            getContext().getSystem().dispatcher()
        );
    }

    private void startNextWriteIfIdle(int key) {
        if (inFlightWrite.containsKey(key)) return;
        Deque<WriteCtx> q = writeQueues.get(key);
        if (q == null || q.isEmpty()) return;

        WriteCtx ctx = q.pollFirst();
        inFlightWrite.put(key, ctx);

        for (ActorRef replica : ringManager.getResponsibleNodes(key)) {
            delayedTell(replica, new VersionRequest(key));
        }

        ctx.timeout = getContext().getSystem().scheduler().scheduleOnce(
            Duration.ofMillis(TIMEOUT),
            () -> {
                WriteCtx cur = inFlightWrite.remove(key);
                if (cur != null) {
                    cur.client.tell("Update failed: quorum not reached", getSelf());
                }
                startNextWriteIfIdle(key);
            },
            getContext().getSystem().dispatcher()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            // UPDATE — enqueue e processa una alla volta per chiave
            .match(UpdateRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Received UPDATE for key=" + msg.key + " value=" + msg.value);
                List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);

                if (responsible.contains(getSelf())) {
                    writeQueues.computeIfAbsent(msg.key, k -> new ArrayDeque<>())
                               .addLast(new WriteCtx(msg, getSender()));
                    startNextWriteIfIdle(msg.key);
                } else {
                    ActorRef coordinator = responsible.get(0);
                    System.out.println("[Node " + nodeId + "] NOT responsible for key=" + msg.key + ", forwarding UPDATE to " + coordinator.path().name());
                    delayedTell(coordinator, msg);
                }
            })

            // Risposta con la versione corrente
            .match(VersionRequest.class, msg -> {
                int version = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0)).version;
                delayedTell(getSender(), new VersionResponse(msg.key, version));
            })

            // Coordinatore riceve versioni → calcola nuova versione → invia commit
            .match(VersionResponse.class, msg -> {
                WriteCtx ctx = inFlightWrite.get(msg.key);
                if (ctx == null) return;

                ctx.versions.add(msg.version);

                if (ctx.versions.size() >= W) {
                    // calcolo versione rispetto alle versioni ricevute e a quella locale
                    int localVersion = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0)).version;
                    int newVersion = Math.max(Collections.max(ctx.versions), localVersion) + 1;

                    if (ctx.timeout != null) ctx.timeout.cancel();

                    for (ActorRef r : ringManager.getResponsibleNodes(msg.key)) {
                        delayedTell(r, new UpdateInternal(msg.key, ctx.req.value, newVersion));
                    }

                    ctx.client.tell("Update committed with version " + newVersion, getSelf());

                    inFlightWrite.remove(msg.key);
                    startNextWriteIfIdle(msg.key);
                }
            })

            // Replica salva il valore aggiornato se ha versione più alta
            .match(UpdateInternal.class, msg -> {
                ValueResponse existing = localStorage.get(msg.key);
                if (existing == null || msg.version > existing.version) {
                    localStorage.put(msg.key, new ValueResponse(msg.key, msg.value, msg.version));
                    System.out.println("[Node " + nodeId + "] Stored key=" + msg.key + " value=" + msg.value + " v=" + msg.version);
                } else {
                    System.out.println("[Node " + nodeId + "] Ignored outdated update for key=" + msg.key + " v=" + msg.version);
                }
            })

            // GET → (SC) rifiuta se c'è una WRITE in corso/in coda; altrimenti quorum R e versione massima
            .match(GetRequest.class, msg -> {
                // --- SC minimale: rifiuta la GET se c'è una WRITE in corso o in coda su questa key ---
                boolean writeOngoing = inFlightWrite.containsKey(msg.key);
                boolean writeQueued = writeQueues.getOrDefault(msg.key, new ArrayDeque<>()).size() > 0;
                if (writeOngoing || writeQueued) {
                    getSender().tell("GET rejected: overlapping write on key " + msg.key, getSelf());
                    return;
                }
                // --- normale gestione GET ---
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

            // === JOIN ===
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

                // Snapshot per evitare ConcurrentModification durante la rimozione
                List<Map.Entry<Integer, ValueResponse>> snapshot = new ArrayList<>(localStorage.entrySet());
                Map<Integer, ValueResponse> toTransfer = new HashMap<>();

                ActorRef targetNode = ringManager.getNodeMap().get(msg.newNodeId);
                for (Map.Entry<Integer, ValueResponse> entry : snapshot) {
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(entry.getKey());
                    ActorRef newPrimary = newResponsible.isEmpty() ? null : newResponsible.get(0);
                    if (targetNode != null && targetNode.equals(newPrimary)) {
                        toTransfer.put(entry.getKey(), entry.getValue());
                    }
                }
                // rimuovi ora (fuori dal loop)
                for (Integer k : toTransfer.keySet()) {
                    localStorage.remove(k);
                    System.out.println("[Node " + nodeId + "] Transferred & removed key=" + k + " to node " + msg.newNodeId);
                }

                delayedTell(getSender(), new TransferKeysResponse(toTransfer));
            })

            .match(TransferKeysResponse.class, msg -> {
                // 1) ricevi e salva
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
                // 2) READ-BACK minimo: riusa la tua GET per allinearti alla versione più recente
                for (Integer k : msg.data.keySet()) {
                    getSelf().tell(new GetRequest(k), getSelf()); // la risposta (stringa) a self verrà ignorata
                }
            })

            // === RECOVERY ===
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

            // === LEAVE ===
            .match(LeaveRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling LEAVE");

                // invia ai veri nuovi responsabili (tutte le N repliche), non solo al successore
                for (Map.Entry<Integer, ValueResponse> entry : localStorage.entrySet()) {
                    int key = entry.getKey();
                    ValueResponse vr = entry.getValue();
                    List<ActorRef> responsible = ringManager.getResponsibleNodes(key);
                    for (ActorRef r : responsible) {
                        if (!r.equals(getSelf())) {
                            delayedTell(r, new UpdateInternal(key, vr.value, vr.version));
                        }
                    }
                    System.out.println("[Node " + nodeId + "] Transferred key=" + key + " to new responsible set size=" + responsible.size());
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

            // DEBUG
            .matchEquals("print_storage", msg -> {
                System.out.println("[Node " + nodeId + "] keys in localStorage: " + localStorage.keySet());
            })

            .build();
    }
}
