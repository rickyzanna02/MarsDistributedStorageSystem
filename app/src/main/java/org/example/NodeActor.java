// NodeActor.java: defines the behavior of the nodes in the system,
// implementing the message handlers defined in Messages.java.
// It handles client requests (GET, UPDATE), enforces quorum and sequential consistency,
// and manages membership changes (JOIN, LEAVE, RECOVERY) with the corresponding data transfers.

package org.example;
import akka.actor.*;
import org.example.Messages.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class NodeActor extends AbstractActor {
    private final int nodeId;                                                      // node identifier 
    private final RingManager ringManager;                                         // ring manager    
    private final Map<Integer, ValueResponse> localStorage;                        // persistent storage
    private final Map<Integer, List<ValueResponse>> pendingGets = new HashMap<>(); // multiple GET per key
    private final Map<Integer, List<ActorRef>> pendingClients = new HashMap<>();   // multiple clients per GET key
    private final Map<Integer, Cancellable> getTimeouts = new HashMap<>();         // timeout per GET key
    private final Set<Integer> pendingWrites = new HashSet<>();                    // multiple WRITES per key
    private final Map<Integer, Cancellable> pendingWriteTimers = new HashMap<>();  // pendingWrites timeout per key
    private final Map<Integer, Deque<WriteCtx>> writeQueues = new HashMap<>();     // write queue per key
    private final Map<Integer, WriteCtx> inFlightWrite = new HashMap<>();          // current write per key

    private boolean recovering = false;                         // true during recovery
    private boolean joining = false;                            // true during join (new node)
    private ActorRef recoveryBootstrap = ActorRef.noSender();   // bootstrap node for recovery

    // Config parameters
    int W = Config.W;
    int R = Config.R;
    int TIMEOUT = Config.T;
    int MIN_DELAY_MS = Config.MIN_DELAY_MS;
    int MAX_DELAY_MS = Config.MAX_DELAY_MS;

    // Constructor
    public NodeActor(int id, RingManager ringManager) {
        this.nodeId = id;
        this.ringManager = ringManager;
        this.localStorage = PersistentStorage.getStorage(id);
    }

    // keep state for in-flight writes (per key)
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

    // network delay simulation (with self as sender)
    private void delayedTell(ActorRef target, Object message) {
        int delay = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS);
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.ofMillis(delay),
            () -> target.tell(message, getSelf()),
            getContext().getSystem().dispatcher()
        );
    }

    // network delay simulation (with original sender)
    private void delayedTell(ActorRef target, Object message, ActorRef originalSender) {
        int delay = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS);
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.ofMillis(delay),
            () -> target.tell(message, originalSender),
            getContext().getSystem().dispatcher()
        );
    }

    // start next write if none in-flight for the key
    private void startNextWriteIfIdle(int key) {
        if (inFlightWrite.containsKey(key)) return;
        Deque<WriteCtx> q = writeQueues.get(key);
        if (q == null || q.isEmpty()) return;
        WriteCtx ctx = q.pollFirst();
        inFlightWrite.put(key, ctx);
        
        for (ActorRef replica : ringManager.getResponsibleNodes(key)) {// VersionRequest to all replicas (including primary)
            delayedTell(replica, new VersionRequest(key));
        }
        ctx.timeout = getContext().getSystem().scheduler().scheduleOnce( // timeout management
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

    // remove keys not responsible anymore for a node (after join/leave)
    private void dropKeysNotResponsibleAnymore() {
        Set<Integer> keys = new HashSet<>(localStorage.keySet());
        for (Integer k : keys) {
            List<ActorRef> resp = ringManager.getResponsibleNodes(k);
            if (!resp.contains(getSelf())) {
                localStorage.remove(k);
            }
        }
    }

    // read-back for all keys the node is now responsible for (after recovery)
    private void readBackAllResponsibleKeys() {
        for (Integer k : new HashSet<>(localStorage.keySet())) {
            getSelf().tell(new GetRequest(k), getSelf());
        }
    }

    // pick a bootstrap node if no bootstrap is passed in RecoverRequest(int)
    private ActorRef pickBootstrapFromRing() {
        for (Map.Entry<Integer, ActorRef> e : ringManager.getNodeMap().entrySet()) {
            if (!e.getValue().equals(getSelf())) return e.getValue();
        }
        return null;
    }

    // recover ActorRef from id (utility)
    private ActorRef refOf(int id) {
        return ringManager.getNodeMap().get(id);
    }

    // cleanup GET state for a key
    private void finishGet(int key) {
        pendingGets.remove(key);
        pendingClients.remove(key);
        Cancellable t = getTimeouts.remove(key);
        if (t != null) t.cancel();
    }

    // Announce JOIN to all and force drop of items no longer responsible
    private void broadcastJoinAndDrop() {
        for (Map.Entry<Integer, ActorRef> e : ringManager.getNodeMap().entrySet()) {
            ActorRef peer = e.getValue();
            delayedTell(peer, new JoinNotification(nodeId, getSelf()));
        }
        dropKeysNotResponsibleAnymore();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()

            // UpdateRequest: Write request from a client (key, value)
            .match(UpdateRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Received UPDATE for key=" + msg.key + " value=" + msg.value);

                // Determine responsible nodes and primary
                List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);
                if (responsible == null || responsible.isEmpty()) {
                    ActorRef client = getSender();
                    if (client != null) {
                        client.tell("UPDATE failed: no responsible nodes for key " + msg.key, getSelf());
                    }
                    System.out.println("[Node " + nodeId + "] No responsible nodes for key=" + msg.key + " -> abort");
                    return;
                }

                ActorRef primary = responsible.get(0);

                // forwarding if not primary, preserving sender
                if (!getSelf().equals(primary)) {
                    System.out.println("[Node " + nodeId + "] Not primary for key=" + msg.key +
                            " (primary=" + primary.path().name() + "), forwarding UPDATE");
                    delayedTell(primary, msg, getSender());
                    return;
                }

                // I'm the primary: coordinate the write
                writeQueues.computeIfAbsent(msg.key, k -> new ArrayDeque<>())
                        .addLast(new WriteCtx(msg, getSender())); 

                startNextWriteIfIdle(msg.key);
            })

            // VersionRequest: Request the current version number for a replica (used in W quorum)
            .match(VersionRequest.class, msg -> {
                Cancellable old = pendingWriteTimers.remove(msg.key);
                if (old != null) old.cancel();
                pendingWrites.add(msg.key);
                Cancellable t = getContext().getSystem().scheduler().scheduleOnce(
                    Duration.ofMillis(TIMEOUT),
                    () -> {
                        pendingWrites.remove(msg.key);
                        pendingWriteTimers.remove(msg.key);
                    },
                    getContext().getSystem().dispatcher()
                );
                pendingWriteTimers.put(msg.key, t);

                int version = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0)).version;
                delayedTell(getSender(), new VersionResponse(msg.key, version));
            })

            // VersionResponse: coordinator receives version numbers → calculates new version → sends commit
            .match(VersionResponse.class, msg -> {
                WriteCtx ctx = inFlightWrite.get(msg.key);
                if (ctx == null) return;

                ctx.versions.add(msg.version);

                if (ctx.versions.size() >= W) {
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

            // UpdateInternal: replicas save the updated value if it has a higher version
            .match(UpdateInternal.class, msg -> {
                ValueResponse existing = localStorage.get(msg.key);
                if (existing == null || msg.version > existing.version) {
                    localStorage.put(msg.key, new ValueResponse(msg.key, msg.value, msg.version));
                    System.out.println("[Node " + nodeId + "] Stored key=" + msg.key + " value=" + msg.value + " v=" + msg.version);
                } else {
                    System.out.println("[Node " + nodeId + "] Ignored outdated update for key=" + msg.key + " v=" + msg.version);
                }
                Cancellable pt = pendingWriteTimers.remove(msg.key);
                if (pt != null) pt.cancel();
                pendingWrites.remove(msg.key);
            })

            //GetRequest: Read request from a client (refused if overlapping write)
            .match(GetRequest.class, msg -> {
                boolean writeOngoing = inFlightWrite.containsKey(msg.key);
                if(writeOngoing) {
                    getSender().tell("GET rejected: overlapping write on key " + msg.key, getSelf());
                    return;
                }

                // multiple clients: ensure all are replied
                List<ActorRef> clients = pendingClients.computeIfAbsent(msg.key, k -> new ArrayList<>());
                clients.add(getSender());

                // if there is already a pending GET for the same key, do not re-launch InternalGet
                if (pendingGets.containsKey(msg.key)) {
                    return;
                }

                // If this is the first GET for this key: start fan-out and timeout
                pendingGets.put(msg.key, new ArrayList<>());
                List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);
                for (ActorRef replica : responsible) {
                    delayedTell(replica, new InternalGet(msg.key));
                }

                Cancellable timeout = getContext().getSystem().scheduler().scheduleOnce( //quorum management
                    Duration.ofMillis(TIMEOUT),
                    () -> {
                        List<ActorRef> cl = pendingClients.get(msg.key);
                        if (cl != null && !cl.isEmpty()) {
                            for (ActorRef c : cl) c.tell("GET failed: quorum not reached", getSelf());
                        }
                        finishGet(msg.key);
                    },
                    getContext().getSystem().dispatcher()
                );
                getTimeouts.put(msg.key, timeout);
            })

            
            //InternalGet: replica receives internal read request
            .match(InternalGet.class, msg -> {
                if (pendingWrites.contains(msg.key)) { //if there is a pending write, respond with PendingResponse
                    delayedTell(getSender(), new PendingResponse(msg.key));
                } else {
                    ValueResponse val = localStorage.getOrDefault(msg.key, new ValueResponse(msg.key, "", 0));
                    delayedTell(getSender(), val);
                }
            })

            // PendingResponse: "hard" SC: reject GET and re-issue InternalGet after random delay
            .match(PendingResponse.class, msg -> {
                final ActorRef pendingReplica = getSender();
                getContext().getSystem().scheduler().scheduleOnce(
                    Duration.ofMillis(ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS)),
                    () -> pendingReplica.tell(new InternalGet(msg.key), getSelf()),
                    getContext().getSystem().dispatcher()
                );
            })

            // ValueResponse: replica responds with value and version
            .match(ValueResponse.class, msg -> {
                List<ValueResponse> responses = pendingGets.get(msg.key);
                if (responses == null) return; // GET already decided or does not exist
                responses.add(msg);
                System.out.println("[Node " + nodeId + "] Received version " + msg.version + " for key=" + msg.key);

                if (responses.size() >= R) { // keep the first R responses and keep the max version
                    ValueResponse latest = responses.stream()
                        .max(Comparator.comparingInt(v -> v.version))
                        .orElse(new ValueResponse(msg.key, "", 0));

                    List<ActorRef> clients = pendingClients.get(msg.key);

                    if (latest.version == 0) {
                        // "not found" if the best is v=0
                        if (clients != null) {
                            for (ActorRef c : clients) {
                                c.tell("GET failed: not found", getSelf());
                            }
                        }
                        finishGet(msg.key);
                        return;
                    }

                    ValueResponse cur = localStorage.get(msg.key);
                    if (cur == null || latest.version > cur.version) { //update local storage if needed (read-repair)
                        localStorage.put(msg.key, new ValueResponse(msg.key, latest.value, latest.version));
                        System.out.println("[Node " + nodeId + "] Read-repair key=" + msg.key + " -> v=" + latest.version);
                    }

                    if (clients != null) {
                        for (ActorRef c : clients) {
                            c.tell("GET key=" + msg.key + " -> value=" + latest.value + " [v=" + latest.version + "]", getSelf());
                        }
                    }
                    finishGet(msg.key);
                }
            })

            // =========================================
            // JOIN — bootstrap → nuovo nodo, donatore → nuovo nodo, annuncio globale
            // =========================================
            // JoinRequest: new node requests to join the ring (to bootstrap)
            .match(JoinRequest.class, msg -> {
                // I'm the bootstrap, I send the node list (id->ref) to the new node
                delayedTell(msg.newNode, new NodeListResponse(ringManager.getNodeMap()));
            })

            // The NEW node receives the NodeList and triggers the transfer from the clockwise successor
            .match(NodeListResponse.class, msg -> {
                // Update local view of the ring
                for (Map.Entry<Integer, ActorRef> e : msg.nodes.entrySet()) {
                    ringManager.addNode(e.getKey(), e.getValue());
                }
                ringManager.addNode(nodeId, getSelf()); 
                if (recovering) {
                    // RECOVERY CASE: drop keys not responsible anymore
                    System.out.println("[Node " + nodeId + "] RECOVERY: received NodeListResponse");
                    dropKeysNotResponsibleAnymore();
                    for (ActorRef peer : ringManager.getNodeMap().values()) {
                        if (!peer.equals(getSelf())) {
                            delayedTell(peer, new RecoveryFetchRequest(nodeId));
                        }
                    }
                    return;
                }
                // JOIN CASE: start transfer from successor
                System.out.println("[Node " + nodeId + "] JOIN: received NodeListResponse");
                joining = true;
                ActorRef successor = ringManager.getClockwiseSuccessor(nodeId);
                if (successor != null) {
                    delayedTell(successor, new TransferKeysRequest(nodeId));
                } else {
                    // No successor? Announce join anyway (empty/single ring)
                    broadcastJoinAndDrop();
                    joining = false;
                }
            })

            //TransferKeysRequest: request to transfer keys to a new node (sent to successor)
            .match(TransferKeysRequest.class, msg -> {
                Map<Integer, ValueResponse> toTransfer = new HashMap<>();
                ActorRef targetNode = refOf(msg.newNodeId);

                // for each key, if the new node is now primary, include it in the transfer
                for (Map.Entry<Integer, ValueResponse> entry : new ArrayList<>(localStorage.entrySet())) {
                    int key = entry.getKey();
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(key);
                    ActorRef newPrimary = newResponsible.isEmpty() ? null : newResponsible.get(0);
                    if (targetNode != null && targetNode.equals(newPrimary)) {
                        toTransfer.put(key, entry.getValue());
                    }
                }
                delayedTell(getSender(), new TransferKeysResponse(toTransfer));

                //Now remove locally the transferred items (JOIN semantics)
                for (Integer k : toTransfer.keySet()) {
                    localStorage.remove(k);
                    System.out.println("[Node " + nodeId + "] Transferred & removed key=" + k + " to node " + msg.newNodeId);
                }
            })

            // TransferKeysResponse: new node receives data, does read-back, then announces join to all
            .match(TransferKeysResponse.class, msg -> {
                if (!msg.data.isEmpty()) {
                    // Save received data if responsible
                    for (Map.Entry<Integer, ValueResponse> entry : msg.data.entrySet()) {
                        int key = entry.getKey();
                        List<ActorRef> responsible = ringManager.getResponsibleNodes(key);
                        if (responsible.contains(getSelf())) {
                            localStorage.put(key, entry.getValue());
                            System.out.println("[Node " + nodeId + "] JOIN imported key=" + key + " v=" + entry.getValue().version);
                        } else {
                            localStorage.remove(key);
                        }
                    }
                    // Read-back to ensure consistency
                    for (Integer k : msg.data.keySet()) {
                        getSelf().tell(new GetRequest(k), getSelf());
                    }
                }
                // Announce JOIN to all
                if (joining) {
                    broadcastJoinAndDrop();
                    joining = false;
                }
            })

            // Global update after join
            .match(JoinNotification.class, jn -> {
                ringManager.addNode(jn.nodeId, jn.ref);
                dropKeysNotResponsibleAnymore();
            })

            
            // RecoverRequest: node requests recovery (after crash)
            .match(RecoverRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling RECOVERY");
                recovering = true;
                // pick bootstrap from message or from ring
                ActorRef bootstrap = (msg.bootstrap != null && !msg.bootstrap.equals(ActorRef.noSender()))
                        ? msg.bootstrap // from message
                        : pickBootstrapFromRing(); // from ring
                // no bootstrap? abort
                if (bootstrap == null) {
                    System.out.println("[Node " + nodeId + "] RECOVERY aborted: no bootstrap available.");
                    recovering = false;
                    return;
                }

                recoveryBootstrap = bootstrap;
                // bootstrap requests the node list
                delayedTell(recoveryBootstrap, new NodeListRequest(nodeId, getSelf()));
            })
            
            // RecoveryFetchRequest: recovering node requests data from other replicas
            .match(RecoveryFetchRequest.class, msg -> {
                // Donator side: respond with items for which the requester is PRIMARY in the current ring
                Map<Integer, ValueResponse> copyForRequester = new HashMap<>();
                ActorRef requester = refOf(msg.requesterId);

                for (Map.Entry<Integer, ValueResponse> e : localStorage.entrySet()) {
                    int key = e.getKey();
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(key);
                    ActorRef newPrimary = newResponsible.isEmpty() ? null : newResponsible.get(0);
                    if (requester != null && requester.equals(newPrimary)) {
                        copyForRequester.put(key, e.getValue());
                    }
                }
                delayedTell(getSender(), new RecoveryFetchResponse(copyForRequester));
            })

            // RecoveryFetchResponse: recovering node receives data, saves, then does read-back
            .match(RecoveryFetchResponse.class, msg -> {
                // Recovering side: save and then read-back
                for (Map.Entry<Integer, ValueResponse> entry : msg.data.entrySet()) {
                    int key = entry.getKey();
                    List<ActorRef> responsible = ringManager.getResponsibleNodes(key);
                    if (responsible.contains(getSelf())) {
                        localStorage.put(key, entry.getValue());
                        System.out.println("[Node " + nodeId + "] Recovery copied key=" + key + " v=" + entry.getValue().version);
                    }
                }
                readBackAllResponsibleKeys();
                recovering = false;
                recoveryBootstrap = ActorRef.noSender();
            })

            
            // NodeListRequest: request the current list of nodes in the ring (used in recovery)
            .match(NodeListRequest.class, msg -> {
                if (msg.requesterId != null && msg.requester != null) {
                    ringManager.addNode(msg.requesterId, msg.requester);
                }
                delayedTell(getSender(), new NodeListResponse(ringManager.getNodeMap()));
            })

            // LeaveRequest: node requests to leave the ring
            .match(LeaveRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling LEAVE");

                // 1) Snapshot of local data
                Map<Integer, ValueResponse> snapshot = new HashMap<>(localStorage);

                // 2) Remove self from LOCAL view (to compute new correct responsibles)
                ringManager.removeNode(nodeId);

                // 3) For each key, send to all new responsibles (N replicas)
                for (Map.Entry<Integer, ValueResponse> entry : snapshot.entrySet()) {
                    int key = entry.getKey();
                    ValueResponse vr = entry.getValue();
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(key); // ora senza self

                    for (ActorRef r : newResponsible) {
                        if (!r.equals(getSelf())) {
                            delayedTell(r, new UpdateInternal(key, vr.value, vr.version));
                        }
                    }
                    System.out.println("[Node " + nodeId + "] Transferred key=" + key + " to new responsible set size=" + newResponsible.size());
                }

                // 4) Notify others that you are leaving, so they update their view and drop
                for (ActorRef peer : ringManager.getNodeMap().values()) {
                    if (!peer.equals(getSelf())) {
                        delayedTell(peer, new LeaveNotification(nodeId));
                    }
                }

                // 5) Clear local storage and terminate
                localStorage.clear();
                getSender().tell(nodeId +": LEAVE_OK", getSelf());
                getContext().stop(getSelf());
            })

            // LeaveNotification: all nodes update their view and drop keys not responsible anymore
            .match(LeaveNotification.class, msg -> {
                ringManager.removeNode(msg.nodeId);
                dropKeysNotResponsibleAnymore();
                System.out.println("[Node " + nodeId + "] Removed node " + msg.nodeId + " from ring.");
            })

            // Print local storage (for debugging)
            .matchEquals("print_storage", msg -> {
                System.out.println("[Node " + nodeId + "] keys in localStorage: " + localStorage.keySet());
            })

            .build();
    }

}
