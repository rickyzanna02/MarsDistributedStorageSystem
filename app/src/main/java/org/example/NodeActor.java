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

    // === GET state ===
    private final Map<Integer, List<ValueResponse>> pendingGets = new HashMap<>();
    private final Map<Integer, ActorRef> pendingClients = new HashMap<>();
    private final Map<Integer, Cancellable> getTimeouts = new HashMap<>();

    // === WRITE state (serializzazione per chiave) ===
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

    // === JOIN / RECOVERY stato ===
    private boolean recovering = false;           // true durante recovery
    private boolean joining = false;             // true durante join (nuovo nodo)
    private ActorRef recoveryBootstrap = ActorRef.noSender();

    // === Config ===
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

    // Delay helper per simulare rete affidabile con jitter
    private void delayedTell(ActorRef target, Object message) {
        int delay = ThreadLocalRandom.current().nextInt(MIN_DELAY_MS, MAX_DELAY_MS);
        getContext().getSystem().scheduler().scheduleOnce(
            Duration.ofMillis(delay),
            () -> target.tell(message, getSelf()),
            getContext().getSystem().dispatcher()
        );
    }

    // Avvio della prossima write (se idle) sulla key
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

    // === Helpers comuni ===
    private void dropKeysNotResponsibleAnymore() {
        Set<Integer> keys = new HashSet<>(localStorage.keySet());
        for (Integer k : keys) {
            List<ActorRef> resp = ringManager.getResponsibleNodes(k);
            if (!resp.contains(getSelf())) {
                localStorage.remove(k);
            }
        }
    }

    private void readBackAllResponsibleKeys() {
        for (Integer k : new HashSet<>(localStorage.keySet())) {
            getSelf().tell(new GetRequest(k), getSelf()); // la risposta stringa verso self è ignorata
        }
    }

    // Fallback se non viene passato un bootstrap nel RecoverRequest(int)
    private ActorRef pickBootstrapFromRing() {
        for (Map.Entry<Integer, ActorRef> e : ringManager.getNodeMap().entrySet()) {
            if (!e.getValue().equals(getSelf())) return e.getValue();
        }
        return null;
    }

    // Recupera ActorRef da id (utility)
    private ActorRef refOf(int id) {
        return ringManager.getNodeMap().get(id);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()

            // =========================================
            // UPDATE — enqueue e processa una alla volta per chiave
            // =========================================
        .match(UpdateRequest.class, msg -> {
            System.out.println("[Node " + nodeId + "] Received UPDATE for key=" + msg.key + " value=" + msg.value);

            // Determina i responsabili e il primario
            List<ActorRef> responsible = ringManager.getResponsibleNodes(msg.key);
                if (responsible == null || responsible.isEmpty()) {
                    // Anello vuoto o errore in RingManager: rispondi al client con errore e abortisci
                    ActorRef client = getSender();
                    if (client != null) {
                        client.tell("UPDATE failed: no responsible nodes for key " + msg.key, getSelf());
                    }
                    System.out.println("[Node " + nodeId + "] No responsible nodes for key=" + msg.key + " -> abort");
                    return;
                }

                ActorRef primary = responsible.get(0);

                // Se non sono il primario, inoltro SEMPRE a lui (coordinamento unico)
                if (!getSelf().equals(primary)) {
                    System.out.println("[Node " + nodeId + "] Not primary for key=" + msg.key +
                                    " (primary=" + primary.path().name() + "), forwarding UPDATE");
                    delayedTell(primary, msg); // mantiene lo stesso comportamento di inoltro già usato nel tuo codice
                    return;
                }

                // Da qui in poi: sono il PRIMARIO -> coordino la write
                writeQueues.computeIfAbsent(msg.key, k -> new ArrayDeque<>())
                        .addLast(new WriteCtx(msg, getSender())); // salva il client originale

                // Avvia la write se non ce n'è già una in corso per la chiave
                startNextWriteIfIdle(msg.key);
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

            // =========================================
            // GET — (SC) rifiuta se esiste WRITE in corso/in coda; altrimenti quorum R
            // =========================================
            .match(GetRequest.class, msg -> {
                boolean writeOngoing = inFlightWrite.containsKey(msg.key);
                boolean writeQueued = writeQueues.getOrDefault(msg.key, new ArrayDeque<>()).size() > 0;
                if (writeOngoing || writeQueued) {
                    getSender().tell("GET rejected: overlapping write on key " + msg.key, getSelf());
                    return;
                }

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

                        // === READ-REPAIR LOCALE ===
                        ValueResponse cur = localStorage.get(msg.key);
                        if (cur == null || latest.version > cur.version) {
                            localStorage.put(msg.key, new ValueResponse(msg.key, latest.value, latest.version));
                            System.out.println("[Node " + nodeId + "] Read-repair key=" + msg.key + " -> v=" + latest.version);
                        }

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

            // =========================================
            // JOIN — bootstrap → nuovo nodo, donatore → nuovo nodo, annuncio globale
            // =========================================
            .match(JoinRequest.class, msg -> {
                // Siamo il bootstrap: inviamo la lista nodi (id->ref) al nuovo nodo
                delayedTell(msg.newNode, new NodeListResponse(ringManager.getNodeMap()));
            })

            // Il NUOVO nodo riceve la NodeList e innesca il trasferimento dal successore orario
            .match(NodeListResponse.class, msg -> {
                // Aggiorna vista locale del ring
                for (Map.Entry<Integer, ActorRef> e : msg.nodes.entrySet()) {
                    ringManager.addNode(e.getKey(), e.getValue());
                }
                ringManager.addNode(nodeId, getSelf()); // assicurati che ci siamo anche noi

                if (recovering) {
                    // Caso RECOVERY: drop e fetch dai peer
                    System.out.println("[Node " + nodeId + "] RECOVERY: received NodeListResponse");
                    dropKeysNotResponsibleAnymore();
                    for (ActorRef peer : ringManager.getNodeMap().values()) {
                        if (!peer.equals(getSelf())) {
                            delayedTell(peer, new RecoveryFetchRequest(nodeId));
                        }
                    }
                    return;
                }

                // Caso JOIN: siamo un nuovo nodo
                System.out.println("[Node " + nodeId + "] JOIN: received NodeListResponse");
                joining = true;
                ActorRef successor = ringManager.getClockwiseSuccessor(nodeId);
                if (successor != null) {
                    delayedTell(successor, new TransferKeysRequest(nodeId));
                } else {
                    // Nessun successore? Annuncia comunque il join (anello vuoto/singolo)
                    broadcastJoinAndDrop();
                    joining = false;
                }
            })

            // Donatore (successore) seleziona item per cui il nuovo nodo diventa responsabile (tipicamente primario)
            .match(TransferKeysRequest.class, msg -> {
                // NON chiamare addNode qui: la vista ring si aggiorna con JoinNotification
                Map<Integer, ValueResponse> toTransfer = new HashMap<>();
                ActorRef targetNode = refOf(msg.newNodeId);

                for (Map.Entry<Integer, ValueResponse> entry : new ArrayList<>(localStorage.entrySet())) {
                    int key = entry.getKey();
                    List<ActorRef> newResponsible = ringManager.getResponsibleNodes(key);
                    ActorRef newPrimary = newResponsible.isEmpty() ? null : newResponsible.get(0);
                    if (targetNode != null && targetNode.equals(newPrimary)) {
                        toTransfer.put(key, entry.getValue());
                        // RIMOZIONE differita: toglieremo dopo aver notificato
                    }
                }

                delayedTell(getSender(), new TransferKeysResponse(toTransfer));

                // Ora rimuovi localmente gli item trasferiti (semantica JOIN)
                for (Integer k : toTransfer.keySet()) {
                    localStorage.remove(k);
                    System.out.println("[Node " + nodeId + "] Transferred & removed key=" + k + " to node " + msg.newNodeId);
                }
            })

            // Nuovo nodo riceve i dati, fa read-back, poi annuncia il join a tutti
            .match(TransferKeysResponse.class, msg -> {
                if (!msg.data.isEmpty()) {
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
                    // Read-back minimo
                    for (Integer k : msg.data.keySet()) {
                        getSelf().tell(new GetRequest(k), getSelf());
                    }
                }
                // Annuncio JOIN e drop sugli altri
                if (joining) {
                    broadcastJoinAndDrop();
                    joining = false;
                }
            })

            // Aggiornamento globale dopo join
            .match(JoinNotification.class, jn -> {
                ringManager.addNode(jn.nodeId, jn.ref);
                dropKeysNotResponsibleAnymore();
            })

            // =========================================
            // RECOVERY (fetch non-distruttivo + read-back)
            // =========================================
            .match(RecoverRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling RECOVERY");
                recovering = true;

                ActorRef bootstrap = (msg.bootstrap != null && !msg.bootstrap.equals(ActorRef.noSender()))
                        ? msg.bootstrap
                        : pickBootstrapFromRing();

                if (bootstrap == null) {
                    System.out.println("[Node " + nodeId + "] RECOVERY aborted: no bootstrap available.");
                    recovering = false;
                    return;
                }

                recoveryBootstrap = bootstrap;
                delayedTell(recoveryBootstrap, new NodeListRequest(nodeId, getSelf()));
            })

            .match(RecoveryFetchRequest.class, msg -> {
                // Lato donatore: rispondi con gli item di cui il richiedente è PRIMARIO nell'attuale ring
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

            .match(RecoveryFetchResponse.class, msg -> {
                // Lato recovering: salva e poi read-back
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

            // === rispondi a NodeListRequest (usata in RECOVERY)
            .match(NodeListRequest.class, msg -> {
                if (msg.requesterId != null && msg.requester != null) {
                    ringManager.addNode(msg.requesterId, msg.requester);
                }
                delayedTell(getSender(), new NodeListResponse(ringManager.getNodeMap()));
            })

            // =========================================
            // LEAVE — trasferisci verso i NUOVI responsabili (ring senza self)
            // =========================================
            .match(LeaveRequest.class, msg -> {
                System.out.println("[Node " + nodeId + "] Handling LEAVE");

                // 1) Snapshot dei dati locali
                Map<Integer, ValueResponse> snapshot = new HashMap<>(localStorage);

                // 2) Rimuovi te stesso dalla vista LOCALE (per calcolare i nuovi responsabili corretti)
                ringManager.removeNode(nodeId);

                // 3) Per ogni key, invia a tutti i nuovi responsabili (N repliche)
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

                // 4) Notifica gli altri che stai lasciando, così aggiornano la vista e droppano
                for (ActorRef peer : ringManager.getNodeMap().values()) {
                    if (!peer.equals(getSelf())) {
                        delayedTell(peer, new LeaveNotification(nodeId));
                    }
                }

                // 5) Svuota locale e termina
                localStorage.clear();
                getSender().tell(new LeaveAck(nodeId), getSelf());
                getContext().stop(getSelf());
            })

            .match(LeaveNotification.class, msg -> {
                ringManager.removeNode(msg.nodeId);
                dropKeysNotResponsibleAnymore();
                System.out.println("[Node " + nodeId + "] Removed node " + msg.nodeId + " from ring.");
            })

            // =========================================
            // DEBUG
            // =========================================
            .matchEquals("print_storage", msg -> {
                System.out.println("[Node " + nodeId + "] keys in localStorage: " + localStorage.keySet());
            })

            .build();
    }

    // Annuncia JOIN a tutti e forza il drop degli item non più di competenza
    private void broadcastJoinAndDrop() {
        for (Map.Entry<Integer, ActorRef> e : ringManager.getNodeMap().entrySet()) {
            ActorRef peer = e.getValue();
            delayedTell(peer, new JoinNotification(nodeId, getSelf()));
        }
        dropKeysNotResponsibleAnymore();
    }
}
