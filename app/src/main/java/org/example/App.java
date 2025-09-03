package org.example;

import akka.actor.*;
import org.example.Messages.*;

public class App {

    public static void main(String[] args) throws Exception {
        final ActorSystem system = ActorSystem.create("ds1");
        final RingManager ring = new RingManager(Config.NODES);

        try {
            // ======== NODI INIZIALI (3) ========
            final ActorRef node10 = system.actorOf(Props.create(NodeActor.class, 10, ring), "node10");
            final ActorRef node20 = system.actorOf(Props.create(NodeActor.class, 20, ring), "node20");
            final ActorRef node30 = system.actorOf(Props.create(NodeActor.class, 30, ring), "node30");

            ring.addNode(10, node10);
            ring.addNode(20, node20);
            ring.addNode(30, node30);

            // ======== CLIENT ========
            final ActorRef clientA = system.actorOf(Props.create(TestClient.class), "clientA");
            final ActorRef clientB = system.actorOf(Props.create(TestClient.class), "clientB"); // per concorrenza

            printSection("SETUP INIZIALE");
            System.out.println("[*] Ring attivo con nodi: 10,20,30");
            Thread.sleep(600);

            // ======== GUARD PARAMETRI ========
            if (Config.W > Config.N || Config.R > Config.N) {
                throw new IllegalArgumentException("Parametri non validi: W,R devono essere <= N");
            }
            if (ring.getNodeMap().size() < Config.N) {
                System.out.println("[WARN] N=" + Config.N + " > nodi attivi=" + ring.getNodeMap().size()
                        + " → possibili fallimenti di quorum");
            }

            // ======== TEST 1 — WRITE + READ base ========
            printSection("TEST 1 — WRITE + READ base");
            clientA.tell("INFO: UPDATE key=42 -> 'Temp: -60C'", ActorRef.noSender());
            // invio a un nodo qualsiasi: il coordinatore verrà scelto/forwardato
            node10.tell(new UpdateRequest(42, "Temp: -60C"), clientA);
            Thread.sleep(1200);

            clientA.tell("INFO: GET key=42", ActorRef.noSender());
            node30.tell(new GetRequest(42), clientA);
            Thread.sleep(1000);

            // ======== TEST 2 — WRITE concorrenti (stessa key) ========
            printSection("TEST 2 — WRITE concorrenti (stessa key)");
            final int kCW = 99;
            ActorRef primaryCW = ring.getResponsibleNodes(kCW).get(0); // inviamo entrambe al primary per forzare serializzazione
            clientA.tell("INFO: Due UPDATE concorrenti su key=" + kCW, ActorRef.noSender());
            primaryCW.tell(new UpdateRequest(kCW, "Pressure: 720Pa"), clientA);
            // quasi in contemporanea
            primaryCW.tell(new UpdateRequest(kCW, "Pressure: 715Pa"), clientB);
            Thread.sleep(2000);

            clientA.tell("INFO: GET key=" + kCW + " (deve vedere l'ultima versione)", ActorRef.noSender());
            primaryCW.tell(new GetRequest(kCW), clientA);
            Thread.sleep(1000);

            // ======== TEST 3 — WRITE + READ concorrenti (Sequential Consistency) ========
            printSection("TEST 3 — WRITE + READ concorrenti (Sequential Consistency)");
            final int kSC = 123;
            ActorRef primarySC = ring.getResponsibleNodes(kSC).get(0);

            clientA.tell("INFO: UPDATE key=" + kSC + " -> 'VAL: A'", ActorRef.noSender());
            primarySC.tell(new UpdateRequest(kSC, "VAL: A"), clientA);
            Thread.sleep(1000);

            clientA.tell("INFO: Avvio WRITE key=" + kSC + " -> 'VAL: B' e SUBITO una GET sulla stessa key al primary", ActorRef.noSender());
            primarySC.tell(new UpdateRequest(kSC, "VAL: B"), clientA);
            // GET subito sullo stesso primary (dove c'è la coda write) → deve essere rifiutata
            primarySC.tell(new GetRequest(kSC), clientA);
            Thread.sleep(1500);

            clientA.tell("INFO: GET key=" + kSC + " dopo il commit (deve leggere VAL: B)", ActorRef.noSender());
            node10.tell(new GetRequest(kSC), clientA);
            Thread.sleep(1000);

            // ======== TEST 4 — CRASH di un nodo (node20) ========
            printSection("TEST 4 — CRASH di node20");
            clientA.tell("INFO: Stopping node20 (crash simulato)", ActorRef.noSender());
            system.stop(node20); // crash: NON rimuovere dal ring
            Thread.sleep(800);

            // ======== TEST 5 — WRITE con 1 replica down (W=2) ========
            printSection("TEST 5 — WRITE con 1 replica down (W=2)");
            clientA.tell("INFO: UPDATE key=42 -> 'Temp: -59C' (node20 giù)", ActorRef.noSender());
            node30.tell(new UpdateRequest(42, "Temp: -59C"), clientA);
            Thread.sleep(1500);

            // ======== TEST 6 — RECOVERY del nodo crashed ========
            printSection("TEST 6 — RECOVERY di node20");
            clientA.tell("INFO: Ricreo node20 e invio RecoverRequest(20, bootstrap=node10)", ActorRef.noSender());
            final ActorRef node20r = system.actorOf(Props.create(NodeActor.class, 20, ring), "node20_recovered");
            node20r.tell(new RecoverRequest(20, node10), clientA);
            Thread.sleep(2000);

            clientA.tell("INFO: GET key=42 dopo recovery (tutte le repliche devono convergere alla v più alta)", ActorRef.noSender());
            node10.tell(new GetRequest(42), clientA);
            Thread.sleep(1000);

            // ======== TEST 7 — JOIN di un nuovo nodo (node40) ========
            printSection("TEST 7 — JOIN di node40");
            final ActorRef node40 = system.actorOf(Props.create(NodeActor.class, 40, ring), "node40");
            // chiediamo al bootstrap (node10) di servire la join: invia NodeListResponse a node40,
            // poi node40 farà TransferKeysRequest al suo successore → trasferimento selettivo
            node10.tell(new JoinRequest(40, node40), clientA);
            Thread.sleep(2000);

            // verifica con GET su alcune key (compresa 42)
            clientA.tell("INFO: GET key=42 dopo JOIN (deve restare coerente)", ActorRef.noSender());
            node40.tell(new GetRequest(42), clientA);
            Thread.sleep(1000);

            // ======== TEST 8 — LEAVE di un nodo (node30) ========
            printSection("TEST 8 — LEAVE di node30");
            node30.tell(new LeaveRequest(30), clientA); // trasferisce ai nuovi responsabili, notifica e si ferma
            Thread.sleep(2000);

            clientA.tell("INFO: GET key=42 dopo LEAVE (deve restare coerente)", ActorRef.noSender());
            node10.tell(new GetRequest(42), clientA);
            Thread.sleep(1000);

            // ======== TEST 9 — NO QUORUM (opzionale, mostra timeout su GET/UPDATE) ========
            printSection("TEST 9 — NO QUORUM (dimostrazione timeout)");
            clientA.tell("INFO: Spengo node10 e node40 → resta solo node20_recovered. Con N=3,R=2: GET deve fallire.", ActorRef.noSender());
            system.stop(node10);
            system.stop(node40);
            Thread.sleep(600);

            clientA.tell("INFO: GET key=42 con 2 nodi down (atteso: 'GET failed: quorum not reached')", ActorRef.noSender());
            node20r.tell(new GetRequest(42), clientA);
            Thread.sleep(Config.T + 400); // attendiamo almeno il timeout

            // (Facoltativo) prova UPDATE → atteso "Update failed: quorum not reached"
            clientA.tell("INFO: UPDATE key=77 con 2 nodi down (atteso: fallimento quorum)", ActorRef.noSender());
            node20r.tell(new UpdateRequest(77, "SoloUnaReplica"), clientA);
            Thread.sleep(Config.T + 400);

            // ======== FINE ========
            printSection("FINE TEST");
            // Non tutti i nodi sono vivi; terminiamo il sistema
        } finally {
            Thread.sleep(1000);
            system.terminate();
        }
    }

    // ================== UTIL ==================
    private static void printSection(String title) {
        System.out.println();
        System.out.println("================= " + title + " =================");
    }

    // Client minimale che stampa qualunque risposta/stringa
    public static class TestClient extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                .match(String.class, msg -> System.out.println("[Client] " + msg))
                .matchAny(msg -> System.out.println("[Client] Received response: " + msg))
                .build();
        }
    }
}
