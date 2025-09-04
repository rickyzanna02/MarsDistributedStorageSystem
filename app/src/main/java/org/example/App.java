package org.example;

import akka.actor.*;
import org.example.Messages.*;

public class App {

    public static void main(String[] args) throws Exception {
        final ActorSystem system = ActorSystem.create("ds1");
        final RingManager ring = new RingManager(Config.N); // usa N (replication factor)

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
            endTestPause();

            // ============================
            // TEST 1 — WRITE + READ base
            // ============================
            printSection("TEST 1: WRITE+READ base su key=42 (atteso v=1 e valore corrispondente)");
            node10.tell(new UpdateRequest(42, "Temp:-60C"), clientA);
            Thread.sleep(300); // <<--- per mostrare subito la write committata
            node20.tell(new GetRequest(42), clientA);
            endTestPause();

            // ==========================================
            // TEST 2 — Forwarding da non-primario
            // ==========================================
            printSection("TEST 2: Forwarding UPDATE/GET da nodo non-primario (atteso inoltro trasparente)");
            node30.tell(new UpdateRequest(777, "Pressure:720Pa"), clientA);
            shortWait();
            node30.tell(new GetRequest(777), clientA);
            endTestPause();

            // ==================================================
            // TEST 3 — Due UPDATE sequenziali (version bump)
            // ==================================================
            printSection("TEST 3: Due UPDATE sequenziali sulla stessa key (atteso v=1 poi v=2)");
            node10.tell(new UpdateRequest(99, "Val:A"), clientA);
            shortWait();
            node10.tell(new UpdateRequest(99, "Val:B"), clientA);
            shortWait();
            node20.tell(new GetRequest(99), clientA);
            endTestPause();

            // =======================================================
            // TEST 4 — WRITE/WRITE concorrenti (serializzazione)
            // =======================================================
            printSection("TEST 4: Due UPDATE concorrenti su key=123 (atteso serializzazione, v=1 quindi v=2)");
            node10.tell(new UpdateRequest(123, "X"), clientA);
            node10.tell(new UpdateRequest(123, "Y"), clientB);
            shortWait(); // diamo tempo alla serializzazione e al commit
            node20.tell(new GetRequest(123), clientA);
            endTestPause();

            // ==================================================================
            // TEST 5 — GET durante WRITE (Sequential Consistency su singola key)
            // ==================================================================
            printSection("TEST 5: GET durante WRITE su key=200 (atteso rifiuto/ritardo GET finché la WRITE non termina)");
            node10.tell(new UpdateRequest(200, "SC-1"), clientA);
            node20.tell(new GetRequest(200), clientB); // subito -> deve rifiutare/attendere
            endTestPause();

            // =====================================================
            // TEST 6 — GET/GET concorrenti (aggregazione risposte)
            // =====================================================
            printSection("TEST 6: Due GET concorrenti su key=42 (atteso stessa risposta per entrambi i client)");
            node10.tell(new GetRequest(42), clientA);
            node10.tell(new GetRequest(42), clientB);
            endTestPause();

            // ===========================================================
            // TEST 7 — Concorrenza su chiavi diverse (indipendenza)
            // ===========================================================
            printSection("TEST 7: UPDATE su key=300 e GET su key=42 in parallelo (atteso indipendenza)");
            node20.tell(new UpdateRequest(300, "Indep"), clientA);
            node30.tell(new GetRequest(42), clientB);
            endTestPause();

            // ======================================
            // TEST 8 — Quorum R/W con N=3,R=2,W=2
            // ======================================
            printSection("TEST 8: Verifica quorum R/W (atteso completamento dopo >=W e >=R risposte)");
            node10.tell(new UpdateRequest(400, "QCheck"), clientA);
            shortWait();
            node10.tell(new GetRequest(400), clientA);
            endTestPause();

            // ============================================
            // TEST 9 — JOIN di un nuovo nodo (id=25)
            // ============================================
            printSection("TEST 9: JOIN nodo 25 (atteso transfer dal successore e annuncio join)");
            final ActorRef node25 = system.actorOf(Props.create(NodeActor.class, 25, ring), "node25");
            node10.tell(new JoinRequest(25, node25), clientA); // node10 come bootstrap
            endTestPause(); // lascia propagare NodeList/Transfer
            node25.tell(new UpdateRequest(401, "AfterJoin"), clientA);
            shortWait();
            node10.tell(new GetRequest(401), clientA);
            endTestPause();

            // ============================================
            // TEST 10 — LEAVE di un nodo non-primario
            // ============================================
            printSection("TEST 10: LEAVE nodo 30 (atteso transfer agli attuali responsabili e stop nodo)");
            node30.tell(new LeaveRequest(30), clientA);
            endTestPause(); // lascia completare transfer + stop
            node10.tell(new GetRequest(42), clientA);
            endTestPause();

            // ======================================================
            // TEST 11 — CRASH di una replica (stop senza leave)
            // ======================================================
            printSection("TEST 11: CRASH nodo 20 (atteso che quorum regga con N=3,R=2,W=2)");
            system.stop(node20); // crash "hard"
            node10.tell(new UpdateRequest(500, "WithOneDown"), clientA);
            shortWait();
            node10.tell(new GetRequest(500), clientA);
            endTestPause();

            // =======================================================
            // TEST 12 — RECOVERY del nodo crashato (node20)
            // =======================================================
            printSection("TEST 12: RECOVERY nodo 20 (atteso fetch/read-back e riallineamento)");
            final ActorRef node20v2 = system.actorOf(Props.create(NodeActor.class, 20, ring), "node20v2");
            node20v2.tell(new RecoverRequest(20, node10), clientA);
            endTestPause(); // lascia finire NodeList/Fetch/Read-back
            node20v2.tell(new GetRequest(500), clientA);
            endTestPause();

            // ====================================================
            // TEST 13 — Read-repair (replica stantia → riparata)
            // ====================================================
            printSection("TEST 13: Read-repair su key=401 (atteso che la versione massima ripari eventuali repliche)");
            node10.tell(new GetRequest(401), clientA);
            endTestPause();

            // ==========================================================
            // TEST 14 — Key inesistente (messaggio not-found chiaro)
            // ==========================================================
            printSection("TEST 14: GET su key inesistente (atteso 'not found')");
            node10.tell(new GetRequest(1597), clientA);
            endTestPause();

            // ==========================================================
            // TEST 15 — Leave del primario di qualche chiave (node10)
            // ==========================================================
            printSection("TEST 15: LEAVE nodo 10 (atteso transfer completo e continuità di servizio)");
            node10.tell(new LeaveRequest(10), clientA);
            endTestPause(); // lascia completare il transfer
            node25.tell(new GetRequest(42), clientA);
            endTestPause();

            // ======================
            // FINE
            // ======================
            printSection("FINE TEST");
            endTestPause();

        } finally {
            system.terminate();
        }
    }

    // =============== Helpers ===============

    private static void printSection(String title) {
        System.out.println();
        System.out.println("===================================================");
        System.out.println(title);
        System.out.println("===================================================");
    }

    // Pausa breve usata SOLO per rendere “visibile” il commit prima della GET
    private static void shortWait() throws InterruptedException {
        // circa metà del timeout: basta a far “comparire” il valore nella demo
        Thread.sleep(Math.max(200, Config.T / 2));
    }

    // Pausa a fine test per separare i log e lasciar chiudere i quorum/pending
    private static void endTestPause() throws InterruptedException {
        Thread.sleep(Math.max(600, Config.T + Config.MAX_DELAY_MS));
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
