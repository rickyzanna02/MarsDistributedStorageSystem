package org.example;

import akka.actor.*;
import org.example.Messages.*;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public class App {
    private static RingManager ring;
    private static final DateTimeFormatter fmt = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("MarsSystem");
        ring = new RingManager(Config.NODES);

        ActorRef client1 = system.actorOf(Props.create(ClientActor.class), "client1");
        ActorRef client2 = system.actorOf(Props.create(ClientActor.class), "client2");

        // === BOOTSTRAP: 3 nodi (10,20,30) ===
        ActorRef n10 = system.actorOf(makeProps(10), "node10");
        ActorRef n20 = system.actorOf(makeProps(20), "node20");
        ActorRef n30 = system.actorOf(makeProps(30), "node30");
        ring.addNode(10, n10);
        ring.addNode(20, n20);
        ring.addNode(30, n30);

        banner("SETUP INIZIALE");
        log("Ring attivo con nodi: 10,20,30");
        sleep(400);

        // ============================================================
        // TEST 1 — Sequential Consistency: GET durante WRITE stessa key
        // ============================================================
        banner("TEST 1 — GET durante WRITE (Sequential Consistency)");

        log("Seed: UPDATE key=42 -> v='Temp: -60C' (client1)");
        n10.tell(new UpdateRequest(42, "Temp: -60C"), client1);
        sleep(600);

        log("Inizio WRITE concorrente: key=42 -> v='Temp: -59C' (client2)");
        n20.tell(new UpdateRequest(42, "Temp: -59C"), client2);

        // Subito due GET concorrenti sulla stessa key
        log("Subito dopo la WRITE: due GET concorrenti su key=42 (client1 & client2)");
        n30.tell(new GetRequest(42), client1);
        n10.tell(new GetRequest(42), client2);

        // Se il codice blocca o rifiuta queste GET, lo vediamo dai log/tempi
        sleep(1500);
        printStorage(system, "DUMP dopo TEST 1");

        // ============================================================
        // TEST 2 — JOIN: broadcast + read-back a versione più recente
        // Chiave "di frontiera": 23 (prima del join apparteneva al vecchio set)
        // ============================================================
        banner("TEST 2 — JOIN (broadcast + read-back)");

        log("Seed: doppio UPDATE rapido su key=23 -> 'A' poi 'B' (per avere versione alta prima del join)");
        n10.tell(new UpdateRequest(23, "A"), client1);
        sleep(200);
        n20.tell(new UpdateRequest(23, "B"), client2);
        sleep(800);

        log("JOIN di nodo 25");
        ActorRef n25 = system.actorOf(makeProps(25), "node25");
        n10.tell(new JoinRequest(25, n25), client1);

        // Subito GET sulla key=23: il nuovo responsabile dovrebbe già essere coerente e con versione più recente
        sleep(1200);
        log("GET key=23 da client1 e client2 (dopo join)");
        n25.tell(new GetRequest(23), client1);
        n20.tell(new GetRequest(23), client2);

        sleep(1200);
        printStorage(system, "DUMP dopo TEST 2 (post-JOIN)");

        // ============================================================
        // TEST 3 — LEAVE: i dati vanno ai veri nuovi responsabili (N repliche), non solo al successore
        // Usiamo una chiave che coinvolgeva 30 tra i responsabili: 55
        // ============================================================
        banner("TEST 3 — LEAVE verso veri nuovi responsabili (non solo successore)");

        log("Seed: UPDATE key=55 -> 'CO2: 12%'");
        n10.tell(new UpdateRequest(55, "CO2: 12%"), client1);
        sleep(800);

        log("LEAVE di nodo 30");
        n30.tell(new LeaveRequest(30), client2);

        // Subito GET da due nodi diversi per vedere dove è andata a finire la chiave
        sleep(1200);
        log("GET key=55 da client1 & client2 (post-LEAVE)");
        n10.tell(new GetRequest(55), client1);
        n25.tell(new GetRequest(55), client2);

        sleep(1200);
        printStorage(system, "DUMP dopo TEST 3 (post-LEAVE)");

        // ============================================================
        // TEST 4 — CRASH + RECOVERY: aggiornamenti durante lo stop e riallineamento alla versione più recente
        // ============================================================
        banner("TEST 4 — CRASH + RECOVERY con aggiornamenti durante il down");

        log("CRASH simulato di nodo 20 (stop actor)");
        system.stop(n20);
        sleep(600);

        log("Durante il crash: UPDATE key=42 -> 'Temp: -58C' (avanza versione)");
        n10.tell(new UpdateRequest(42, "Temp: -58C"), client1);
        sleep(1000);

        log("RECOVERY di nodo 20");
        ActorRef recovered20 = system.actorOf(makeProps(20), "node20_recovered");
        ring.addNode(20, recovered20);
        recovered20.tell(new RecoverRequest(20), client2);

        // Dopo recover, leggiamo la 42 da più nodi e poi dump storage
        sleep(1500);
        log("GET key=42 da client1 (n25) e client2 (20_recovered) dopo recover");
        n25.tell(new GetRequest(42), client1);
        recovered20.tell(new GetRequest(42), client2);

        sleep(1500);
        printStorage(system, "DUMP finale (post-RECOVERY)");

        banner("FINE TEST — Controlla nei log:");
        log("1) Se le GET su key=42 durante WRITE sono state bloccate/ritardate o hanno restituito un valore vecchio.");
        log("2) Dopo JOIN(25): key=23 deve avere la versione più recente sul nuovo set di responsabili.");
        log("3) Dopo LEAVE(30): key=55 deve essere trasferita ai veri nuovi responsabili (tutte N repliche).");
        log("4) Dopo RECOVERY(20): key=42 sul nodo recuperato deve avere la versione più recente.");

        sleep(800);
        system.terminate();
    }

    // ===== Helpers =====
    public static Props makeProps(int id) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, ring));
    }

    private static void printStorage(ActorSystem system, String title) {
        System.out.println("\n==== " + ts() + " " + title + " ====");
        system.actorSelection("/user/*").tell("print_storage", ActorRef.noSender());
        sleep(700);
        System.out.println("==== END " + title + " ====\n");
    }

    private static void banner(String title) {
        System.out.println("\n================= " + title + " =================");
    }

    private static String ts() {
        return "[" + LocalTime.now().format(fmt) + "]";
    }

    private static void log(String s) {
        System.out.println(ts() + " " + s);
    }

    private static void sleep(int ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { e.printStackTrace(); }
    }
}
