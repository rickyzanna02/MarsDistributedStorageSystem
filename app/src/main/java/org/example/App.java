package org.example;

import akka.actor.*;
import org.example.Messages.*;

public class App {
    private static RingManager ring;

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("MarsSystem");
        ring = new RingManager(3); // N = 3

        ActorRef client1 = system.actorOf(Props.create(ClientActor.class), "client1");
        ActorRef client2 = system.actorOf(Props.create(ClientActor.class), "client2");

        ActorRef n10 = system.actorOf(makeProps(10), "node10");
        ActorRef n20 = system.actorOf(makeProps(20), "node20");
        ActorRef n30 = system.actorOf(makeProps(30), "node30");

        ring.addNode(10, n10);
        ring.addNode(20, n20);
        ring.addNode(30, n30);

        System.out.println("[TEST] UPDATE iniziale su key=42 da client1");
        n10.tell(new UpdateRequest(42, "Temp: -60C"), client1);
        sleep(1000);

        System.out.println("[TEST] GET su key=42 da client2");
        n20.tell(new GetRequest(42), client2);
        sleep(1000);

        System.out.println("[TEST] JOIN di node25");
        ActorRef n25 = system.actorOf(makeProps(25), "node25");
        ring.addNode(25, n25);
        n10.tell(new JoinRequest(25, n25), client1);
        sleep(1000);

        System.out.println("[TEST] UPDATE su key=42 da client2 (valore aggiornato)");
        n25.tell(new UpdateRequest(42, "Temp: -59C"), client2);
        sleep(1000);

        System.out.println("[TEST] LEAVE di node30");
        n30.tell(new LeaveRequest(30), client1);
        sleep(1500);

        System.out.println("[TEST] Simulazione crash di node20");
        ring.removeNode(20);
        sleep(500);

        System.out.println("[TEST] UPDATE su key=55 da client1 (quorum fallito)");
        n10.tell(new UpdateRequest(55, "CO2: 12%"), client1);
        sleep(1500);

        System.out.println("[TEST] RECOVERY di node20");
        ActorRef recovered20 = system.actorOf(makeProps(20), "node20_recovered");
        ring.addNode(20, recovered20);
        recovered20.tell(new RecoverRequest(20), client2);
        sleep(1500);

        System.out.println("[TEST] GET concorrente su key=42 da entrambi i client");
        n25.tell(new GetRequest(42), client1);
        recovered20.tell(new GetRequest(42), client2);
        sleep(1500);

        System.out.println("[TEST] Stato finale dello storage di tutti i nodi:");
        printStorage(system);
        sleep(1000);

        system.terminate();
    }

    public static Props makeProps(int id) {
        return Props.create(NodeActor.class, () -> new NodeActor(id, ring));
    }

    private static void printStorage(ActorSystem system) {
        system.actorSelection("/user/*").tell("print_storage", ActorRef.noSender());
        sleep(500);
    }

    private static void sleep(int ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
