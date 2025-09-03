package org.example;

import akka.actor.*;
import org.example.Messages.*;

public class App {
    private static RingManager ring;

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("MarsSystem");
        ring = new RingManager(Config.NODES); // numero di nodi (recuperato da config)
///////////////////////////
       

        ActorRef client1 = system.actorOf(Props.create(ClientActor.class), "client1");
        ActorRef client2 = system.actorOf(Props.create(ClientActor.class), "client2");

        // Nodo coordinatore (es: n10)
        ActorRef n10 = system.actorOf(makeProps(10), "node10");
        ActorRef n20 = system.actorOf(makeProps(20), "node20");
        ActorRef n30 = system.actorOf(makeProps(30), "node30");

        ring.addNode(10, n10);
        ring.addNode(20, n20);
        ring.addNode(30, n30);

        sleep(1000); // lascia tempo alla rete

        System.out.println("[TEST] Persistenza: scrivo su key=101 da client1");
        n10.tell(new UpdateRequest(101, "Humidity: 42%"), client1);
        sleep(1000);

        System.out.println("[TEST] LEAVE di node10 (simulazione crash con rimozione)");
        n10.tell(new LeaveRequest(10), client2);
        sleep(1000);

        // Rimuovo manualmente dalla ring, per simulare l’effetto di un crash
        ring.removeNode(10);

        System.out.println("[TEST] RECOVERY di node10 (nuovo attore, stesso ID)");
        ActorRef recovered10 = system.actorOf(makeProps(10), "node10_recovered");
        ring.addNode(10, recovered10);
        recovered10.tell(new RecoverRequest(10), client1);
        sleep(1000);

        System.out.println("[TEST] GET su key=101 da node10_recovered (dovrebbe avere valore precedente)");
        recovered10.tell(new GetRequest(101), client2);
        sleep(1500);

        System.out.println("[TEST] Stato dello storage dopo recovery:");
        printStorage(system);

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
