// CLIENTACTOR.JAVA: defines a simple client actor used to interact with the distributed system.
// It receives responses from nodes (string) and prints them to the console.

package org.example;
import akka.actor.AbstractActor;

public class ClientActor extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(String.class, msg -> {
                System.out.println("[Client] Received response: " + msg);
            })
            .build();
    }
}
