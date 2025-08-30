package org.example;

import akka.actor.AbstractActor;
//import org.example.Messages.*;

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
