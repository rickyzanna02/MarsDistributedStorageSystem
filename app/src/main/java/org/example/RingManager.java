package org.example;

import akka.actor.ActorRef;

import java.util.*;

public class RingManager {
    private final TreeMap<Integer, ActorRef> ring = new TreeMap<>();
    private final int replicationFactor;

    public RingManager(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void addNode(int id, ActorRef ref) {
        ring.put(id, ref);
    }

    public void removeNode(int id) {
        ring.remove(id);
    }

    public Map<Integer, ActorRef> getNodeMap() {
        return new HashMap<>(ring);
    }

    public List<ActorRef> getResponsibleNodes(int key) {
        List<ActorRef> responsible = new ArrayList<>();
        if (ring.isEmpty()) return responsible;

        SortedMap<Integer, ActorRef> tail = ring.tailMap(key);
        Iterator<Map.Entry<Integer, ActorRef>> it = tail.entrySet().iterator();
        Set<ActorRef> unique = new LinkedHashSet<>();

        while (unique.size() < replicationFactor && it.hasNext()) {
            unique.add(it.next().getValue());
        }

        if (unique.size() < replicationFactor) {
            it = ring.entrySet().iterator();
            while (unique.size() < replicationFactor && it.hasNext()) {
                unique.add(it.next().getValue());
            }
        }

        return new ArrayList<>(unique);
    }

    public ActorRef getClockwiseSuccessor(int nodeId) {
        SortedMap<Integer, ActorRef> tail = ring.tailMap(nodeId + 1);
        if (!tail.isEmpty()) {
            return tail.get(tail.firstKey());
        } else {
            return ring.get(ring.firstKey());
        }
    }
}
