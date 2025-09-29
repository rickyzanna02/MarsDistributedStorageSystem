// RINGMANAGER.JAVA: manages the ring structure of nodes in the distributed system.
// Provides operations to add/remove nodes, query the current ring,
// and compute responsible replicas for a given key.

package org.example;
import akka.actor.ActorRef;
import java.util.*;

public class RingManager {
    private final TreeMap<Integer, ActorRef> ring = new TreeMap<>(); // Sorted map of node IDs to ActorRefs
    private final int replicationFactor;                             // Number of replicas per node

    // Constructor
    public RingManager(int replicationFactor) { 
        this.replicationFactor = replicationFactor;
    }

    // Add a node to the ring
    public void addNode(int id, ActorRef ref) {
        ring.put(id, ref);
    }

    // Remove a node from the ring
    public void removeNode(int id) {
        ring.remove(id);
    }

    // Get a copy of the current ring
    public Map<Integer, ActorRef> getNodeMap() {
        return new HashMap<>(ring);
    }

    // Get the list of nodes responsible for a given key
    public List<ActorRef> getResponsibleNodes(int key) {
        List<ActorRef> responsible = new ArrayList<>();
        if (ring.isEmpty()){
            return responsible;
        }
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

    // Get the clockwise successor of a given node ID
    public ActorRef getClockwiseSuccessor(int nodeId) {
        SortedMap<Integer, ActorRef> tail = ring.tailMap(nodeId + 1);
        if (!tail.isEmpty()) {
            return tail.get(tail.firstKey());
        } else {
            return ring.get(ring.firstKey());
        }
    }
}
