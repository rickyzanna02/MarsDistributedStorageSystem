// PERSISTENTSTORAGE.JAVA: Simulates persistent storage for each node.
// Data remains available even if the node is "recreated" after a crash.

package org.example;
import org.example.Messages.*;
import java.util.HashMap;
import java.util.Map;

// Global map: nodeId -> (key -> value/version)
// Represents the persistent storage associated with each node.
public class PersistentStorage {
    private static final Map<Integer, Map<Integer, ValueResponse>> allData = new HashMap<>();
    public static Map<Integer, ValueResponse> getStorage(int nodeId) {
        return allData.computeIfAbsent(nodeId, k -> new HashMap<>());
    }
}
