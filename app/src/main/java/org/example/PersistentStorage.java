package org.example;

import org.example.Messages.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Simula uno storage persistente per ogni nodo.
 * I dati rimangono disponibili anche se il nodo viene "ricreato" dopo un crash.
 */
public class PersistentStorage {
    // Mappa globale: nodeId → storage key → ValueResponse
    private static final Map<Integer, Map<Integer, ValueResponse>> allData = new HashMap<>();

    // Restituisce lo storage associato a un dato nodo.
    public static Map<Integer, ValueResponse> getStorage(int nodeId) {
        return allData.computeIfAbsent(nodeId, k -> new HashMap<>());
    }
}
