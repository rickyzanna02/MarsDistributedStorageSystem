package org.example;

import java.io.Serializable;
import java.util.Map;
import akka.actor.ActorRef;

public class Messages {

    // ========= CLIENT OPS =========
    public static class UpdateRequest implements Serializable {
        public final int key;
        public final String value;
        public UpdateRequest(int key, String value) { this.key = key; this.value = value; }
    }

    public static class GetRequest implements Serializable {
        public final int key;
        public GetRequest(int key) { this.key = key; }
    }

    // ========= INTERNAL READ/WRITE =========
    public static class UpdateInternal implements Serializable {
        public final int key;
        public final String value;
        public final int version;
        public UpdateInternal(int key, String value, int version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    public static class InternalGet implements Serializable {
        public final int key;
        public InternalGet(int key) { this.key = key; }
    }

    public static class ValueResponse implements Serializable {
        public final int key;
        public final String value;
        public final int version;
        public ValueResponse(int key, String value, int version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    public static class VersionRequest implements Serializable {
        public final int key;
        public VersionRequest(int key) { this.key = key; }
    }

    public static class VersionResponse implements Serializable {
        public final int key;
        public final int version;
        public VersionResponse(int key, int version) { this.key = key; this.version = version; }
    }

    // ========= JOIN / RING SYNC =========
    public static class JoinRequest implements Serializable {
        public final int newId;
        public final ActorRef newNode;
        public JoinRequest(int newId, ActorRef newNode) { this.newId = newId; this.newNode = newNode; }
    }

    public static class DataItem implements java.io.Serializable {
        public final int key;
        public final String value;
        public final int version;
        public DataItem(int key, String value, int version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    public static class JoinNotification implements java.io.Serializable {
        public final int nodeId;
        public final akka.actor.ActorRef ref;
        public JoinNotification(int nodeId, akka.actor.ActorRef ref) {
            this.nodeId = nodeId; this.ref = ref;
        }
    }

    // NodeListRequest con costruttore vuoto (usato dal RECOVERY) + variante con campi (se ti serve altrove)
    public static class NodeListRequest implements Serializable {
        public final Integer requesterId;   // può essere null
        public final ActorRef requester;    // può essere null
        public NodeListRequest() { this.requesterId = null; this.requester = null; }
        public NodeListRequest(int requesterId, ActorRef requester) {
            this.requesterId = requesterId; this.requester = requester;
        }
    }

    public static class NodeListResponse implements Serializable {
        public final Map<Integer, ActorRef> nodes;
        public NodeListResponse(Map<Integer, ActorRef> nodes) { this.nodes = nodes; }
    }

    public static class TransferKeysRequest implements Serializable {
        public final int newNodeId;
        public TransferKeysRequest(int newNodeId) { this.newNodeId = newNodeId; }
    }

    public static class TransferKeysResponse implements Serializable {
        public final Map<Integer, ValueResponse> data;
        public TransferKeysResponse(Map<Integer, ValueResponse> data) { this.data = data; }
    }

    public static final class PendingResponse implements Serializable {
        public final int key;
        public PendingResponse(int key) { this.key = key; }
        @Override public String toString() { return "PendingResponse{key=" + key + "}"; }
    }

    // ========= LEAVE =========
    public static class LeaveRequest implements Serializable {
        public final int nodeId;
        public LeaveRequest(int nodeId) { this.nodeId = nodeId; }
    }

    public static class LeaveAck implements Serializable {
        public final int nodeId;
        public LeaveAck(int nodeId) { this.nodeId = nodeId; }
    }

    public static class LeaveNotification implements Serializable {
        public final int nodeId;
        public LeaveNotification(int nodeId) { this.nodeId = nodeId; }
    }

    // ========= (OPZ) TIMEOUT MARKERS — se vuoi mantenere i tuoi scheduler dedicati =========
    public static class GetTimeout implements Serializable {
        public final int key;
        public GetTimeout(int key) { this.key = key; }
    }
    public static class UpdateTimeout implements Serializable {
        public final int key;
        public UpdateTimeout(int key) { this.key = key; }
    }

    // ========= RECOVERY (fetch non-distruttivo) =========
    public static class RecoverRequest implements Serializable {
        public final int nodeId;
        public final ActorRef bootstrap; // a chi chiedere la NodeList
        public RecoverRequest(int nodeId, ActorRef bootstrap) { this.nodeId = nodeId; this.bootstrap = bootstrap; }
        public RecoverRequest(int nodeId) {
            this.nodeId = nodeId;
            this.bootstrap = null;
        }

    }

    public static class RecoveryFetchRequest implements Serializable {
        public final int requesterId;
        public RecoveryFetchRequest(int requesterId) { this.requesterId = requesterId; }
    }

    public static class RecoveryFetchResponse implements Serializable {
        public final Map<Integer, ValueResponse> data;
        public RecoveryFetchResponse(Map<Integer, ValueResponse> data) { this.data = data; }
    }

    /** (Facoltativo) Notifica di fine trasferimento al termine di join/recovery per sbloccare step successivi. */
    public static class TransferDone implements java.io.Serializable {
        public final int forNodeId;
        public TransferDone(int forNodeId){ this.forNodeId = forNodeId; }
    }
}
