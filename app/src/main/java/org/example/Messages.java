//MESSAGES.JAVA: defines all the message types exchanged between actors in the distributed systems.
//Each class represents a request, response, or system event for coordination, replication, and membership management.

package org.example;
import java.io.Serializable;
import java.util.Map;
import akka.actor.ActorRef;

public class Messages {
    
    // =================================================
    // 1) CLIENT API
    // =================================================

    // UpdateRequest: Write request from a client (key, value)
    public static class UpdateRequest implements Serializable {
        public final int key;
        public final String value;
        public UpdateRequest(int key, String value) { this.key = key; this.value = value; }
    }

    // GetRequest: Read request from a client (key)
    public static class GetRequest implements Serializable {
        public final int key;
        public GetRequest(int key) { this.key = key; }
    }


    // =================================================
    // 2) INTERNAL REPLICATION & QUORUM
    // =================================================

    // UpdateInternal: Internal write sent to all replicas with the new version
    public static class UpdateInternal implements Serializable {
        public final int key;
        public final String value;
        public final int version;
        public UpdateInternal(int key, String value, int version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    // InternalGet: Internal read request sent to replicas (R quorum)
    public static class InternalGet implements Serializable {
        public final int key;
        public InternalGet(int key) { this.key = key; }
    }

    // ValueResponse: Response with the value and version
    public static class ValueResponse implements Serializable {
        public final int key;
        public final String value;
        public final int version;
        public ValueResponse(int key, String value, int version) {
            this.key = key; this.value = value; this.version = version;
        }
    }

    // VersionRequest: Request the current version number for a replica (used in W quorum)
    public static class VersionRequest implements Serializable {
        public final int key;
        public VersionRequest(int key) { this.key = key; }
    }

    // VersionResponse: Reply with the current version number for a key
    public static class VersionResponse implements Serializable {
        public final int key;
        public final int version;
        public VersionResponse(int key, int version) { this.key = key; this.version = version; }
    }

    // PendingResponse: Marker to indicate pending response to client (to avoid read/write overlap)
    public static final class PendingResponse implements Serializable {
        public final int key;
        public PendingResponse(int key) { this.key = key; }
        @Override public String toString() { return "PendingResponse{key=" + key + "}"; }
    }

    // =================================================
    // 3) MEMBERSHIP MANAGEMENT: JOIN, LEAVE, RECOVERY
    // =================================================

    // JoinRequest: Initial request to join the ring (sent to bootstrap)
    public static class JoinRequest implements Serializable {
        public final int newId;
        public final ActorRef newNode;
        public JoinRequest(int newId, ActorRef newNode) { this.newId = newId; this.newNode = newNode; }
    }

    // JoinNotification: Notification to all nodes about a new member joining
    public static class JoinNotification implements java.io.Serializable {
        public final int nodeId;
        public final akka.actor.ActorRef ref;
        public JoinNotification(int nodeId, akka.actor.ActorRef ref) {
            this.nodeId = nodeId; this.ref = ref;
        }
    }

    // NodeListRequest: Request the current list of nodes in the ring
    public static class NodeListRequest implements Serializable {
        public final Integer requesterId;
        public final ActorRef requester;
        public NodeListRequest() { this.requesterId = null; this.requester = null; }
        public NodeListRequest(int requesterId, ActorRef requester) {
            this.requesterId = requesterId; this.requester = requester;
        }
    }

    // NodeListResponse: Response with the current list of nodes in the ring
    public static class NodeListResponse implements Serializable {
        public final Map<Integer, ActorRef> nodes;
        public NodeListResponse(Map<Integer, ActorRef> nodes) { this.nodes = nodes; }
    } 

    // LeaveRequest: Request to leave the ring
    public static class LeaveRequest implements Serializable {
        public final int nodeId;
        public LeaveRequest(int nodeId) { this.nodeId = nodeId; }
    }

    // LeaveNotification: Notification to all nodes about a member leaving
    public static class LeaveNotification implements Serializable {
        public final int nodeId;
        public LeaveNotification(int nodeId) { this.nodeId = nodeId; }
    }

    // RecoverRequest: Recover request after a crash
    public static class RecoverRequest implements Serializable {
        public final int nodeId;
        public final ActorRef bootstrap; // a chi chiedere la NodeList
        public RecoverRequest(int nodeId, ActorRef bootstrap) { this.nodeId = nodeId; this.bootstrap = bootstrap; }
        public RecoverRequest(int nodeId) {
            this.nodeId = nodeId;
            this.bootstrap = null;
        }
    }

    // RecoveryFetchRequest: Fetch data from other replicas during recovery
    public static class RecoveryFetchRequest implements Serializable {
        public final int requesterId;
        public RecoveryFetchRequest(int requesterId) { this.requesterId = requesterId; }
    }

    // RecoveryFetchResponse: Response with data during recovery
    public static class RecoveryFetchResponse implements Serializable {
        public final Map<Integer, ValueResponse> data;
        public RecoveryFetchResponse(Map<Integer, ValueResponse> data) { this.data = data; }
    }

    
    // =================================================
    // 4) DATA TRANSFER FOR JOIN/LEAVE/RECOVERY
    // =================================================

    // TransferKeysRequest: Request to transfer keys to a new node 
    public static class TransferKeysRequest implements Serializable {
        public final int newNodeId;
        public TransferKeysRequest(int newNodeId) { this.newNodeId = newNodeId; }
    }

    // TransferKeysResponse: Response with the data to transfer to the new node
    public static class TransferKeysResponse implements Serializable {
        public final Map<Integer, ValueResponse> data;
        public TransferKeysResponse(Map<Integer, ValueResponse> data) { this.data = data; }
    }
      
}
