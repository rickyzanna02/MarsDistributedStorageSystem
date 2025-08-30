package org.example;

import java.io.Serializable;
import java.util.Map;

import akka.actor.ActorRef;

public class Messages {

    public static class UpdateRequest implements Serializable {
        public final int key;
        public final String value;

        public UpdateRequest(int key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class GetRequest implements Serializable {
        public final int key;

        public GetRequest(int key) {
            this.key = key;
        }
    }

    public static class UpdateInternal implements Serializable {
        public final int key;
        public final String value;
        public final int version;

        public UpdateInternal(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }

    public static class GetInternal implements Serializable {
        public final int key;

        public GetInternal(int key) {
            this.key = key;
        }
    }

    public static class ValueResponse implements Serializable {
        public final int key;
        public final String value;
        public final int version;

        public ValueResponse(int key, String value, int version) {
            this.key = key;
            this.value = value;
            this.version = version;
        }
    }

    public static class VersionRequest implements Serializable {
        public final int key;

        public VersionRequest(int key) {
            this.key = key;
        }
    }

    public static class VersionResponse implements Serializable {
        public final int key;
        public final int version;

        public VersionResponse(int key, int version) {
            this.key = key;
            this.version = version;
        }
    }

    public static class InternalGet implements Serializable {
        public final int key;

        public InternalGet(int key) {
            this.key = key;
        }
    }


    public static class JoinRequest implements Serializable {
        public final int newId;
        public final ActorRef newNode;

        public JoinRequest(int newId, ActorRef newNode) {
            this.newId = newId;
            this.newNode = newNode;
        }
    }

    public static class NodeListResponse implements Serializable {
        public final Map<Integer, ActorRef> nodes;

        public NodeListResponse(Map<Integer, ActorRef> nodes) {
            this.nodes = nodes;
        }
    }

    public static class TransferKeysRequest implements Serializable {
        public final int newNodeId;

        public TransferKeysRequest(int newNodeId) {
            this.newNodeId = newNodeId;
        }
    }

    public static class TransferKeysResponse implements Serializable {
        public final Map<Integer, ValueResponse> data;

        public TransferKeysResponse(Map<Integer, ValueResponse> data) {
            this.data = data;
        }
    }

    ////////////////
    
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

        public LeaveNotification(int nodeId) {
            this.nodeId = nodeId;
        }
    }

    ////////////timeout
    
    public static class GetTimeout implements Serializable {
        public final int key;
        public GetTimeout(int key) { this.key = key; }
    }

    public static class UpdateTimeout implements Serializable {
        public final int key;
        public UpdateTimeout(int key) { this.key = key; }
    }

    //////////////fine timeout
    /// 
    /// recovery
    
    public static class RecoverRequest {
        public final int nodeId;

        public RecoverRequest(int nodeId) {
           this.nodeId = nodeId;
        }
    }


    public static class NodeListRequest implements Serializable {
        public final int requesterId;
        public final ActorRef requester;

        public NodeListRequest(int requesterId, ActorRef requester) {
            this.requesterId = requesterId;
            this.requester = requester;
        }
    }





    //fine recovery


}
