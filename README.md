# Mars Distributed Storage System

This project was developed as part of the Distributed Systems course in the Master’s Degree program in Information Engineering at the University of Trento.

The main goal of the project is to implements a peer-to-peer key–value store for ESA’s distributed network of Martian data stations. Nodes replicate data across a logical ring and use quorum-based reads/writes to ensure availability and robustness under harsh, failure-prone conditions (crash, disconnect, reboot, join/leave). The system enforces sequential consistency, performs selective data transfer on membership changes (join/leave/recovery), and includes a main driver that simulates end-to-end scenarios: data upload and retrieval, node failures and recovery, and basic network management.

---

## Structure of the project
- **Messages.java**: defines all message types exchanged between actors (client ↔ node, node ↔ node, membership management).
- **NodeActor.java**: main actor implementing the behavior of a storage node (GET, UPDATE, JOIN, LEAVE, RECOVERY).
- **RingManager.java**: maintains the logical ring of nodes and computes responsible replicas for each key.
- **PersistentStorage.java**: simulates persistent local storage for each node, surviving crashes and recovery.
- **ClientActor.java**: simple client actor that receives responses from the system and prints them.
- **App.java**: sets up the actor system and runs test scenarios (basic read/write, join/leave, crash/recovery, sequential consistency).

---

## Main design choices
- **Ring-based partitioning**: nodes are arranged on a ring (TreeMap by ID), and each key is mapped to a set of *N* responsible replicas clockwise from the key.
- **Quorum-based replication**: write requires *W* acknowledgments; read requires *R* responses. Parameters N, R, W are configurable.
- **Sequential consistency enforcement**: only one WRITE at a time per key; GET is blocked or retried if overlapping with a WRITE.
- **Minimal data movement**: JOIN, LEAVE, and RECOVERY transfer only the keys for which the target node is responsible.
- **Network simulation**: messages are delayed with random latency, but FIFO and reliable delivery are assumed.

---

## 🔄 How ongoing operations are tracked
- **Write operations**:  
  Each key has a `writeQueue` and an `inFlightWrite` context (`WriteCtx`) storing the client, versions received, and a timeout.
- **Read operations**:  
  Each key has `pendingGets` (responses collected), `pendingClients` (clients waiting for the same key), and `getTimeouts` (abort if quorum not reached).
- **Pending writes marker**:  
  Replicas mark keys as `pendingWrite` while responding to version requests, so GETs are retried until the write is stable.

---

## ✉️ Main messages used in the system
- **Client ↔ Node**: `UpdateRequest`, `GetRequest` (plus responses).  
- **Replication / Quorum**: `VersionRequest`, `VersionResponse`, `UpdateInternal`, `InternalGet`, `ValueResponse`, `PendingResponse`.  
- **Membership management**: `JoinRequest`, `JoinNotification`, `LeaveRequest`, `LeaveNotification`, `NodeListRequest`, `NodeListResponse`.  
- **Data transfer**: `TransferKeysRequest`, `TransferKeysResponse`, `RecoveryFetchRequest`, `RecoveryFetchResponse`.  

---

## 💥 Crash and Recovery
- **Crash**: simulated by stopping a node actor (`system.stop(nodeRef)`); the `PersistentStorage` map retains its data.  
- **Recovery**: the node restarts with the same ID, requests the ring view (`NodeListRequest`), drops obsolete keys, then fetches only the keys for which it is primary (`RecoveryFetchRequest`). A final read-back ensures alignment.

---

## 📖 Discussion
- All required features are covered: quorum reads/writes, sequential consistency, join/leave/recovery with selective key transfer.  
- No redundant messages: multiple clients on the same key share a single quorum fan-out; reads/writes are coalesced.  
- Code is modular and maintainable: clear separation between messages, storage, ring management, and node behavior.  
- Sequential consistency is enforced both by serializing writes per key and by retrying GETs if overlapping with writes.  

---

## ✅ Sequential Consistency
The system guarantees **Sequential Consistency**:
- Writes are serialized per key (only one in-flight write).  
- GETs are blocked/retried if a write is in progress (`PendingResponse`).  
- Thus, all clients observe operations in the same order.

---

## 📉 Message minimization
- **Write**: coordinator collects W version responses, then sends a single `UpdateInternal` to all N replicas.  
- **Read**: coordinator fans out once, collects R responses, replies to all waiting clients.  
- **Join**: successor transfers only the keys for which the new node is primary.  
- **Leave**: leaving node sends each key only to its new responsible replicas.  
- **Recovery**: peers send only the keys for which the recovering node is primary.  

All strategies ensure that data is sent **only to nodes responsible for it**.

---

## ⚙️ Assumptions
- Reliable, FIFO network (messages delayed but not lost or reordered).  
- At most one membership change (join/leave/recovery) at a time.  
- Node IDs are unique and stable.  
- PersistentStorage simulates stable storage as an in-memory static map.

---

## 🚀 How to run
1. Install Gradle and import the project files. 
2. Build the project with `gradle build` 
2. Execute with  `gradle run` to start the actor system and execute the predefined test scenarios.  
3. Logs in the console will show step-by-step behavior: quorum checks, updates, reads, joins, leaves, crash/recovery.

---

## 👥 Authors
- *Cappellaro Nicola* - nicola.cappellaro@studenti.unitn.it
- *Zannoni Riccardo* - riccardo.zannoni@studenti.unitn.it


