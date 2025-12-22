---
name: OM Follower Reads
overview: Implement HDFS-style follower reads for Ozone Manager using AlignmentContext and a new FollowerReadProxyProvider, allowing clients to route read requests to OM followers for improved read scalability.
todos:
  - id: proto-msync
    content: Add Msync request/response types to OmClientProtocol.proto
    status: completed
  - id: client-alignment
    content: Implement OMClientAlignmentContext for tracking client-side state ID
    status: completed
  - id: server-alignment
    content: Implement OMServerAlignmentContext using StateMachine lastAppliedIndex
    status: completed
  - id: follower-proxy
    content: Create OMFollowerReadProxyProvider for routing reads to followers
    status: completed
  - id: server-integration
    content: Integrate AlignmentContext with OM RPC server and add msync handler
    status: completed
    dependencies:
      - proto-msync
      - server-alignment
  - id: client-integration
    content: Integrate FollowerReadProxyProvider with Hadoop3OmTransport
    status: completed
    dependencies:
      - client-alignment
      - follower-proxy
  - id: config-keys
    content: Add configuration keys for follower read feature
    status: completed
  - id: tests
    content: Add unit and integration tests for follower read functionality
    status: completed
    dependencies:
      - server-integration
      - client-integration
---

# Implement HDFS-style Follower Reads for Ozone Manager

## Overview

This plan implements consistent follower reads for Ozone Manager following the HDFS observer read pattern (HDFS-12943). The key mechanism uses `AlignmentContext` to track state IDs between client and server, allowing the server to requeue requests until followers catch up to the client's expected state.

## Architecture Flow

```mermaid
sequenceDiagram
    participant Client
    participant ProxyProvider as FollowerReadProxyProvider
    participant Leader as OM_Leader
    participant Follower as OM_Follower
    
    Note over Client,Follower: msync flow (establish baseline)
    Client->>ProxyProvider: msync()
    ProxyProvider->>Leader: msync request
    Leader-->>ProxyProvider: lastAppliedIndex=100
    ProxyProvider-->>Client: stateId=100 stored in AlignmentContext
    
    Note over Client,Follower: Read from follower
    Client->>ProxyProvider: lookupKey() [read]
    ProxyProvider->>Follower: lookupKey() with stateId=100
    alt Follower not caught up
        Follower->>Follower: requeue call until lastApplied >= 100
    end
    Follower-->>ProxyProvider: response with follower stateId
    ProxyProvider-->>Client: key data
    
    Note over Client,Follower: Write goes to leader
    Client->>ProxyProvider: createKey() [write]
    ProxyProvider->>Leader: createKey()
    Leader-->>ProxyProvider: response with stateId=105
    ProxyProvider-->>Client: stateId updated to 105
```



## Key Files to Create/Modify

### 1. Client-side AlignmentContext

Create [`hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/ha/OMClientAlignmentContext.java`](hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/ha/OMClientAlignmentContext.java) implementing `AlignmentContext`:

- Track `lastSeenStateId` received from OM responses
- `updateRequestState()`: Set `stateId` in RPC request header  
- `receiveResponseState()`: Update `lastSeenStateId` from RPC response header
- `isCoordinatedCall()`: Return true for read operations that need coordination

### 2. Server-side AlignmentContext  

Create [`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ha/OMServerAlignmentContext.java`](hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ha/OMServerAlignmentContext.java) implementing `AlignmentContext`:

- `getLastSeenStateId()`: Return `OzoneManagerStateMachine.getLastAppliedTermIndex().getIndex()`
- `updateResponseState()`: Set current `lastAppliedIndex` in RPC response header
- `receiveRequestState()`: Extract client's `stateId` from request header
- `isCoordinatedCall()`: Identify which methods need coordination (read ops to followers)

### 3. Follower Read Proxy Provider

Create [`hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/ha/OMFollowerReadProxyProvider.java`](hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/ha/OMFollowerReadProxyProvider.java):

- Extend `OMFailoverProxyProviderBase`
- Route read requests to OM followers (round-robin among followers)
- Route write requests and msync to OM leader
- Track current leader vs follower nodes
- Handle fallback to leader on follower failures

### 4. msync Request Type

Add new OMRequest type for msync in protobuf:

- Modify [`hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto`](hadoop-ozone/interface-client/src/main/proto/OmClientProtocol.proto)
- Add `Msync` to `Type` enum and create `MsyncRequest`/`MsyncResponse` messages
- Response includes current `lastAppliedIndex` from leader

### 5. Server Integration

Modify [`hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java`](hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/OzoneManager.java):

- Create `OMServerAlignmentContext` during initialization
- Pass AlignmentContext to `RPC.Builder.setAlignmentContext()` in `startRpcServer()`
- Add msync request handler in [`OzoneManagerRequestHandler.java`](hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/protocolPB/OzoneManagerRequestHandler.java)

### 6. Client Integration  

Modify [`hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/protocolPB/Hadoop3OmTransport.java`](hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/protocolPB/Hadoop3OmTransport.java):

- Add option to use `OMFollowerReadProxyProvider` instead of `HadoopRpcOMFailoverProxyProvider`
- Pass `OMClientAlignmentContext` to proxy creation

### 7. Configuration

Add configuration keys to [`hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/OMConfigKeys.java`](hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/om/OMConfigKeys.java):

- `ozone.om.follower.read.enabled` (default: false)
- `ozone.om.follower.read.auto.msync.enabled` (auto-msync before reads)
- `ozone.om.follower.read.stale.threshold.ms` (max staleness before forcing msync)

## Existing Infrastructure Leveraged

- AlignmentContext interface: [`hadoop-hdds/common/src/main/java/org/apache/hadoop/ipc_/AlignmentContext.java`](hadoop-hdds/common/src/main/java/org/apache/hadoop/ipc_/AlignmentContext.java) - already exists
- Server requeue logic: [`Server.Handler.run()`](hadoop-hdds/common/src/main/java/org/apache/hadoop/ipc_/Server.java) lines 2920-2940 - already implements call requeuing when `clientStateId > alignmentContext.getLastSeenStateId()`
- StateMachine index: [`OzoneManagerStateMachine.getLastAppliedTermIndex()`](hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/ratis/OzoneManagerStateMachine.java) - provides last applied Ratis index
- Read/Write classification: [`OmUtils.isReadOnly()`](hadoop-ozone/common/src/main/java/org/apache/hadoop/ozone/OmUtils.java) - determines if request is read-only

## Limitations

- Only supports Hadoop RPC clients (gRPC requires separate implementation)