/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Datanode lifecycle, SCM and Recon communication, and SCM command processing.
 *
 * <h2>Class Structure</h2>
 *
 * <p>Ownership relationships:
 *
 * <pre>
 * DatanodeStateMachine
 * +-- StateContext
 * `-- SCMConnectionManager
 *     `-- EndpointStateMachine (one per SCM or Recon endpoint)
 *         +-- RPC client
 *         `-- Single-thread task executor
 * </pre>
 *
 * <p>{@link StateContext} also holds a reference back to its parent {@link DatanodeStateMachine}. Registration and
 * heartbeat tasks share this context rather than keeping a separate context for each endpoint.
 *
 * <h2>Class Responsibilities</h2>
 *
 * <ul>
 * <li>{@link DatanodeStateMachine} owns the datanode lifecycle and coordinates state execution, reporting and command
 * processing. It starts and stops the associated services, threads and executors.</li>
 * <li>{@link StateContext} holds the current datanode state, heartbeat timing, pending reports and actions, and queued
 * SCM commands. It selects and executes the implementation for the current datanode state.</li>
 * <li>{@link SCMConnectionManager} manages the map of SCM and Recon endpoints and creates, replaces and closes their
 * connections. It does not manage each endpoint's protocol state.</li>
 * <li>{@link EndpointStateMachine} owns one endpoint's RPC client, protocol state, single-thread task executor and
 * cached FCR lease. It prevents duplicate queued or running endpoint tasks.</li>
 * </ul>
 *
 * <h2>Execution Flow</h2>
 *
 * <p>{@link DatanodeStateMachine} runs the datanode state-machine loop through {@link StateContext}.
 * The datanode lifecycle states are {@code INIT}, {@code RUNNING} and {@code SHUTDOWN};
 * these are distinct from endpoint protocol states. While the datanode is running, {@link
 * org.apache.hadoop.ozone.container.common.states.datanode.RunningDatanodeState RunningDatanodeState} selects version,
 * registration or heartbeat tasks according to each endpoint's {@code GETVERSION}, {@code REGISTER} or
 * {@code HEARTBEAT} state. Tasks run on the endpoint's own executor, allowing endpoints to communicate independently.
 *
 * <p>Report publishers and container operations supply reports and actions through {@link StateContext}. Heartbeat
 * tasks collect these for their endpoint, process SCM responses and enqueue commands in the context. A separate
 * command-processing thread polls the command queue and delegates commands to
 * {@link org.apache.hadoop.ozone.container.common.statemachine.commandhandler.CommandDispatcher} handlers.
 *
 * <h2 id="lock-ordering">Endpoint and Report Lock Ordering</h2>
 *
 * <p>The following rules cover endpoint tasks, command queues and container reporting, not every datanode lock.
 * Acquire only the locks an operation needs. If multiple locks are held together, acquire them in the order shown by
 * the arrows.
 * Ordering applies to specific locks and monitors, not entire classes. Release nested locks in reverse order.
 *
 * <p>{@link SCMConnectionManager}'s {@code mapLock} protects {@code scmMachines}, the map of SCM and Recon endpoints
 * known to the datanode.
 *
 * <ul>
 * <li><b>Commands Processing:</b> {@code EndpointStateMachine.lock -> StateContext.commandQueueLock
 * -> SCMConnectionManager.mapLock}.
 * Heartbeat tasks enqueue SCM commands from the heartbeat response to the command queue while holding
 * the endpoint RPC lock.
 * During startup, {@link StateContext#getNextCommand()} holds {@code commandQueueLock} while checking SCM endpoint
 * readiness through {@link SCMConnectionManager#getValues()}, which acquires {@code mapLock}.
 * Commands are dispatched after releasing {@code commandQueueLock}. While holding {@code SCMConnectionManager.mapLock},
 * do not acquire endpoint RPC or command-queue locks, or wait for endpoint tasks to finish.</li>
 * <li><b>Reports:</b> {@code EndpointStateMachine.lock -> ContainerSet monitor
 * -> StateContext.endpointLock}. Endpoint tasks may generate an FCR while holding the RPC lock.
 * FCR generation and cleanup of covered ICRs hold the ContainerSet monitor across both operations, preventing ICR
 * generation and enqueueing from interleaving with them. Do not acquire the ContainerSet monitor or an endpoint RPC
 * lock while holding {@code StateContext.endpointLock}. This lock also serializes endpoint addition and removal with
 * report and action queue operations, report-readiness updates and queue-size snapshots. Release it before generating
 * an FCR or obtaining a pipeline report.</li>
 * <li><b>Endpoint scheduling:</b> the endpoint RPC lock precedes the {@link EndpointStateMachine} object's monitor.
 * Endpoint removal in {@link SCMConnectionManager} may also acquire this monitor while holding {@code mapLock}.
 * This monitor protects task submission and shutdown, not RPC execution; do not acquire the RPC lock, perform RPCs
 * or wait for endpoint tasks while holding it. Task factories run under it and must follow these restrictions.</li>
 * </ul>
 *
 * <p>Lock fields document their protected state and local restrictions; protected fields use {@code @GuardedBy}
 * where applicable. Keep caller-held-lock requirements on methods and warnings beside surprising nested calls.
 * When changing a call path, check its transitive lock acquisitions, including callbacks and shutdown, against
 * these rules rather than assuming that an internally synchronized method is safe to call under any lock.
 */
package org.apache.hadoop.ozone.container.common.statemachine;
