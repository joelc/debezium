/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

/**
 * Programmatic ByteBuddy interceptor for adding global lock coordination to BinaryLogClient.
 *
 * <p>This package provides a non-invasive, non-agent-based way to add MySQL named lock
 * (GET_LOCK/RELEASE_LOCK) acquisition around BinaryLogClient's binlog streaming lifecycle.
 *
 * <p><strong>Key Features:</strong>
 * <ul>
 *   <li>No Java agent required - uses programmatic ByteBuddy API</li>
 *   <li>No source code modification - works with external libraries</li>
 *   <li>Multi-instance coordination using MySQL GET_LOCK()</li>
 *   <li>Automatic lock lifecycle management</li>
 * </ul>
 *
 * <p><strong>Usage Example:</strong>
 * <pre>
 * // Initialize before any Debezium code
 * GlobalLockManager.GlobalLockConfig config = GlobalLockManager.GlobalLockConfig.builder()
 *     .enabled(true)
 *     .lockName("my_app_lock")
 *     .lockTimeoutSeconds(30)
 *     .build();
 *
 * BinlogLockInterceptor.initialize(config);
 *
 * // Now create Debezium Engine as normal
 * DebeziumEngine engine = ...
 * </pre>
 *
 * <p><strong>Requirements:</strong>
 * <ul>
 *   <li>JVM flag: {@code -Djdk.attach.allowAttachSelf=true} (Java 9+)</li>
 *   <li>Must be called before BinaryLogClient class is loaded</li>
 * </ul>
 *
 * <p>Key components:
 * <ul>
 *   <li>{@link io.debezium.binlog.lock.BinlogLockInterceptor} - Main entry point for initialization</li>
 *   <li>{@link io.debezium.binlog.lock.GlobalLockManager} - Lock lifecycle management</li>
 *   <li>{@link io.debezium.binlog.lock.BinaryLogClientInterceptor} - ByteBuddy method interceptor</li>
 * </ul>
 *
 * @see io.debezium.binlog.lock.BinlogLockInterceptor
 * @see io.debezium.binlog.lock.GlobalLockManager
 */
package io.debezium.binlog.lock;
