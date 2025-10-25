/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.network.protocol.PacketChannel;
import com.github.shyiko.mysql.binlog.network.protocol.ResultSetRowPacket;
import com.github.shyiko.mysql.binlog.network.protocol.command.QueryCommand;

/**
 * Manages MySQL global locks using GET_LOCK() and RELEASE_LOCK() on the
 * BinaryLogClient's own connection channel.
 *
 * This ensures the lock is tightly coupled to the binlog streaming connection.
 * If the connection dies, MySQL automatically releases the lock.
 *
 * <p>System Properties:
 * <ul>
 *   <li>debezium.binlog.lock.enabled - Enable/disable locking (default: false)</li>
 *   <li>debezium.binlog.lock.name - Lock name (default: debezium_binlog_lock)</li>
 *   <li>debezium.binlog.lock.timeout.seconds - Timeout in seconds (default: 30)</li>
 * </ul>
 *
 * @author Debezium Community
 */
public class GlobalLockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalLockManager.class);

    // Configuration from system properties
    private final boolean enabled;
    private final String lockName;
    private final int lockTimeoutSeconds;

    public GlobalLockManager() {
        this.enabled = Boolean.parseBoolean(
            System.getProperty("debezium.binlog.lock.enabled", "false")
        );
        this.lockName = System.getProperty(
            "debezium.binlog.lock.name",
            "debezium_binlog_lock"
        );
        this.lockTimeoutSeconds = Integer.parseInt(
            System.getProperty("debezium.binlog.lock.timeout.seconds", "30")
        );

        if (enabled) {
            LOGGER.info("GlobalLockManager initialized: lockName={}, timeoutSeconds={}",
                lockName, lockTimeoutSeconds);
        }
    }

    /**
     * Acquires a global lock on the BinaryLogClient's connection channel.
     * This is called by the transformed connect() method.
     *
     * CRITICAL: This executes on the SAME connection as the binlog stream,
     * ensuring tight coupling - if the connection dies, MySQL auto-releases the lock.
     *
     * @param clientInstance The BinaryLogClient instance
     * @throws Exception if lock acquisition fails
     */
    public void acquireLock(Object clientInstance) throws Exception {
        if (!enabled) {
            LOGGER.trace("Global lock disabled, skipping acquisition");
            return;
        }

        LOGGER.info("Acquiring global lock '{}' on binlog connection", lockName);

        // Access the protected 'channel' field from BinaryLogClient
        PacketChannel channel = getChannel(clientInstance);
        if (channel == null) {
            throw new IllegalStateException("BinaryLogClient channel not available");
        }

        // Send GET_LOCK query on the same connection
        String query = String.format("SELECT GET_LOCK('%s', %d)", escapeSqlString(lockName), lockTimeoutSeconds);
        channel.write(new QueryCommand(query));

        // Read the result using reflection to call private readResultSet()
        ResultSetRowPacket[] resultSet = readResultSet(clientInstance);

        if (resultSet == null || resultSet.length == 0) {
            throw new RuntimeException("GET_LOCK returned no result");
        }

        // GET_LOCK returns: 1=success, 0=timeout, NULL=error
        String result = resultSet[0].getValue(0);
        if (result == null) {
            throw new RuntimeException("GET_LOCK failed with NULL (error occurred)");
        }

        int lockResult = Integer.parseInt(result);
        if (lockResult != 1) {
            throw new RuntimeException(String.format(
                "Failed to acquire global lock '%s' within %d seconds. " +
                "Another instance may be holding the lock. GET_LOCK returned: %d",
                lockName, lockTimeoutSeconds, lockResult
            ));
        }

        LOGGER.info("Successfully acquired global lock '{}' on binlog connection", lockName);
    }

    /**
     * Releases the global lock on the BinaryLogClient's connection channel.
     * This is called by the transformed disconnect() method.
     *
     * @param clientInstance The BinaryLogClient instance
     */
    public void releaseLock(Object clientInstance) {
        if (!enabled) {
            return;
        }

        LOGGER.info("Releasing global lock '{}'", lockName);

        try {
            PacketChannel channel = getChannel(clientInstance);
            if (channel == null) {
                LOGGER.debug("Channel not available, lock may have been auto-released on disconnect");
                return;
            }

            // Send RELEASE_LOCK query
            String query = String.format("SELECT RELEASE_LOCK('%s')", escapeSqlString(lockName));
            channel.write(new QueryCommand(query));

            // Read result
            ResultSetRowPacket[] resultSet = readResultSet(clientInstance);

            if (resultSet != null && resultSet.length > 0) {
                String result = resultSet[0].getValue(0);
                int releaseResult = result != null ? Integer.parseInt(result) : 0;

                // RELEASE_LOCK returns: 1=released, 0=not held, NULL=doesn't exist
                if (releaseResult == 1) {
                    LOGGER.info("Successfully released global lock '{}'", lockName);
                } else {
                    LOGGER.warn("RELEASE_LOCK returned {}: lock may not have been held by this connection", releaseResult);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error releasing global lock '{}'", lockName, e);
        }
    }

    /**
     * Access the protected 'channel' field from BinaryLogClient using reflection.
     */
    private PacketChannel getChannel(Object client) throws Exception {
        Field channelField = client.getClass().getDeclaredField("channel");
        channelField.setAccessible(true);
        return (PacketChannel) channelField.get(client);
    }

    /**
     * Call the private readResultSet() method from BinaryLogClient using reflection.
     */
    private ResultSetRowPacket[] readResultSet(Object client) throws Exception {
        Method readResultSetMethod = client.getClass().getDeclaredMethod("readResultSet");
        readResultSetMethod.setAccessible(true);
        return (ResultSetRowPacket[]) readResultSetMethod.invoke(client);
    }

    /**
     * Escape single quotes in SQL strings to prevent injection.
     */
    private String escapeSqlString(String input) {
        return input.replace("'", "''");
    }
}
