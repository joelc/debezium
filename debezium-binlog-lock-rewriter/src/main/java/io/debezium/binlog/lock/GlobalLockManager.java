/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages MySQL global locks using GET_LOCK() and RELEASE_LOCK() functions.
 * Configuration is done via system properties (no code changes needed).
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
    private static final GlobalLockManager INSTANCE = new GlobalLockManager();

    // Map to track lock connections per BinaryLogClient instance
    private final Map<Object, LockConnection> lockConnections = new ConcurrentHashMap<>();

    // Configuration from system properties
    private final boolean enabled;
    private final String lockName;
    private final int lockTimeoutSeconds;

    private GlobalLockManager() {
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

    public static GlobalLockManager getInstance() {
        return INSTANCE;
    }

    /**
     * Acquires a global lock before the BinaryLogClient connects.
     * This is called by the transformed BinaryLogClient.connect() method.
     *
     * @param clientInstance The BinaryLogClient instance
     * @param hostname MySQL host
     * @param port MySQL port
     * @param username MySQL username
     * @param password MySQL password
     * @throws SQLException if lock acquisition fails
     */
    public void acquireLock(Object clientInstance, String hostname, int port,
                           String username, String password) throws SQLException {
        if (!enabled) {
            LOGGER.trace("Global lock disabled, skipping acquisition");
            return;
        }

        LOGGER.info("Acquiring global lock '{}' for {}:{}", lockName, hostname, port);

        String jdbcUrl = String.format(
            "jdbc:mysql://%s:%d/?allowPublicKeyRetrieval=true&useSSL=false",
            hostname, port
        );

        Connection lockConnection = null;
        try {
            lockConnection = DriverManager.getConnection(jdbcUrl, username, password);
            lockConnection.setAutoCommit(true);

            boolean acquired = executeGetLock(lockConnection, lockName, lockTimeoutSeconds);

            if (!acquired) {
                closeQuietly(lockConnection);
                throw new SQLException(String.format(
                    "Failed to acquire global lock '%s' within %d seconds. " +
                    "Another instance may be holding the lock.",
                    lockName, lockTimeoutSeconds
                ));
            }

            LOGGER.info("Successfully acquired global lock '{}'", lockName);
            lockConnections.put(clientInstance, new LockConnection(lockConnection, hostname, port));

        } catch (SQLException e) {
            closeQuietly(lockConnection);
            throw e;
        }
    }

    /**
     * Releases the global lock when the BinaryLogClient disconnects.
     * This is called by the transformed BinaryLogClient.disconnect() method.
     *
     * @param clientInstance The BinaryLogClient instance
     */
    public void releaseLock(Object clientInstance) {
        if (!enabled) {
            return;
        }

        LockConnection lc = lockConnections.remove(clientInstance);
        if (lc == null) {
            LOGGER.trace("No lock connection found for client instance");
            return;
        }

        LOGGER.info("Releasing global lock '{}' for {}:{}", lockName, lc.hostname, lc.port);

        try {
            executeReleaseLock(lc.connection, lockName);
            LOGGER.info("Successfully released global lock '{}'", lockName);
        } catch (SQLException e) {
            LOGGER.error("Error releasing global lock '{}'", lockName, e);
        } finally {
            closeQuietly(lc.connection);
        }
    }

    private boolean executeGetLock(Connection conn, String lockName, int timeoutSeconds)
            throws SQLException {
        String sql = "SELECT GET_LOCK(?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lockName);
            stmt.setInt(2, timeoutSeconds);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int result = rs.getInt(1);
                    LOGGER.debug("GET_LOCK returned: {}", result);
                    return result == 1;
                }
            }
        }
        return false;
    }

    private void executeReleaseLock(Connection conn, String lockName) throws SQLException {
        String sql = "SELECT RELEASE_LOCK(?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lockName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int result = rs.getInt(1);
                    LOGGER.debug("RELEASE_LOCK returned: {}", result);

                    if (result != 1) {
                        LOGGER.warn("RELEASE_LOCK returned unexpected value: {}", result);
                    }
                }
            }
        }
    }

    private void closeQuietly(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOGGER.warn("Error closing connection", e);
            }
        }
    }

    private static class LockConnection {
        final Connection connection;
        final String hostname;
        final int port;

        LockConnection(Connection connection, String hostname, int port) {
            this.connection = connection;
            this.hostname = hostname;
            this.port = port;
        }
    }
}
