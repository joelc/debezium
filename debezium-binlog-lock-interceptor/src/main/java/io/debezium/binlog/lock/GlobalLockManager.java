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
 * This is a singleton that maintains separate MySQL connections for lock management.
 *
 * @author Debezium Community
 */
public class GlobalLockManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalLockManager.class);
    private static volatile GlobalLockManager INSTANCE;

    // Map to track lock connections per BinaryLogClient instance
    private final Map<Object, LockConnection> lockConnections = new ConcurrentHashMap<>();

    // Configuration
    private final GlobalLockConfig config;

    private GlobalLockManager(GlobalLockConfig config) {
        this.config = config;
        LOGGER.info("GlobalLockManager initialized: enabled={}, lockName={}, timeoutSeconds={}",
            config.enabled, config.lockName, config.lockTimeoutSeconds);
    }

    public static void initialize(GlobalLockConfig config) {
        if (INSTANCE == null) {
            synchronized (GlobalLockManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new GlobalLockManager(config);
                }
            }
        }
    }

    public static GlobalLockManager getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException(
                "GlobalLockManager not initialized. Call initialize() first.");
        }
        return INSTANCE;
    }

    /**
     * Acquires a global lock before the BinaryLogClient connects.
     *
     * @param clientInstance The BinaryLogClient instance (used as key)
     * @param hostname MySQL host
     * @param port MySQL port
     * @param username MySQL username
     * @param password MySQL password
     * @throws SQLException if lock acquisition fails
     */
    public void acquireLock(Object clientInstance, String hostname, int port,
                           String username, String password) throws SQLException {
        if (!config.enabled) {
            LOGGER.debug("Global lock is disabled, skipping acquisition");
            return;
        }

        LOGGER.info("Attempting to acquire global lock '{}' for {}:{}", config.lockName, hostname, port);

        // Create dedicated connection for the lock
        String jdbcUrl = buildJdbcUrl(hostname, port);

        Connection lockConnection = null;
        try {
            lockConnection = DriverManager.getConnection(jdbcUrl, username, password);
            lockConnection.setAutoCommit(true);

            // Attempt to acquire the lock
            boolean acquired = executeGetLock(lockConnection, config.lockName, config.lockTimeoutSeconds);

            if (!acquired) {
                if (lockConnection != null) {
                    try {
                        lockConnection.close();
                    } catch (SQLException e) {
                        LOGGER.warn("Error closing connection after failed lock acquisition", e);
                    }
                }
                throw new SQLException(String.format(
                    "Failed to acquire global lock '%s' within %d seconds. " +
                    "Another Debezium instance may be holding the lock.",
                    config.lockName, config.lockTimeoutSeconds
                ));
            }

            LOGGER.info("Successfully acquired global lock '{}'", config.lockName);

            // Store the lock connection for later release
            LockConnection lc = new LockConnection(lockConnection, hostname, port);
            lockConnections.put(clientInstance, lc);

        } catch (SQLException e) {
            if (lockConnection != null) {
                try {
                    lockConnection.close();
                } catch (SQLException closeEx) {
                    LOGGER.warn("Error closing connection after exception", closeEx);
                }
            }
            throw e;
        }
    }

    /**
     * Releases the global lock when the BinaryLogClient disconnects.
     *
     * @param clientInstance The BinaryLogClient instance
     */
    public void releaseLock(Object clientInstance) {
        if (!config.enabled) {
            return;
        }

        LockConnection lc = lockConnections.remove(clientInstance);
        if (lc == null) {
            LOGGER.debug("No lock connection found for client instance");
            return;
        }

        LOGGER.info("Releasing global lock '{}' for {}:{}", config.lockName, lc.hostname, lc.port);

        try {
            executeReleaseLock(lc.connection, config.lockName);
            LOGGER.info("Successfully released global lock '{}'", config.lockName);
        } catch (SQLException e) {
            LOGGER.error("Error releasing global lock '{}'", config.lockName, e);
        } finally {
            try {
                lc.connection.close();
            } catch (SQLException e) {
                LOGGER.warn("Error closing lock connection", e);
            }
        }
    }

    private String buildJdbcUrl(String hostname, int port) {
        StringBuilder url = new StringBuilder("jdbc:mysql://")
            .append(hostname)
            .append(":")
            .append(port)
            .append("/?allowPublicKeyRetrieval=true");

        if (config.sslMode != null && !config.sslMode.isEmpty()) {
            url.append("&useSSL=").append(!config.sslMode.equalsIgnoreCase("disabled"));
            if (!config.sslMode.equalsIgnoreCase("disabled")) {
                url.append("&sslMode=").append(config.sslMode);
            }
        } else {
            url.append("&useSSL=false");
        }

        return url.toString();
    }

    /**
     * Executes GET_LOCK(lockName, timeout) and returns true if acquired.
     */
    private boolean executeGetLock(Connection conn, String lockName, int timeoutSeconds)
            throws SQLException {
        String sql = "SELECT GET_LOCK(?, ?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lockName);
            stmt.setInt(2, timeoutSeconds);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int result = rs.getInt(1);
                    // GET_LOCK returns:
                    // 1 if the lock was obtained successfully
                    // 0 if the attempt timed out
                    // NULL if an error occurred
                    LOGGER.debug("GET_LOCK returned: {}", result);
                    return result == 1;
                }
            }
        }
        return false;
    }

    /**
     * Executes RELEASE_LOCK(lockName).
     */
    private void executeReleaseLock(Connection conn, String lockName) throws SQLException {
        String sql = "SELECT RELEASE_LOCK(?)";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, lockName);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    int result = rs.getInt(1);
                    // RELEASE_LOCK returns:
                    // 1 if the lock was released
                    // 0 if the lock was not established by this thread
                    // NULL if the named lock did not exist
                    LOGGER.debug("RELEASE_LOCK returned: {}", result);

                    if (result != 1) {
                        LOGGER.warn("RELEASE_LOCK returned unexpected value: {}", result);
                    }
                }
            }
        }
    }

    /**
     * Internal class to track lock connection details.
     */
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

    /**
     * Configuration for global lock behavior.
     */
    public static class GlobalLockConfig {
        private final boolean enabled;
        private final String lockName;
        private final int lockTimeoutSeconds;
        private final String sslMode;

        private GlobalLockConfig(Builder builder) {
            this.enabled = builder.enabled;
            this.lockName = builder.lockName;
            this.lockTimeoutSeconds = builder.lockTimeoutSeconds;
            this.sslMode = builder.sslMode;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private boolean enabled = false;
            private String lockName = "debezium_binlog_replication_lock";
            private int lockTimeoutSeconds = 30;
            private String sslMode = null;

            public Builder enabled(boolean enabled) {
                this.enabled = enabled;
                return this;
            }

            public Builder lockName(String lockName) {
                this.lockName = lockName;
                return this;
            }

            public Builder lockTimeoutSeconds(int lockTimeoutSeconds) {
                this.lockTimeoutSeconds = lockTimeoutSeconds;
                return this;
            }

            public Builder sslMode(String sslMode) {
                this.sslMode = sslMode;
                return this;
            }

            public GlobalLockConfig build() {
                return new GlobalLockConfig(this);
            }
        }
    }
}
