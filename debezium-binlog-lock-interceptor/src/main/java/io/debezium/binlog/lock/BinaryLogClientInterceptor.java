/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

/**
 * ByteBuddy interceptor for BinaryLogClient.connect() and disconnect() methods.
 * Acquires/releases global MySQL lock around the binlog streaming lifecycle.
 *
 * @author Debezium Community
 */
public class BinaryLogClientInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryLogClientInterceptor.class);

    /**
     * Intercepts BinaryLogClient.connect() method.
     * Acquires global lock before connecting, then proceeds with the original connect().
     */
    @RuntimeType
    public static Object interceptConnect(
            @This Object client,
            @AllArguments Object[] args,
            @SuperCall Callable<?> zuper,
            @Origin String origin) throws Exception {

        LOGGER.debug("Intercepted BinaryLogClient.connect() - {}", origin);

        try {
            // Extract connection details from BinaryLogClient using reflection
            String hostname = getFieldValue(client, "hostname", String.class);
            Integer port = getFieldValue(client, "port", Integer.class);
            String username = getFieldValue(client, "username", String.class);
            String password = getFieldValue(client, "password", String.class);

            if (hostname != null && port != null && username != null) {
                LOGGER.info("Acquiring global lock before connecting to {}:{}", hostname, port);

                // Acquire the global lock
                GlobalLockManager.getInstance().acquireLock(
                    client,
                    hostname,
                    port,
                    username,
                    password != null ? password : ""
                );
            } else {
                LOGGER.warn("Could not extract connection details from BinaryLogClient, " +
                           "skipping lock acquisition");
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to acquire global lock", e);
            throw new RuntimeException("Failed to acquire global lock before binlog streaming", e);
        } catch (Exception e) {
            LOGGER.error("Unexpected error during lock acquisition", e);
            // Continue anyway - don't break the connection attempt for unexpected errors
        }

        // Proceed with the original connect() method
        try {
            return zuper.call();
        } catch (Exception e) {
            // If connect fails, release the lock
            LOGGER.warn("BinaryLogClient.connect() failed, releasing lock");
            try {
                GlobalLockManager.getInstance().releaseLock(client);
            } catch (Exception releaseEx) {
                LOGGER.error("Error releasing lock after failed connect", releaseEx);
            }
            throw e;
        }
    }

    /**
     * Intercepts BinaryLogClient.disconnect() method.
     * Releases global lock after disconnecting.
     */
    @RuntimeType
    public static Object interceptDisconnect(
            @This Object client,
            @AllArguments Object[] args,
            @SuperCall Callable<?> zuper,
            @Origin String origin) throws Exception {

        LOGGER.debug("Intercepted BinaryLogClient.disconnect() - {}", origin);

        try {
            // First, execute the original disconnect
            return zuper.call();
        } finally {
            // Always release the lock after disconnect
            try {
                GlobalLockManager.getInstance().releaseLock(client);
            } catch (Exception e) {
                LOGGER.error("Error releasing lock during disconnect", e);
            }
        }
    }

    /**
     * Uses reflection to get a field value from the BinaryLogClient.
     */
    @SuppressWarnings("unchecked")
    private static <T> T getFieldValue(Object obj, String fieldName, Class<T> type) {
        try {
            Field field = obj.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(obj);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            LOGGER.debug("Could not access field '{}' on {}", fieldName, obj.getClass().getName(), e);
            return null;
        }
    }
}
