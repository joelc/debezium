/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import static net.bytebuddy.matcher.ElementMatchers.*;

import java.lang.reflect.Field;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.build.Plugin;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import java.util.concurrent.Callable;

/**
 * ByteBuddy build plugin that transforms BinaryLogClient at compile time.
 * This plugin is invoked by byte-buddy-maven-plugin during the Maven build.
 *
 * @author Debezium Community
 */
public class BinlogLockPlugin implements Plugin {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogLockPlugin.class);

    @Override
    public boolean matches(TypeDescription target) {
        boolean matches = target.getName().equals("com.github.shyiko.mysql.binlog.BinaryLogClient");
        if (matches) {
            LOGGER.info("ByteBuddy plugin matched BinaryLogClient for transformation");
        }
        return matches;
    }

    @Override
    public DynamicType.Builder<?> apply(DynamicType.Builder<?> builder,
                                         TypeDescription typeDescription,
                                         ClassFileLocator classFileLocator) {
        LOGGER.info("Applying lock interceptor transformation to BinaryLogClient");

        return builder
            // Intercept connect() - no arguments
            .method(named("connect").and(takesArguments(0)))
            .intercept(MethodDelegation.to(ConnectInterceptor.class))
            // Intercept connect(long timeout) - one argument
            .method(named("connect").and(takesArguments(1)))
            .intercept(MethodDelegation.to(ConnectInterceptor.class))
            // Intercept disconnect()
            .method(named("disconnect"))
            .intercept(MethodDelegation.to(DisconnectInterceptor.class));
    }

    @Override
    public void close() {
        // No resources to clean up
    }

    /**
     * Interceptor for BinaryLogClient.connect() methods.
     */
    public static class ConnectInterceptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectInterceptor.class);

        @RuntimeType
        public static Object intercept(
                @This Object client,
                @AllArguments Object[] args,
                @SuperCall Callable<?> zuper) throws Exception {

            LOGGER.debug("Intercepted BinaryLogClient.connect()");

            try {
                // Extract connection details via reflection
                String hostname = getField(client, "hostname", String.class);
                Integer port = getField(client, "port", Integer.class);
                String username = getField(client, "username", String.class);
                String password = getField(client, "password", String.class);

                if (hostname != null && port != null && username != null) {
                    // Acquire the global lock
                    GlobalLockManager.getInstance().acquireLock(
                        client,
                        hostname,
                        port,
                        username,
                        password != null ? password : ""
                    );
                }
            } catch (SQLException e) {
                LOGGER.error("Failed to acquire global lock", e);
                throw new RuntimeException("Failed to acquire global lock before binlog streaming", e);
            } catch (Exception e) {
                LOGGER.warn("Error during lock acquisition, continuing with connect", e);
                // Don't fail if lock acquisition has unexpected errors
            }

            // Proceed with original connect()
            try {
                return zuper.call();
            } catch (Exception e) {
                // If connect fails, release the lock
                try {
                    GlobalLockManager.getInstance().releaseLock(client);
                } catch (Exception releaseEx) {
                    LOGGER.error("Error releasing lock after failed connect", releaseEx);
                }
                throw e;
            }
        }

        @SuppressWarnings("unchecked")
        private static <T> T getField(Object obj, String fieldName, Class<T> type) {
            try {
                Field field = obj.getClass().getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(obj);
            } catch (Exception e) {
                LOGGER.debug("Could not access field '{}'", fieldName, e);
                return null;
            }
        }
    }

    /**
     * Interceptor for BinaryLogClient.disconnect() method.
     */
    public static class DisconnectInterceptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(DisconnectInterceptor.class);

        @RuntimeType
        public static Object intercept(
                @This Object client,
                @SuperCall Callable<?> zuper) throws Exception {

            LOGGER.debug("Intercepted BinaryLogClient.disconnect()");

            try {
                // Execute original disconnect first
                return zuper.call();
            } finally {
                // Always release lock after disconnect
                try {
                    GlobalLockManager.getInstance().releaseLock(client);
                } catch (Exception e) {
                    LOGGER.error("Error releasing lock during disconnect", e);
                }
            }
        }
    }
}
