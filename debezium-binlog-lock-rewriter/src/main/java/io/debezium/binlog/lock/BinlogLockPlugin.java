/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import static net.bytebuddy.matcher.ElementMatchers.*;

import java.util.concurrent.Callable;

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

/**
 * ByteBuddy build plugin that transforms BinaryLogClient at compile time.
 * This plugin is invoked by byte-buddy-maven-plugin during the Maven build.
 *
 * The transformed BinaryLogClient will execute GET_LOCK() on its own connection
 * channel before streaming begins, ensuring tight coupling between the lock
 * and the binlog connection.
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
     *
     * This executes GET_LOCK() on the BinaryLogClient's own PacketChannel
     * AFTER the connection is established but BEFORE binlog streaming begins.
     */
    public static class ConnectInterceptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(ConnectInterceptor.class);
        private static final GlobalLockManager lockManager = new GlobalLockManager();

        @RuntimeType
        public static Object intercept(
                @This Object client,
                @AllArguments Object[] args,
                @SuperCall Callable<?> zuper) throws Exception {

            LOGGER.debug("Intercepted BinaryLogClient.connect()");

            // First, execute the original connect() to establish the connection
            Object result = zuper.call();

            // NOW the channel is available - acquire lock on the SAME connection
            try {
                lockManager.acquireLock(client);
            } catch (Exception e) {
                LOGGER.error("Failed to acquire global lock on binlog connection", e);

                // Lock acquisition failed - disconnect the client
                try {
                    // Call disconnect to cleanup the connection
                    client.getClass().getMethod("disconnect").invoke(client);
                } catch (Exception disconnectEx) {
                    LOGGER.error("Error disconnecting after failed lock acquisition", disconnectEx);
                }

                throw new RuntimeException("Failed to acquire global lock on binlog connection", e);
            }

            return result;
        }
    }

    /**
     * Interceptor for BinaryLogClient.disconnect() method.
     *
     * Releases the global lock BEFORE disconnecting. Since the lock is on the
     * same connection, it will be auto-released by MySQL when the connection closes,
     * but we explicitly release it for clean shutdown.
     */
    public static class DisconnectInterceptor {

        private static final Logger LOGGER = LoggerFactory.getLogger(DisconnectInterceptor.class);
        private static final GlobalLockManager lockManager = new GlobalLockManager();

        @RuntimeType
        public static Object intercept(
                @This Object client,
                @SuperCall Callable<?> zuper) throws Exception {

            LOGGER.debug("Intercepted BinaryLogClient.disconnect()");

            // First, try to release the lock while connection is still alive
            try {
                lockManager.releaseLock(client);
            } catch (Exception e) {
                LOGGER.error("Error releasing lock during disconnect (will auto-release on connection close)", e);
                // Continue with disconnect even if explicit release fails
                // MySQL will auto-release when connection closes
            }

            // Now execute the original disconnect
            return zuper.call();
        }
    }
}
