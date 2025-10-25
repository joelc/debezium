/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.binlog.lock;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;

/**
 * Programmatic ByteBuddy interceptor that instruments BinaryLogClient to add global lock management.
 *
 * <p>This class provides a non-agent approach to instrumenting BinaryLogClient. It uses ByteBuddy's
 * programmatic API with self-attachment to transform the class at runtime.
 *
 * <p><strong>Usage:</strong>
 * <pre>
 * // Initialize BEFORE any BinaryLogClient instantiation
 * GlobalLockManager.GlobalLockConfig config = GlobalLockManager.GlobalLockConfig.builder()
 *     .enabled(true)
 *     .lockName("my_binlog_lock")
 *     .lockTimeoutSeconds(30)
 *     .build();
 *
 * BinlogLockInterceptor.initialize(config);
 *
 * // Now create your Debezium Engine as normal
 * DebeziumEngine engine = ...
 * </pre>
 *
 * <p><strong>Requirements:</strong>
 * <ul>
 *   <li>JVM flag: {@code -Djdk.attach.allowAttachSelf=true} (Java 9+)</li>
 *   <li>Must be called BEFORE BinaryLogClient class is loaded</li>
 * </ul>
 *
 * @author Debezium Community
 */
public class BinlogLockInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BinlogLockInterceptor.class);
    private static volatile boolean initialized = false;

    /**
     * Initializes the BinaryLogClient interceptor with the given configuration.
     *
     * @param config Configuration for global lock behavior
     * @throws IllegalStateException if already initialized or if transformation fails
     */
    public static synchronized void initialize(GlobalLockManager.GlobalLockConfig config) {
        if (initialized) {
            LOGGER.warn("BinlogLockInterceptor already initialized, skipping");
            return;
        }

        LOGGER.info("Initializing BinlogLockInterceptor...");

        try {
            // Initialize the GlobalLockManager
            GlobalLockManager.initialize(config);

            // Install ByteBuddy agent (self-attach)
            LOGGER.debug("Installing ByteBuddy agent (self-attach)...");
            ByteBuddyAgent.install();

            // Transform the BinaryLogClient class
            LOGGER.debug("Transforming BinaryLogClient class...");
            Class<?> binaryLogClientClass = Class.forName("com.github.shyiko.mysql.binlog.BinaryLogClient");

            new ByteBuddy()
                .redefine(binaryLogClientClass)
                // Intercept connect() method (no args)
                .method(named("connect").and(takesArguments(0)))
                .intercept(MethodDelegation.to(BinaryLogClientInterceptor.class)
                    .filter(named("interceptConnect")))
                // Intercept connect(long timeout) method
                .method(named("connect").and(takesArguments(1)))
                .intercept(MethodDelegation.to(BinaryLogClientInterceptor.class)
                    .filter(named("interceptConnect")))
                // Intercept disconnect() method
                .method(named("disconnect"))
                .intercept(MethodDelegation.to(BinaryLogClientInterceptor.class)
                    .filter(named("interceptDisconnect")))
                .make()
                .load(binaryLogClientClass.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());

            initialized = true;
            LOGGER.info("BinlogLockInterceptor initialized successfully");

        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                "BinaryLogClient class not found. Ensure mysql-binlog-connector-java is on the classpath.", e);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize BinlogLockInterceptor", e);
        }
    }

    /**
     * Initializes with default configuration (disabled).
     * Useful for testing or conditional initialization.
     */
    public static void initializeWithDefaults() {
        GlobalLockManager.GlobalLockConfig config = GlobalLockManager.GlobalLockConfig.builder()
            .enabled(false)
            .build();
        initialize(config);
    }

    /**
     * Returns whether the interceptor has been initialized.
     */
    public static boolean isInitialized() {
        return initialized;
    }
}
