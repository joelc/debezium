# Quick Start Guide

This guide shows you how to use the BinaryLogClient Lock Interceptor in 5 minutes.

## Step 1: Add Dependency

Add to your application's `pom.xml`:

```xml
<dependency>
    <groupId>io.debezium</groupId>
    <artifactId>debezium-binlog-lock-interceptor</artifactId>
    <version>3.4.0-SNAPSHOT</version>
</dependency>
```

## Step 2: Add JVM Flag

Add this to your JVM arguments (Maven, IDE, or command line):

```bash
-Djdk.attach.allowAttachSelf=true
```

## Step 3: Initialize in Your Code

Add this **BEFORE** creating Debezium Engine:

```java
import io.debezium.binlog.lock.BinlogLockInterceptor;
import io.debezium.binlog.lock.GlobalLockManager;

public class MyApp {
    public static void main(String[] args) {
        // STEP 1: Initialize the interceptor FIRST
        GlobalLockManager.GlobalLockConfig config =
            GlobalLockManager.GlobalLockConfig.builder()
                .enabled(true)
                .lockName("my_app_binlog_lock")
                .lockTimeoutSeconds(30)
                .build();

        BinlogLockInterceptor.initialize(config);

        // STEP 2: Now create your Debezium Engine as usual
        Properties props = new Properties();
        // ... your Debezium configuration

        DebeziumEngine engine = DebeziumEngine.create(...)
            .using(props)
            .notifying(record -> {
                // Process records
            })
            .build();

        // Run the engine
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        // ... rest of your application
    }
}
```

## Step 4: Run Your Application

### With Maven:
```bash
mvn exec:java \
    -Dexec.mainClass="com.example.MyApp" \
    -Djdk.attach.allowAttachSelf=true
```

### With JAR:
```bash
java -Djdk.attach.allowAttachSelf=true \
     -jar my-app.jar
```

### With IDE (IntelliJ/Eclipse):
Add VM options:
```
-Djdk.attach.allowAttachSelf=true
```

## What Happens

1. When your app starts, `BinlogLockInterceptor.initialize()` transforms the BinaryLogClient class
2. When Debezium Engine starts and BinaryLogClient tries to connect:
   - It first acquires MySQL lock: `SELECT GET_LOCK('my_app_binlog_lock', 30)`
   - If successful, proceeds with binlog streaming
   - If timeout (another instance holds lock), throws exception and fails to start
3. When your app stops and BinaryLogClient disconnects:
   - It releases the lock: `SELECT RELEASE_LOCK('my_app_binlog_lock')`

## Testing with Multiple Instances

### Terminal 1:
```bash
java -Djdk.attach.allowAttachSelf=true -jar my-app.jar
```
Output: `Successfully acquired global lock 'my_app_binlog_lock'`

### Terminal 2 (while Terminal 1 is running):
```bash
java -Djdk.attach.allowAttachSelf=true -jar my-app.jar
```
Output: `Failed to acquire global lock 'my_app_binlog_lock' within 30 seconds`

Only ONE instance can stream at a time!

## Configuration Options

```java
GlobalLockConfig config = GlobalLockConfig.builder()
    .enabled(true)                  // Turn on/off
    .lockName("custom_lock_name")   // Unique lock name
    .lockTimeoutSeconds(60)         // How long to wait
    .sslMode("REQUIRED")            // MySQL SSL mode
    .build();
```

## Troubleshooting

### "Cannot attach to current VM"
→ Add JVM flag: `-Djdk.attach.allowAttachSelf=true`

### "BinaryLogClient class not found"
→ Ensure `mysql-binlog-connector-java` is on classpath

### "Lock timeout"
→ Another instance is running. Wait for it to release the lock, or use a different lock name

### No visible effect
→ Call `initialize()` BEFORE any Debezium code runs

## Next Steps

- Read [README.md](README.md) for comprehensive documentation
- See advanced examples in the README
- Integrate with Spring Boot, Kubernetes, Docker

## Complete Example

```java
package com.example.debezium;

import io.debezium.binlog.lock.BinlogLockInterceptor;
import io.debezium.binlog.lock.GlobalLockManager;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebeziumApp {
    public static void main(String[] args) throws Exception {
        // Initialize lock interceptor
        GlobalLockManager.GlobalLockConfig config =
            GlobalLockManager.GlobalLockConfig.builder()
                .enabled(true)
                .lockName("debezium_app_lock")
                .lockTimeoutSeconds(30)
                .build();
        BinlogLockInterceptor.initialize(config);

        // Configure Debezium Engine
        Properties props = new Properties();
        props.setProperty("name", "my-engine");
        props.setProperty("connector.class",
            "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage",
            "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
        props.setProperty("database.hostname", "localhost");
        props.setProperty("database.port", "3306");
        props.setProperty("database.user", "debezium");
        props.setProperty("database.password", "dbz");
        props.setProperty("database.server.id", "184054");
        props.setProperty("topic.prefix", "my-app");
        props.setProperty("database.include.list", "inventory");
        props.setProperty("schema.history.internal",
            "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename",
            "/tmp/dbhistory.dat");

        // Create engine
        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine =
            DebeziumEngine.create(ChangeEventFormat.of(
                io.debezium.engine.format.Json.class))
                .using(props)
                .notifying(record -> {
                    System.out.println("Change: " + record);
                })
                .build();

        // Run engine
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                engine.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            executor.shutdown();
        }));

        Thread.currentThread().join();
    }
}
```

Run with:
```bash
mvn exec:java \
    -Dexec.mainClass="com.example.debezium.DebeziumApp" \
    -Djdk.attach.allowAttachSelf=true
```

That's it! You now have multi-instance coordination with MySQL locks.
