# Debezium BinaryLogClient Lock Rewriter

A **build-time** ByteBuddy transformation of BinaryLogClient that adds MySQL `GET_LOCK()` coordination for multi-instance deployments.

## Key Features

✅ **NO Java agent required**
✅ **NO JVM flags required**
✅ **NO command-line arguments**
✅ **NO source code modification**
✅ **NO application code changes**

Just add the dependency and configure via system properties (in your application.properties or similar).

## How It Works

This module uses ByteBuddy's **Maven Plugin** to transform `BinaryLogClient` at **build time**:

1. During Maven build, `byte-buddy-maven-plugin` runs
2. It transforms `com.github.shyiko.mysql.binlog.BinaryLogClient`
3. Injects lock acquisition into `connect()` methods
4. Injects lock release into `disconnect()` method
5. Transformed class is packaged into the JAR
6. When you add this dependency **before** `mysql-binlog-connector-java`, the transformed version loads first

```
Your Application Classpath:
┌────────────────────────────────────────────────────┐
│ 1. debezium-binlog-lock-rewriter.jar              │  ← Contains transformed BinaryLogClient
│    └── com/github/shyiko/mysql/binlog/            │     with lock interceptors
│        BinaryLogClient.class (transformed)         │
├────────────────────────────────────────────────────┤
│ 2. mysql-binlog-connector-java.jar                │  ← Original (ignored because class
│    └── com/github/shyiko/mysql/binlog/            │     already loaded from #1)
│        BinaryLogClient.class (original)            │
└────────────────────────────────────────────────────┘

When Debezium does:  new BinaryLogClient(...)
Java loads from #1 (transformed version with locks)
```

## Usage

### Step 1: Add Dependency (BEFORE mysql-binlog-connector-java)

**CRITICAL**: This dependency MUST appear BEFORE `mysql-binlog-connector-java` or `debezium-connector-mysql` in your POM:

```xml
<dependencies>
    <!-- ADD THIS FIRST -->
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-binlog-lock-rewriter</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>

    <!-- Then other Debezium dependencies -->
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-mysql</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>

    <!-- Rest of your dependencies -->
</dependencies>
```

### Step 2: Configure via System Properties

Set these properties in your application (NOT on command line):

**Option A: application.properties (Spring Boot)**
```properties
debezium.binlog.lock.enabled=true
debezium.binlog.lock.name=my_app_binlog_lock
debezium.binlog.lock.timeout.seconds=30
```

**Option B: System.setProperty() in Code**
```java
public class MyApp {
    public static void main(String[] args) {
        // Configure lock BEFORE creating Debezium Engine
        System.setProperty("debezium.binlog.lock.enabled", "true");
        System.setProperty("debezium.binlog.lock.name", "my_app_lock");
        System.setProperty("debezium.binlog.lock.timeout.seconds", "30");

        // Now create Debezium Engine as normal
        DebeziumEngine engine = ...
    }
}
```

**Option C: Environment Variables** (converted to system properties)
```bash
# In your Docker/K8s environment
DEBEZIUM_BINLOG_LOCK_ENABLED=true
DEBEZIUM_BINLOG_LOCK_NAME=my_app_lock
DEBEZIUM_BINLOG_LOCK_TIMEOUT_SECONDS=30
```

Then in your application:
```java
static {
    String enabled = System.getenv("DEBEZIUM_BINLOG_LOCK_ENABLED");
    if (enabled != null) {
        System.setProperty("debezium.binlog.lock.enabled", enabled);
    }
    // ... same for other properties
}
```

### Step 3: Run Your Application Normally

```bash
# No special flags needed!
mvn exec:java

# Or
java -jar my-app.jar

# Or
./gradlew run
```

That's it! No infrastructure changes required.

## Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `debezium.binlog.lock.enabled` | `false` | Enable/disable global lock |
| `debezium.binlog.lock.name` | `debezium_binlog_lock` | MySQL lock name |
| `debezium.binlog.lock.timeout.seconds` | `30` | Lock acquisition timeout |

## Complete Example

```java
package com.example.app;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DebeziumApp {

    public static void main(String[] args) throws Exception {
        // Configure lock via system properties
        System.setProperty("debezium.binlog.lock.enabled", "true");
        System.setProperty("debezium.binlog.lock.name", "my_app_binlog_lock");
        System.setProperty("debezium.binlog.lock.timeout.seconds", "30");

        // Configure Debezium Engine (normal setup)
        Properties props = new Properties();
        props.setProperty("name", "my-engine");
        props.setProperty("connector.class", "io.debezium.connector.mysql.MySqlConnector");
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
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

        // Create and run engine
        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine =
            DebeziumEngine.create(ChangeEventFormat.of(
                io.debezium.engine.format.Json.class))
                .using(props)
                .notifying(record -> {
                    System.out.println("Received: " + record);
                })
                .build();

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

**pom.xml:**
```xml
<dependencies>
    <!-- FIRST: Lock rewriter with transformed BinaryLogClient -->
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-binlog-lock-rewriter</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>

    <!-- THEN: Debezium dependencies -->
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-embedded</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-connector-mysql</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>

    <!-- JSON format support -->
    <dependency>
        <groupId>io.debezium</groupId>
        <artifactId>debezium-engine-format-json</artifactId>
        <version>3.4.0-SNAPSHOT</version>
    </dependency>
</dependencies>
```

Run with:
```bash
mvn exec:java -Dexec.mainClass="com.example.app.DebeziumApp"
```

## Multi-Instance Behavior

### Instance 1 Starts
```log
[INFO] GlobalLockManager - GlobalLockManager initialized: lockName=my_app_binlog_lock, timeoutSeconds=30
[INFO] GlobalLockManager - Acquiring global lock 'my_app_binlog_lock' for localhost:3306
[INFO] GlobalLockManager - Successfully acquired global lock 'my_app_binlog_lock'
[INFO] BinaryLogClient - Connected to localhost:3306
```

### Instance 2 Tries to Start (while Instance 1 running)
```log
[INFO] GlobalLockManager - GlobalLockManager initialized: lockName=my_app_binlog_lock, timeoutSeconds=30
[INFO] GlobalLockManager - Acquiring global lock 'my_app_binlog_lock' for localhost:3306
[INFO] GlobalLockManager - Waiting for lock...
... 30 seconds later ...
[ERROR] GlobalLockManager - Failed to acquire global lock 'my_app_binlog_lock' within 30 seconds
Exception: RuntimeException: Failed to acquire global lock before binlog streaming
```

### Instance 1 Stops
```log
[INFO] BinaryLogClient - Disconnecting...
[INFO] GlobalLockManager - Releasing global lock 'my_app_binlog_lock' for localhost:3306
[INFO] GlobalLockManager - Successfully released global lock 'my_app_binlog_lock'
```

### Now Instance 2 Can Start
```log
[INFO] GlobalLockManager - Acquiring global lock 'my_app_binlog_lock' for localhost:3306
[INFO] GlobalLockManager - Successfully acquired global lock 'my_app_binlog_lock'
```

## Verifying the Transformation

To verify that BinaryLogClient was transformed correctly:

```bash
# Build the module
mvn clean package

# Extract and decompile the transformed class
cd target/classes
javap -c com.github.shyiko.mysql.binlog.BinaryLogClient | grep -A 5 "connect"

# You should see calls to GlobalLockManager.getInstance()
```

## Troubleshooting

### Issue: Lock never acquired

**Check classpath ordering:**
```bash
mvn dependency:tree

# Ensure debezium-binlog-lock-rewriter appears BEFORE mysql-binlog-connector-java
```

**Solution:**
Move `debezium-binlog-lock-rewriter` dependency to the TOP of your `<dependencies>` section.

### Issue: Properties not read

**Problem:**
```java
System.setProperty("debezium.binlog.lock.enabled", "true");
```
Called AFTER BinaryLogClient was already loaded.

**Solution:**
Set properties in a `static {}` block or at the very start of `main()`.

### Issue: Lock timeout

**Diagnosis:**
```sql
-- Check if lock is held
SELECT IS_USED_LOCK('my_app_binlog_lock') as thread_id;

-- If returns a number, that thread holds the lock
-- If returns NULL, lock is free
```

**Solutions:**
1. Wait for other instance to release lock
2. Increase timeout: `debezium.binlog.lock.timeout.seconds=60`
3. Use different lock name per instance
4. Force release: `SELECT RELEASE_LOCK('my_app_binlog_lock');`

### Issue: Two instances both acquire lock

**Cause:** Using different lock names or lock disabled.

**Check:**
```java
// Ensure both instances use SAME lock name
System.setProperty("debezium.binlog.lock.name", "SAME_NAME_HERE");

// Ensure lock is enabled
System.setProperty("debezium.binlog.lock.enabled", "true");
```

## Spring Boot Integration

**application.yml:**
```yaml
debezium:
  binlog:
    lock:
      enabled: true
      name: ${LOCK_NAME:my_app_binlog_lock}
      timeout:
        seconds: ${LOCK_TIMEOUT:30}
```

**Configuration class:**
```java
@Configuration
public class DebeziumConfig {

    @Value("${debezium.binlog.lock.enabled:false}")
    private boolean lockEnabled;

    @Value("${debezium.binlog.lock.name:debezium_binlog_lock}")
    private String lockName;

    @Value("${debezium.binlog.lock.timeout.seconds:30}")
    private int lockTimeout;

    @PostConstruct
    public void configureLock() {
        System.setProperty("debezium.binlog.lock.enabled", String.valueOf(lockEnabled));
        System.setProperty("debezium.binlog.lock.name", lockName);
        System.setProperty("debezium.binlog.lock.timeout.seconds", String.valueOf(lockTimeout));
    }

    @Bean
    public DebeziumEngine<?> debeziumEngine() {
        // ... normal configuration
    }
}
```

## Docker Deployment

**Dockerfile:**
```dockerfile
FROM openjdk:17-slim

COPY target/my-app.jar /app/app.jar

# No special JVM flags needed!
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

**docker-compose.yml:**
```yaml
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: inventory
    ports:
      - "3306:3306"

  debezium-instance-1:
    build: .
    depends_on:
      - mysql
    environment:
      # Configure via env vars, convert to system properties in your app
      LOCK_ENABLED: "true"
      LOCK_NAME: "production_binlog_lock"
      LOCK_TIMEOUT: "30"

  debezium-instance-2:
    build: .
    depends_on:
      - mysql
    environment:
      LOCK_ENABLED: "true"
      LOCK_NAME: "production_binlog_lock"  # Same lock - only one will run
      LOCK_TIMEOUT: "30"
    restart: on-failure  # Will retry when instance-1 releases lock
```

## Kubernetes Deployment

**deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: debezium-app
spec:
  replicas: 2  # Only one will acquire lock and run
  selector:
    matchLabels:
      app: debezium
  template:
    metadata:
      labels:
        app: debezium
    spec:
      containers:
      - name: app
        image: my-debezium-app:latest
        env:
        - name: LOCK_ENABLED
          value: "true"
        - name: LOCK_NAME
          value: "k8s_debezium_lock"
        - name: LOCK_TIMEOUT
          value: "120"
```

## MySQL Lock Details

### GET_LOCK() Behavior

- **Named lock** - string identifier
- **Connection-scoped** - auto-released if connection drops
- **Single-holder** - only one connection can hold a lock at a time
- **Timeout** - waits up to N seconds, then returns 0

```sql
-- Acquire lock
SELECT GET_LOCK('lock_name', 30);
-- Returns:
--   1 = success
--   0 = timeout
--   NULL = error

-- Check who holds lock
SELECT IS_USED_LOCK('lock_name');
-- Returns:
--   thread_id = held by this thread
--   NULL = free

-- Release lock
SELECT RELEASE_LOCK('lock_name');
-- Returns:
--   1 = released
--   0 = not held by this thread
--   NULL = doesn't exist
```

### Monitoring Locks

```sql
-- MySQL 8.0+
SELECT OBJECT_NAME, LOCK_TYPE, OWNER_THREAD_ID, PROCESSLIST_ID
FROM performance_schema.metadata_locks
WHERE OBJECT_TYPE = 'USER LEVEL LOCK';

-- See what the thread is doing
SELECT t.*, p.*
FROM performance_schema.threads t
JOIN information_schema.PROCESSLIST p ON t.PROCESSLIST_ID = p.ID
WHERE t.THREAD_ID = (SELECT IS_USED_LOCK('my_lock'));
```

## Advantages Over Other Approaches

| Approach | Requires Agent | Requires JVM Flags | Requires Source Modification | Infrastructure Approval |
|----------|----------------|-------------------|------------------------------|------------------------|
| Java Agent | ✅ Yes | ✅ Yes (-javaagent) | ❌ No | ⚠️ Difficult |
| Self-Attach | ❌ No | ✅ Yes (-Djdk.attach...) | ❌ No | ⚠️ Difficult |
| **Build-Time Transform (THIS)** | ❌ No | ❌ No | ❌ No | ✅ Easy |
| Fork Library | ❌ No | ❌ No | ✅ Yes | ⚠️ Maintenance burden |

## Technical Details

### ByteBuddy Maven Plugin

The transformation happens during Maven's `process-classes` phase:

1. `maven-dependency-plugin` unpacks `BinaryLogClient.class` from `mysql-binlog-connector-java`
2. `byte-buddy-maven-plugin` loads `BinlogLockPlugin`
3. Plugin matches `com.github.shyiko.mysql.binlog.BinaryLogClient`
4. Plugin applies transformations:
   - `connect()` → wrapped with lock acquisition
   - `connect(long)` → wrapped with lock acquisition
   - `disconnect()` → wrapped with lock release
5. Transformed class written to `target/classes`
6. JAR包 includes transformed class

### Classpath Precedence

Java ClassLoader searches in order:
1. Bootstrap classpath (JDK classes)
2. Extension classpath
3. **Application classpath** (your dependencies)

Within application classpath, **first occurrence wins**:
```
debezium-binlog-lock-rewriter.jar    ← Loaded from here
└── com/.../BinaryLogClient.class (transformed)

mysql-binlog-connector-java.jar      ← Ignored (class already loaded)
└── com/.../BinaryLogClient.class (original)
```

### Why This Works

- Maven dependency order determines classpath order
- First dependency listed = first on classpath
- ClassLoader finds transformed version first
- Original class never loaded

## License

Apache Software License 2.0 (same as Debezium)

## When to Use This

✅ **Use this when:**
- Multiple Debezium Engine instances, only one should stream
- High-availability deployment with active/standby
- Cannot modify infrastructure (no Java agents or JVM flags allowed)
- Need MySQL-level coordination
- Want zero code changes to your application

❌ **Don't use this when:**
- Single instance deployment
- Using Kafka Connect (use Connect's distributed mode)
- Can use external coordination (ZooKeeper, Kubernetes leader election)
- Don't have MySQL
