package com.simplekafka.schema;

import com.simplekafka.broker.BrokerInfo;
import com.simplekafka.broker.Partition;
import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.client.BrokerErrorException;
import com.simplekafka.client.SimpleKafkaClient;
import com.simplekafka.gdpr.CryptoShredder;
import com.simplekafka.gdpr.EnvMasterKeyProvider;
import com.simplekafka.network.KafkaFrame;
import com.simplekafka.network.NettyBrokerServer;
import com.simplekafka.observability.AuditLog;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SchemaAndGdprTest {

    private static final String ORDER_V1 = """
            {"type":"record","name":"Order","fields":[
              {"name":"id","type":"string"},
              {"name":"amount","type":"long"}]}""";
    // Compatible evolution: new field with a default
    private static final String ORDER_V2 = """
            {"type":"record","name":"Order","fields":[
              {"name":"id","type":"string"},
              {"name":"amount","type":"long"},
              {"name":"currency","type":"string","default":"EUR"}]}""";
    // Breaking under BACKWARD: new required field without a default
    private static final String ORDER_BREAKING = """
            {"type":"record","name":"Order","fields":[
              {"name":"id","type":"string"},
              {"name":"amount","type":"long"},
              {"name":"customer","type":"string"}]}""";
    private static final String USER_PII = """
            {"type":"record","name":"User","pii.entity":"userId","fields":[
              {"name":"userId","type":"string"},
              {"name":"email","type":"string","pii":true},
              {"name":"phone","type":["null","string"],"default":null,"pii":true},
              {"name":"country","type":"string"}]}""";

    @TempDir
    File tempDir;

    private SimpleKafkaBroker broker;
    private NettyBrokerServer server;
    private SimpleKafkaClient client;
    private AvroSerde serde;

    @BeforeEach
    void setup() throws Exception {
        int[] port = new int[1];
        broker = new SimpleKafkaBroker(1, "127.0.0.1", 0, 0) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override public List<BrokerInfo> liveBrokers() {
                return List.of(new BrokerInfo(1, "127.0.0.1", port[0]));
            }
            @Override protected String dataDir() {
                return tempDir.getAbsolutePath();
            }
        };
        for (String topic : List.of("orders", "users", "plain")) {
            broker.getTopics().put(topic, new ArrayList<>(List.of(new Partition(0, new File(tempDir, topic).getAbsolutePath()))));
        }
        server = new NettyBrokerServer("127.0.0.1", 0, broker);
        server.start();
        port[0] = server.getPort();
        client = new SimpleKafkaClient("127.0.0.1", server.getPort());
        client.initialize();
        serde = new AvroSerde(client::getSchema);
    }

    @AfterEach
    void teardown() {
        client.close();
        server.stop();
        if (broker.getSchemaService().shredder() != null) {
            broker.getSchemaService().shredder().close();
        }
    }

    private static GenericRecord order(String schemaJson, String id, long amount) {
        GenericRecord r = new GenericData.Record(new Schema.Parser().parse(schemaJson));
        r.put("id", id);
        r.put("amount", amount);
        return r;
    }

    private static short errorOf(Executable action) {
        BrokerErrorException e = assertThrows(BrokerErrorException.class, action::run);
        return e.getErrorCode();
    }

    interface Executable {
        void run() throws Exception;
    }

    // ---- 5.1 / 5.2: wire format and validation ----

    @Test
    void testValidFramedRecordRoundTrips() throws Exception {
        int id = client.registerSchema("orders", ORDER_V1);
        Schema v1 = new Schema.Parser().parse(ORDER_V1);
        client.send("orders", 0, AvroSerde.serialize(id, v1, order(ORDER_V1, "o-1", 42)));

        GenericRecord back = serde.deserialize(client.fetch("orders", 0, 0, 1 << 20).get(0));
        assertEquals("o-1", back.get("id").toString());
        assertEquals(42L, back.get("amount"));
    }

    @Test
    void testInvalidRecordsAreRejectedWithReasons() throws Exception {
        int id = client.registerSchema("orders", ORDER_V1);

        BrokerErrorException unframed = assertThrows(BrokerErrorException.class,
                () -> client.send("orders", 0, "{\"id\":\"o-1\"}".getBytes(StandardCharsets.UTF_8)));
        assertEquals(KafkaFrame.ERROR_SCHEMA_VIOLATION, unframed.getErrorCode());
        assertTrue(unframed.getMessage().contains("Missing schema framing"), unframed.getMessage());

        BrokerErrorException garbage = assertThrows(BrokerErrorException.class,
                () -> client.send("orders", 0, SchemaWireFormat.frame(id, new byte[]{1, 2, 3})));
        assertTrue(garbage.getMessage().contains("does not match schema id " + id), garbage.getMessage());

        BrokerErrorException unknownId = assertThrows(BrokerErrorException.class,
                () -> client.send("orders", 0, SchemaWireFormat.frame(999, new byte[]{0})));
        assertTrue(unknownId.getMessage().contains("not registered for topic 'orders'"), unknownId.getMessage());

        assertEquals(0, broker.hostedPartition("orders", 0).getNextOffset(), "Nothing invalid may reach the log");
        client.send("plain", 0, "anything".getBytes(StandardCharsets.UTF_8)); // topics without a schema are unchecked
    }

    @Test
    void testBackwardCompatibilityRejectsBreakingChangeWithReason() throws Exception {
        int v1 = client.registerSchema("orders", ORDER_V1);
        BrokerErrorException breaking = assertThrows(BrokerErrorException.class,
                () -> client.registerSchema("orders", ORDER_BREAKING));
        assertEquals(KafkaFrame.ERROR_INCOMPATIBLE_SCHEMA, breaking.getErrorCode());
        assertTrue(breaking.getMessage().contains("BACKWARD") && breaking.getMessage().contains("customer"),
                breaking.getMessage());

        // A producer using the unregistered breaking schema is rejected at produce time too
        Schema breakingSchema = new Schema.Parser().parse(ORDER_BREAKING);
        GenericRecord r = new GenericData.Record(breakingSchema);
        r.put("id", "o-2");
        r.put("amount", 1L);
        r.put("customer", "c");
        assertEquals(KafkaFrame.ERROR_SCHEMA_VIOLATION,
                errorOf(() -> client.send("orders", 0, AvroSerde.serialize(v1 + 100, breakingSchema, r))));

        int v2 = client.registerSchema("orders", ORDER_V2);
        assertNotEquals(v1, v2, "Compatible evolution registers a new version");
        assertEquals(v2, client.registerSchema("orders", ORDER_V2), "Re-registering is idempotent");
        // Old producers keep working after the evolution
        client.send("orders", 0, AvroSerde.serialize(v1, new Schema.Parser().parse(ORDER_V1), order(ORDER_V1, "o-3", 3)));
    }

    @Test
    void testForwardAndFullModes() throws Exception {
        client.setSchemaCompatibility("orders", "FORWARD");
        client.registerSchema("orders", ORDER_V2);
        // FORWARD: the latest (v2) must read data written with v1. v1 lacks 'currency', which has a default.
        client.registerSchema("orders", ORDER_V1);
        client.setSchemaCompatibility("orders", "FULL");
        BrokerErrorException full = assertThrows(BrokerErrorException.class,
                () -> client.registerSchema("orders", ORDER_BREAKING));
        assertTrue(full.getMessage().contains("FULL"), full.getMessage());
        assertEquals(KafkaFrame.ERROR_INCOMPATIBLE_SCHEMA,
                errorOf(() -> client.setSchemaCompatibility("orders", "SIDEWAYS")));
    }

    // ---- 5.3 / 5.5: crypto-shredding ----

    private int registerUsersWithShredding() throws Exception {
        byte[] masterKey = new byte[32];
        new SecureRandom().nextBytes(masterKey);
        broker.enableCryptoShredding(new EnvMasterKeyProvider(masterKey));
        return client.registerSchema("users", USER_PII);
    }

    private byte[] user(int schemaId, String userId, String email, String phone) throws Exception {
        Schema schema = new Schema.Parser().parse(USER_PII);
        GenericRecord r = new GenericData.Record(schema);
        r.put("userId", userId);
        r.put("email", email);
        r.put("phone", phone);
        r.put("country", "DE");
        return AvroSerde.serialize(schemaId, schema, r);
    }

    private List<GenericRecord> fetchUsers() throws Exception {
        List<GenericRecord> users = new ArrayList<>();
        for (byte[] b : client.fetch("users", 0, 0, 1 << 20)) {
            users.add(serde.deserialize(b));
        }
        return users;
    }

    @Test
    void testPiiIsEncryptedInTheLogAndDecryptedTransparently() throws Exception {
        int id = registerUsersWithShredding();
        client.send("users", 0, user(id, "u-123", "alice@example.com", "+49 30 1234"));

        String stored = new String(broker.hostedPartition("users", 0).readRecords(0, 1 << 20).get(0).payload(),
                StandardCharsets.ISO_8859_1);
        assertFalse(stored.contains("alice@example.com"), "PII must not be stored in plaintext");
        assertFalse(stored.contains("+49 30 1234"));
        assertTrue(stored.contains(CryptoShredder.PREFIX));
        assertTrue(stored.contains("DE"), "Non-PII fields stay in plaintext");

        GenericRecord fetched = fetchUsers().get(0);
        assertEquals("alice@example.com", fetched.get("email").toString());
        assertEquals("+49 30 1234", fetched.get("phone").toString());
    }

    @Test
    void testForgetEntityRedactsOnlyThatEntityAndIsAudited() throws Exception {
        int id = registerUsersWithShredding();
        client.send("users", 0, user(id, "u-123", "alice@example.com", null));
        client.send("users", 0, user(id, "u-456", "bob@example.com", "+1 555"));

        client.forgetEntity("u-123");

        List<GenericRecord> users = fetchUsers();
        assertEquals(CryptoShredder.REDACTED, users.get(0).get("email").toString());
        assertNull(users.get(0).get("phone"), "Absent PII stays absent");
        assertEquals("u-123", users.get(0).get("userId").toString(), "Entity ID itself is not PII-encrypted");
        assertEquals("bob@example.com", users.get(1).get("email").toString(), "Other entities are unaffected");

        // New data about a forgotten entity gets a new key; the old data stays erased
        client.send("users", 0, user(id, "u-123", "alice-new@example.com", null));
        users = fetchUsers();
        assertEquals(CryptoShredder.REDACTED, users.get(0).get("email").toString());
        assertEquals("alice-new@example.com", users.get(2).get("email").toString());

        List<String> audit = broker.hostedPartition(AuditLog.TOPIC, 0).readRecords(0, 1 << 20).stream()
                .map(r -> new String(r.payload(), StandardCharsets.UTF_8)).toList();
        assertTrue(audit.stream().anyMatch(e -> e.contains("ENTITY_FORGOTTEN") && e.contains("entitySha256")), audit.toString());
        assertTrue(audit.stream().noneMatch(e -> e.contains("u-123")), "The audit log must not re-record the identifier");
    }

    @Test
    void testErasureLearnedFromSnapshotOnlyIsEnforcedAndReconciled() throws Exception {
        byte[] masterKey = new byte[32];
        new SecureRandom().nextBytes(masterKey);
        broker.enableCryptoShredding(new EnvMasterKeyProvider(masterKey));
        int id = client.registerSchema("users", USER_PII);
        client.send("users", 0, user(id, "u-777", "carol@example.com", null));

        // The image learns of the erasure without the FORGET_ENTITY command being delivered to
        // this broker's listeners: what happens when it catches up from a Raft snapshot
        String key = CryptoShredder.entityKey("u-777");
        broker.metadataImage().apply(com.simplekafka.metadata.MetadataCommand.forgetEntity(key));
        assertEquals(CryptoShredder.REDACTED, fetchUsers().get(0).get("email").toString(),
                "Replicated erasure state is enforced on decrypt even before the local key is destroyed");

        broker.reconcileForgottenEntities();
        broker.getSchemaService().shredder().close();

        // A fresh broker on the same data whose image does NOT know about the erasure:
        // only the destroyed key keeps the data unreadable
        SimpleKafkaBroker fresh = new SimpleKafkaBroker(1, "127.0.0.1", 0, 0) {
            @Override public void start() {}
            @Override public void stop() {}
            @Override protected String dataDir() {
                return tempDir.getAbsolutePath();
            }
        };
        fresh.enableCryptoShredding(new EnvMasterKeyProvider(masterKey));
        fresh.applyMetadata(com.simplekafka.metadata.MetadataCommand.registerSchema("users", USER_PII));
        byte[] stored = broker.hostedPartition("users", 0).readRecords(0, 1 << 20).get(0).payload();
        GenericRecord viaFreshBroker = serde.deserialize(fresh.getSchemaService().onFetch("users", stored));
        assertEquals(CryptoShredder.REDACTED, viaFreshBroker.get("email").toString(), "Reconciliation destroyed the key");
        fresh.getSchemaService().shredder().close();
        broker.getSchemaService().setShredder(null);
    }

    @Test
    void testOnlyTheHashOfTheEntityIdIsReplicated() throws Exception {
        registerUsersWithShredding();
        client.forgetEntity("u-123");
        assertEquals(java.util.Map.of(CryptoShredder.entityKey("u-123"), 1), broker.metadataImage().erasures());
        assertFalse(broker.metadataImage().toJson().contains("u-123"), "Snapshots must not contain the raw entity ID");
    }

    @Test
    void testPiiSchemaRulesAndMissingKeyProvider() throws Exception {
        String noEntity = USER_PII.replace("\"pii.entity\":\"userId\",", "");
        BrokerErrorException invalid = assertThrows(BrokerErrorException.class, () -> client.registerSchema("users", noEntity));
        assertTrue(invalid.getMessage().contains("pii.entity"), invalid.getMessage());

        int id = client.registerSchema("users", USER_PII); // valid schema, but no master key configured
        BrokerErrorException noKeys = assertThrows(BrokerErrorException.class,
                () -> client.send("users", 0, user(id, "u-1", "x@y.z", null)));
        assertTrue(noKeys.getMessage().contains("no master key provider"), noKeys.getMessage());
    }
}
