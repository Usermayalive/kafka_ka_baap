package com.simplekafka.schema;

import com.simplekafka.gdpr.CryptoShredder;
import com.simplekafka.metadata.MetadataImage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Broker-side schema enforcement on the produce path, plus PII field encryption (produce) and
 * decryption (fetch). Runs entirely in memory: registered schemas come from the replicated
 * metadata image and are parsed once into a local cache, invalidated when the Raft state
 * machine applies a schema change.
 *
 * Topics without a registered schema are untouched.
 */
public class SchemaService {
    private static final Logger LOGGER = Logger.getLogger(SchemaService.class.getName());

    private final Supplier<MetadataImage> image;
    private volatile CryptoShredder shredder;
    private final Map<Integer, Schema> parsed = new ConcurrentHashMap<>();
    private final Map<String, Set<Integer>> topicSchemaIds = new ConcurrentHashMap<>();

    public SchemaService(Supplier<MetadataImage> image) {
        this.image = image;
    }

    public void setShredder(CryptoShredder shredder) {
        this.shredder = shredder;
    }

    public CryptoShredder shredder() {
        return shredder;
    }

    /** Called when the metadata quorum applies a schema change. */
    public void invalidate() {
        topicSchemaIds.clear();
    }

    private Set<Integer> idsFor(String topic) {
        return topicSchemaIds.computeIfAbsent(topic, t -> {
            Set<Integer> ids = ConcurrentHashMap.newKeySet();
            image.get().schemas(t).forEach(v -> ids.add(v.id()));
            return ids;
        });
    }

    private Schema schema(int id) {
        return parsed.computeIfAbsent(id, i -> {
            MetadataImage.SchemaVersion v = image.get().schemaById(i);
            return v == null ? null : new Schema.Parser().parse(v.schema());
        });
    }

    public boolean hasSchema(String topic) {
        return !idsFor(topic).isEmpty();
    }

    /** Validates the record and encrypts PII fields; returns the bytes to store. */
    public byte[] onProduce(String topic, byte[] record) throws SchemaViolationException, IOException {
        Set<Integer> ids = idsFor(topic);
        if (ids.isEmpty()) {
            return record;
        }
        SchemaWireFormat.Framed framed = SchemaWireFormat.parse(record);
        if (!ids.contains(framed.schemaId())) {
            throw new SchemaViolationException("Schema id " + framed.schemaId() + " is not registered for topic '"
                    + topic + "' (registered: " + ids + ")");
        }
        Schema schema = schema(framed.schemaId());
        GenericRecord value = decode(schema, framed.payload(), framed.schemaId());

        List<String> pii = PiiSchemaRules.piiFields(schema);
        if (pii.isEmpty()) {
            return record;
        }
        CryptoShredder s = shredder;
        if (s == null) {
            throw new SchemaViolationException("Topic '" + topic + "' has PII fields " + pii
                    + " but this broker has no master key provider configured");
        }
        String entityId = String.valueOf(value.get(PiiSchemaRules.entityField(schema)));
        for (String field : pii) {
            Object plaintext = value.get(field);
            if (plaintext != null) {
                value.put(field, s.encrypt(entityId, field, plaintext.toString()));
            }
        }
        return SchemaWireFormat.frame(framed.schemaId(), encode(schema, value));
    }

    /** Decrypts PII fields (or redacts them for forgotten entities); other records pass through. */
    public byte[] onFetch(String topic, byte[] record) {
        CryptoShredder s = shredder;
        if (s == null || !SchemaWireFormat.isFramed(record) || !hasSchema(topic)) {
            return record;
        }
        try {
            SchemaWireFormat.Framed framed = SchemaWireFormat.parse(record);
            Schema schema = schema(framed.schemaId());
            List<String> pii = schema == null ? List.of() : PiiSchemaRules.piiFields(schema);
            if (pii.isEmpty()) {
                return record;
            }
            GenericRecord value = decode(schema, framed.payload(), framed.schemaId());
            String entityId = String.valueOf(value.get(PiiSchemaRules.entityField(schema)));
            for (String field : pii) {
                Object stored = value.get(field);
                if (stored != null) {
                    value.put(field, s.decrypt(entityId, field, stored.toString()));
                }
            }
            return SchemaWireFormat.frame(framed.schemaId(), encode(schema, value));
        } catch (SchemaViolationException | IOException | RuntimeException e) {
            LOGGER.log(Level.WARNING, "Could not decrypt record on " + topic + "; returning it as stored", e);
            return record;
        }
    }

    private static GenericRecord decode(Schema schema, byte[] payload, int schemaId) throws SchemaViolationException {
        if (schema == null) {
            throw new SchemaViolationException("Unknown schema id " + schemaId);
        }
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            GenericRecord record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
            if (!decoder.isEnd()) {
                throw new SchemaViolationException("Payload has trailing bytes after a complete " + schema.getFullName()
                        + " (schema id " + schemaId + ")");
            }
            return record;
        } catch (IOException | AvroRuntimeException | ArrayIndexOutOfBoundsException | NegativeArraySizeException e) {
            throw new SchemaViolationException("Payload does not match schema id " + schemaId + " ("
                    + schema.getFullName() + "): " + e);
        }
    }

    static byte[] encode(Schema schema, GenericRecord value) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(schema, GenericData.get()).write(value, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
