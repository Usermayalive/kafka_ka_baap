package com.simplekafka.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Client-side Avro (de)serialization in the {@link SchemaWireFormat} framing. */
public class AvroSerde {

    @FunctionalInterface
    public interface SchemaLookup {
        String schemaJson(int schemaId) throws IOException;
    }

    private final SchemaLookup lookup;
    private final Map<Integer, Schema> cache = new ConcurrentHashMap<>();

    public AvroSerde(SchemaLookup lookup) {
        this.lookup = lookup;
    }

    public static byte[] serialize(int schemaId, Schema schema, GenericRecord record) throws IOException {
        return SchemaWireFormat.frame(schemaId, SchemaService.encode(schema, record));
    }

    /** Decodes with the writer schema named in the record's header (fetched once per ID). */
    public GenericRecord deserialize(byte[] framedRecord) throws IOException {
        SchemaWireFormat.Framed framed;
        try {
            framed = SchemaWireFormat.parse(framedRecord);
        } catch (SchemaViolationException e) {
            throw new IOException(e.getMessage(), e);
        }
        Schema writer = cache.get(framed.schemaId());
        if (writer == null) {
            writer = new Schema.Parser().parse(lookup.schemaJson(framed.schemaId()));
            cache.put(framed.schemaId(), writer);
        }
        return new GenericDatumReader<GenericRecord>(writer).read(null,
                DecoderFactory.get().binaryDecoder(framed.payload(), null));
    }
}
