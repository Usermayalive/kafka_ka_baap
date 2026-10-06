package com.simplekafka.schema;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * PII tagging convention in Avro schemas:
 * <pre>
 * {"type":"record","name":"User","pii.entity":"userId","fields":[
 *   {"name":"userId","type":"string"},
 *   {"name":"email","type":"string","pii":true},
 *   {"name":"phone","type":["null","string"],"pii":true}]}
 * </pre>
 * Fields with {@code "pii": true} are encrypted under the key of the entity named by the
 * record's {@code pii.entity} field. PII fields must be strings (ciphertext is stored in the
 * same field so the record stays valid under its schema).
 */
public final class PiiSchemaRules {
    public static final String PII = "pii";
    public static final String ENTITY = "pii.entity";

    private PiiSchemaRules() {
    }

    public static List<String> piiFields(Schema schema) {
        List<String> fields = new ArrayList<>();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field f : schema.getFields()) {
                if (Boolean.TRUE.equals(f.getObjectProp(PII))) {
                    fields.add(f.name());
                }
            }
        }
        return fields;
    }

    public static String entityField(Schema schema) {
        return schema.getType() == Schema.Type.RECORD ? schema.getProp(ENTITY) : null;
    }

    /** @return why the schema's PII tagging is invalid, or null if it's valid (or untagged) */
    public static String violation(Schema schema) {
        List<String> pii = piiFields(schema);
        if (pii.isEmpty()) {
            return null;
        }
        String entity = entityField(schema);
        if (entity == null || schema.getField(entity) == null) {
            return "Schema has PII fields " + pii + " but no valid \"" + ENTITY + "\" naming the entity ID field";
        }
        if (schema.getField(entity).schema().getType() != Schema.Type.STRING) {
            return "Entity ID field '" + entity + "' must be a non-null string";
        }
        if (pii.contains(entity)) {
            return "Entity ID field '" + entity + "' cannot itself be PII-encrypted (it selects the key)";
        }
        for (String name : pii) {
            if (!isStringOrNullableString(schema.getField(name).schema())) {
                return "PII field '" + name + "' must be a string or [\"null\",\"string\"]";
            }
        }
        return null;
    }

    private static boolean isStringOrNullableString(Schema s) {
        if (s.getType() == Schema.Type.STRING) {
            return true;
        }
        if (s.getType() == Schema.Type.UNION) {
            return s.getTypes().stream().allMatch(t -> t.getType() == Schema.Type.STRING || t.getType() == Schema.Type.NULL);
        }
        return false;
    }
}
