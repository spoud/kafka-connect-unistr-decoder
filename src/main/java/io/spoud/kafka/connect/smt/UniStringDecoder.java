package io.spoud.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class UniStringDecoder<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Decode unistr fields";
    public static final Pattern PATTERN = Pattern.compile("\\\\([0-9A-Fa-f]{4})");

    private interface ConfigName {
        String UNISTR_FIELDS = "unistr.fields";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.UNISTR_FIELDS, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Field name for UUID");

    private static final String PURPOSE = "Decoding UNISTR fields";

    private List<String> fieldNames;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);

        fieldNames = config.getList(ConfigName.UNISTR_FIELDS);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }


    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
        final Map<String, Object> updatedValue = new HashMap<>(value);

        for (String fieldName : fieldNames) {
            if (updatedValue.containsKey(fieldName)) {
                String possiblyEncodedValue = updatedValue.get(fieldName).toString();
                if (possiblyEncodedValue.startsWith("UNISTR")) {
                    String encoded = possiblyEncodedValue.substring(8, possiblyEncodedValue.length() - 2);
                    updatedValue.put(fieldName, decodeUnicode(encoded));
                }
            }
        }

        return newRecord(record, null, updatedValue);
    }


    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            if (fieldNames.contains(field.name())) {
                if (field.schema().type() == Schema.Type.STRING) {
                    String possiblyEncodedValue = value.getString(field.name());
                    if (possiblyEncodedValue != null && possiblyEncodedValue.startsWith("UNISTR")) {
                        String encoded = possiblyEncodedValue.substring(8, possiblyEncodedValue.length() - 2);
                        updatedValue.put(field.name(), decodeUnicode(encoded));
                    }
                }
            } else {
                updatedValue.put(field.name(), value.get(field));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }


    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        schema.fields().forEach(f -> builder.field(f.name(), f.schema()));
        return builder.build();
    }

    private String decodeUnicode(String encoded) {
        Matcher matcher = PATTERN.matcher(encoded);
        StringBuilder result = new StringBuilder();

        while (matcher.find()) {
            String hexCode = matcher.group(1);
            int codePoint = Integer.parseInt(hexCode, 16);
            String unicodeChar = new String(Character.toChars(codePoint));
            matcher.appendReplacement(result, unicodeChar);
        }
        matcher.appendTail(result);

        return result.toString();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends UniStringDecoder<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    updatedSchema, updatedValue,
                    record.valueSchema(), record.value(),
                    record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends UniStringDecoder<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(), record.kafkaPartition(),
                    record.keySchema(), record.key(),
                    updatedSchema, updatedValue,
                    record.timestamp());
        }
    }
}