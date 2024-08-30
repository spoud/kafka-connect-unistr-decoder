package io.spoud.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class UniStringDecoderTest {

    private final UniStringDecoder<SourceRecord> connectRecord = new UniStringDecoder.Value<>();

    @After
    public void tearDown() {
        connectRecord.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        connectRecord.configure(Collections.singletonMap("unistr.fields", "DESCRIPTION"));
        connectRecord.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test(expected = ConfigException.class)
    public void schemalessDecodeOneFieldWithDefaultConfig() {
        final Map<String, Object> props = new HashMap<>();
        connectRecord.configure(props);
    }

    @Test
    public void schemalessDecodeOneField() {
        final Map<String, Object> props = new HashMap<>();

        props.put("unistr.fields", "DESCRIPTION");

        connectRecord.configure(props);

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("DESCRIPTION", "UNISTR('M\\00E9nage \\00E9\\00E9\\00E9')");
        fields.put("Can't", "touch this");
        fields.put("MC", 1);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, fields);

        final SourceRecord transformedRecord = connectRecord.apply(record);
        assertEquals("Ménage ééé", ((Map<?, ?>) transformedRecord.value()).get("DESCRIPTION"));
        assertEquals("touch this", ((Map<?, ?>) transformedRecord.value()).get("Can't"));
        assertEquals(1, ((Map<?, ?>) transformedRecord.value()).get("MC"));

    }

    @Test
    public void schemalessDecodeMultipleFields() {
        final Map<String, Object> props = new HashMap<>();

        props.put("unistr.fields", "DESCRIPTION,field2,field3");

        connectRecord.configure(props);

        HashMap<String, Object> fields = new HashMap<>();
        fields.put("DESCRIPTION", "UNISTR('M\\00E9nage \\00E9\\00E9\\00E9')");
        fields.put("field2", "UNISTR('Homage \\0391 \\2665')");
        fields.put("field3", "UNISTR('Wow, look! A \\9F8D !')");
        fields.put("Can't", "touch this");
        fields.put("MC", 1);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, fields);

        final SourceRecord transformedRecord = connectRecord.apply(record);
        assertEquals("Ménage ééé", ((Map<?, ?>) transformedRecord.value()).get("DESCRIPTION"));
        assertEquals("Homage Α ♥", ((Map<?, ?>) transformedRecord.value()).get("field2"));
        assertEquals("Wow, look! A 龍 !", ((Map<?, ?>) transformedRecord.value()).get("field3"));
        assertEquals("touch this", ((Map<?, ?>) transformedRecord.value()).get("Can't"));
        assertEquals(1, ((Map<?, ?>) transformedRecord.value()).get("MC"));

    }


    @Test
    public void copySchemaAndInsertUuidField() {
        final Map<String, Object> props = new HashMap<>();


        props.put("unistr.fields", "DESCRIPTION,field2,field3");

        connectRecord.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("magic", Schema.OPTIONAL_INT64_SCHEMA)
                .field("DESCRIPTION", Schema.STRING_SCHEMA)
                .field("field4", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct simpleStruct = new Struct(simpleStructSchema);
        simpleStruct.put("magic", 42L);
        simpleStruct.put("DESCRIPTION", "UNISTR('M\\00E9nage \\00E9\\00E9\\00E9')");
        simpleStruct.put("field4", "all good é");

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = connectRecord.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("DESCRIPTION").schema());
        assertEquals("Ménage ééé", ((Struct) transformedRecord.value()).getString("DESCRIPTION"));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("field4").schema());
        assertEquals("all good é", ((Struct) transformedRecord.value()).getString("field4"));

        // Exercise caching
        final SourceRecord transformedRecord2 = connectRecord.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema,
                        new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }
}