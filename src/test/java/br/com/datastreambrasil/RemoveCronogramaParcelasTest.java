package br.com.datastreambrasil;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RemoveCronogramaParcelasTest {

    private static final Schema AFTER_SCHEMA = SchemaBuilder.struct()
            .field("jsn_simulacao_motor", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .field("after", AFTER_SCHEMA)
            .build();

    private final RemoveCronogramaParcelas<SourceRecord> transformation = new RemoveCronogramaParcelas<>();

    // ===== Core removal =====

    @Test
    void shouldRemoveCronogramaParcelasFromRootObject() {
        SourceRecord record = newRecord("{\"id\":1,\"CronogramaParcelas\":[1,2],\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldRemoveNestedObjectField() {
        SourceRecord record = newRecord("{\"id\":1,\"CronogramaParcelas\":{\"a\":1,\"b\":[2,3]},\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldRemoveWhenFieldIsOnlyEntry() {
        SourceRecord record = newRecord("{\"CronogramaParcelas\":[1,2,3]}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldRemoveWhenFieldValueIsNull() {
        SourceRecord record = newRecord("{\"id\":1,\"CronogramaParcelas\":null,\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldRemoveWhenFieldValueIsString() {
        SourceRecord record = newRecord("{\"id\":1,\"CronogramaParcelas\":\"text\",\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldPreserveAllOtherFieldTypes() {
        String json = "{\"str\":\"v\",\"num\":42,\"dec\":3.14,\"bool\":true,"
                + "\"nil\":null,\"arr\":[1,2],\"obj\":{\"k\":\"v\"},"
                + "\"CronogramaParcelas\":[9]}";

        SourceRecord record = newRecord(json);
        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals(
                "{\"str\":\"v\",\"num\":42,\"dec\":3.14,\"bool\":true,"
                        + "\"nil\":null,\"arr\":[1,2],\"obj\":{\"k\":\"v\"}}",
                after.getString("jsn_simulacao_motor"));
    }

    // ===== Fast-path: same record reference returned =====

    @Test
    void shouldReturnSameRecordWhenTargetFieldDoesNotExist() {
        SourceRecord record = newRecord("{\"id\":1,\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);

        assertSame(record, transformed);
        Struct after = ((Struct) transformed.value()).getStruct("after");
        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldReturnSameRecordWhenPayloadIsNotAnObject() {
        SourceRecord record = newRecord("[{\"CronogramaParcelas\":[1,2]}]");

        SourceRecord transformed = transformation.apply(record);

        assertSame(record, transformed);
        Struct after = ((Struct) transformed.value()).getStruct("after");
        assertEquals("[{\"CronogramaParcelas\":[1,2]}]", after.getString("jsn_simulacao_motor"));
    }

    @Test
    void shouldReturnSameRecordWhenJsonIsNull() {
        SourceRecord record = newRecord(null);

        assertSame(record, transformation.apply(record));
    }

    @Test
    void shouldReturnSameRecordWhenJsonIsEmpty() {
        SourceRecord record = newRecord("");

        assertSame(record, transformation.apply(record));
    }

    @Test
    void shouldReturnSameRecordWhenValueIsNotStruct() {
        SourceRecord record = new SourceRecord(
                null, null, "topic", Schema.STRING_SCHEMA, "plain-string");

        assertSame(record, transformation.apply(record));
    }

    @Test
    void shouldReturnSameRecordWhenAfterFieldIsMissing() {
        Schema noAfterSchema = SchemaBuilder.struct()
                .field("before", AFTER_SCHEMA)
                .build();
        Struct value = new Struct(noAfterSchema)
                .put("before", new Struct(AFTER_SCHEMA).put("jsn_simulacao_motor", "{\"CronogramaParcelas\":[1]}"));
        SourceRecord record = new SourceRecord(null, null, "topic", null, noAfterSchema, value);

        assertSame(record, transformation.apply(record));
    }

    @Test
    void shouldReturnSameRecordWhenIndexOfMatchesButFieldNotAtRootLevel() {
        // "CronogramaParcelas" appears inside a nested value, not as a root key.
        // indexOf will match, but the streaming parser won't find it at root level.
        String json = "{\"id\":1,\"data\":\"contains CronogramaParcelas text\"}";
        SourceRecord record = newRecord(json);

        assertSame(record, transformation.apply(record));
    }

    // ===== Idempotency / repeated calls =====

    @Test
    void shouldBeIdempotentOnRepeatedCalls() {
        SourceRecord r1 = newRecord("{\"id\":1,\"CronogramaParcelas\":[1],\"ativo\":true}");
        transformation.apply(r1);

        SourceRecord r2 = newRecord("{\"id\":2,\"CronogramaParcelas\":[2],\"x\":false}");
        SourceRecord t2 = transformation.apply(r2);
        Struct after = ((Struct) t2.value()).getStruct("after");

        assertEquals("{\"id\":2,\"x\":false}", after.getString("jsn_simulacao_motor"));
    }

    // ===== Helper =====

    private SourceRecord newRecord(String json) {
        Struct after = new Struct(AFTER_SCHEMA)
                .put("jsn_simulacao_motor", json);
        Struct value = new Struct(VALUE_SCHEMA)
                .put("after", after);

        return new SourceRecord(null, null, "topic", null, VALUE_SCHEMA, value);
    }
}
