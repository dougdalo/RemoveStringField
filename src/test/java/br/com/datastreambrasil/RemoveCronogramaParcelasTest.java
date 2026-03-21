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

    @Test
    void shouldRemoveCronogramaParcelasFromRootObject() {
        SourceRecord record = newRecord("{\"id\":1,\"CronogramaParcelas\":[1,2],\"ativo\":true}");

        SourceRecord transformed = transformation.apply(record);
        Struct after = ((Struct) transformed.value()).getStruct("after");

        assertEquals("{\"id\":1,\"ativo\":true}", after.getString("jsn_simulacao_motor"));
    }

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

    private SourceRecord newRecord(String json) {
        Struct after = new Struct(AFTER_SCHEMA)
                .put("jsn_simulacao_motor", json);
        Struct value = new Struct(VALUE_SCHEMA)
                .put("after", after);

        return new SourceRecord(null, null, "topic", null, VALUE_SCHEMA, value);
    }
}
