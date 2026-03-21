package br.com.datastreambrasil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.JsonGenerator;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public class RemoveCronogramaParcelas<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String AFTER_FIELD = "after";
    private static final String JSON_FIELD = "jsn_simulacao_motor";
    private static final String REMOVED_FIELD = "CronogramaParcelas";

    private final JsonFactory jsonFactory = new JsonFactory();

    @Override
    public R apply(R record) {

        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct value = (Struct) record.value();
        Struct after = value.getStruct(AFTER_FIELD);
        if (after == null) {
            return record;
        }

        String jsonStr = after.getString(JSON_FIELD);
        if (jsonStr == null || jsonStr.isEmpty() || jsonStr.indexOf(REMOVED_FIELD) < 0) {
            return record;
        }

        try {
            String cleaned = removeCronogramaParcelas(jsonStr);
            if (cleaned == null) {
                return record;
            }

            // ⚠️ IMPORTANTE: altera o Struct existente
            after.put(JSON_FIELD, cleaned);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao processar jsn_simulacao_motor", e);
        }

        return record;
    }

    private String removeCronogramaParcelas(String jsonStr) throws IOException {
        try (JsonParser parser = jsonFactory.createParser(jsonStr)) {
            JsonToken firstToken = parser.nextToken();
            if (firstToken != JsonToken.START_OBJECT) {
                return null;
            }

            StringWriter writer = new StringWriter(jsonStr.length());
            boolean removed = false;

            try (JsonGenerator generator = jsonFactory.createGenerator(writer)) {
                generator.writeStartObject();

                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = parser.currentName();
                    JsonToken valueToken = parser.nextToken();

                    if (REMOVED_FIELD.equals(fieldName)) {
                        parser.skipChildren();
                        removed = true;
                        continue;
                    }

                    generator.writeFieldName(fieldName);
                    generator.copyCurrentStructure(parser);

                    if (valueToken == null) {
                        break;
                    }
                }

                generator.writeEndObject();
            }

            return removed ? writer.toString() : null;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void close() {}

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new org.apache.kafka.common.config.ConfigDef();
    }
}
