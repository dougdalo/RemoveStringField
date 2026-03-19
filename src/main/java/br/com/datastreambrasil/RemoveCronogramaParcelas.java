package br.com.datastreambrasil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;

public class RemoveCronogramaParcelas<R extends ConnectRecord<R>> implements Transformation<R> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public R apply(R record) {

        if (record.value() == null || !(record.value() instanceof Struct)) {
            return record;
        }

        Struct value = (Struct) record.value();

        Struct after = value.getStruct("after");
        if (after == null) return record;

        String jsonStr = after.getString("jsn_simulacao_motor");
        if (jsonStr == null || jsonStr.isEmpty()) return record;

        try {
            JsonNode inner = mapper.readTree(jsonStr);

            if (inner instanceof ObjectNode) {
                ((ObjectNode) inner).remove("CronogramaParcelas");
            }

            String cleaned = mapper.writeValueAsString(inner);

            // ⚠️ IMPORTANTE: altera o Struct existente
            after.put("jsn_simulacao_motor", cleaned);

        } catch (Exception e) {
            throw new RuntimeException("Erro ao processar jsn_simulacao_motor", e);
        }

        return record;
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