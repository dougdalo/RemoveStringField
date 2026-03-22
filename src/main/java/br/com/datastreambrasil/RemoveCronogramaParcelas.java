package br.com.datastreambrasil;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class RemoveCronogramaParcelas<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String AFTER_FIELD = "after";
    private static final String JSON_FIELD = "jsn_simulacao_motor";
    private static final String REMOVED_FIELD = "CronogramaParcelas";

    // Static & thread-safe. Disable symbol-table canonicalization: we do a single-pass
    // stream-through, so interning field names into ByteQuadsCanonicalizer is pure waste.
    private static final JsonFactory JSON_FACTORY = new JsonFactory()
            .disable(JsonFactory.Feature.CANONICALIZE_FIELD_NAMES)
            .disable(JsonFactory.Feature.INTERN_FIELD_NAMES);

    // Unsynchronized StringBuilder per thread — eliminates StringWriter's
    // synchronized StringBuffer and avoids allocating a new writer per record.
    private static final ThreadLocal<StringBuilder> REUSABLE_BUFFER =
            ThreadLocal.withInitial(() -> new StringBuilder(4096));

    @Override
    public R apply(R record) {
        // Fast-path: type check
        if (!(record.value() instanceof Struct)) {
            return record;
        }

        Struct value = (Struct) record.value();

        // Fast-path: schema doesn't contain "after" (non-Debezium record)
        if (value.schema().field(AFTER_FIELD) == null) {
            return record;
        }

        Struct after = value.getStruct(AFTER_FIELD);
        if (after == null) {
            return record;
        }

        String jsonStr = after.getString(JSON_FIELD);

        // Fast-path: pre-flight indexOf before any Jackson allocation
        if (jsonStr == null || jsonStr.isEmpty() || jsonStr.indexOf(REMOVED_FIELD) < 0) {
            return record;
        }

        try {
            String cleaned = removeCronogramaParcelas(jsonStr);
            if (cleaned == null) {
                return record;
            }
            after.put(JSON_FIELD, cleaned);
        } catch (IOException e) {
            throw new RuntimeException("Erro ao processar jsn_simulacao_motor", e);
        }

        return record;
    }

    private String removeCronogramaParcelas(String jsonStr) throws IOException {
        try (JsonParser parser = JSON_FACTORY.createParser(jsonStr)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                return null;
            }

            // Reuse thread-local StringBuilder instead of new StringWriter per call
            StringBuilder sb = REUSABLE_BUFFER.get();
            sb.setLength(0);
            // Ensure capacity to avoid intermediate resizes — output is at most
            // the same length as input (we only remove, never add).
            sb.ensureCapacity(jsonStr.length());

            boolean removed = false;

            try (JsonGenerator gen = JSON_FACTORY.createGenerator(new StringBuilderWriter(sb))) {
                gen.writeStartObject();

                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String fieldName = parser.currentName();
                    parser.nextToken(); // advance to value

                    if (REMOVED_FIELD.equals(fieldName)) {
                        parser.skipChildren();
                        removed = true;
                        continue;
                    }

                    gen.writeFieldName(fieldName);
                    gen.copyCurrentStructure(parser);
                }

                gen.writeEndObject();
            }

            return removed ? sb.toString() : null;
        }
    }

    /**
     * Minimal Writer backed by a StringBuilder — zero synchronization overhead
     * unlike StringWriter which delegates to synchronized StringBuffer.
     * Only implements the methods Jackson's UTF8JsonGenerator/WriterBasedJsonGenerator
     * actually calls, keeping the vtable small for JIT inlining.
     */
    static final class StringBuilderWriter extends Writer {
        private final StringBuilder sb;

        StringBuilderWriter(StringBuilder sb) {
            this.sb = sb;
        }

        @Override
        public void write(final char[] cbuf, final int off, final int len) {
            sb.append(cbuf, off, len);
        }

        @Override
        public void write(final String str) {
            sb.append(str);
        }

        @Override
        public void write(final String str, final int off, final int len) {
            sb.append(str, off, off + len);
        }

        @Override
        public void write(final int c) {
            sb.append((char) c);
        }

        @Override
        public Writer append(final CharSequence csq) {
            sb.append(csq);
            return this;
        }

        @Override
        public Writer append(final CharSequence csq, final int start, final int end) {
            sb.append(csq, start, end);
            return this;
        }

        @Override
        public void flush() {
            // no-op: StringBuilder has no buffer to flush
        }

        @Override
        public void close() {
            // no-op: nothing to release
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
