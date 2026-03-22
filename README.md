# RemoveCronogramaParcelas SMT

A high-performance Kafka Connect Single Message Transform (SMT) that removes the `CronogramaParcelas` field from JSON strings embedded in Debezium CDC `after` structs. Designed for high-throughput pipelines where every allocation and CPU cycle in the `apply()` hot path matters.

## How It Works

The transform intercepts Debezium-style records, locates the `jsn_simulacao_motor` field inside the `after` Struct, and strips the `CronogramaParcelas` key from the root-level JSON object using Jackson's streaming API. The original record reference is returned in all cases — the Struct is mutated in-place only when a removal actually occurs.

**Input:**
```json
{"id": 1, "CronogramaParcelas": [1, 2, 3], "ativo": true}
```

**Output:**
```json
{"id": 1, "ativo": true}
```

## Architecture & Performance

### Fast-Path Pre-flight Check

Before any Jackson object is allocated, the `apply()` method runs a cascade of zero-cost guards:

1. `instanceof Struct` — rejects non-Debezium records
2. `schema().field("after") == null` — rejects schemas without the target struct
3. `after == null` — handles tombstones and deletes
4. `jsonStr.indexOf("CronogramaParcelas") < 0` — **the critical gate**: a primitive string scan that short-circuits Jackson initialization entirely when the target field is absent

In production, the vast majority of records will exit at step 4 with zero allocations beyond the method frame itself.

### Zero-Allocation Buffer Reuse

The standard `StringWriter` delegates to `StringBuffer`, which is **synchronized on every write call** — a hidden cost that compounds across millions of records per second. This SMT eliminates that overhead with two components:

- **`ThreadLocal<StringBuilder>`** — a per-thread, pre-allocated 4 KB buffer that is cleared (`setLength(0)`) and reused across calls. Capacity grows once and is retained, so steady-state operation triggers zero GC pressure from the output buffer.
- **`StringBuilderWriter`** — a minimal `Writer` implementation backed directly by the `StringBuilder`. It overrides only the methods Jackson's `WriterBasedJsonGenerator` actually invokes, keeping the vtable small for JIT inlining. No synchronization, no intermediate buffer, no `flush()` overhead.

### Jackson Streaming with Disabled Canonicalization

The JSON processing uses Jackson's `JsonParser`/`JsonGenerator` streaming API for single-pass, forward-only processing with no intermediate DOM tree:

- **`copyCurrentStructure(parser)`** passes non-target fields directly from parser to generator without object materialization.
- **`skipChildren()`** discards the `CronogramaParcelas` value (arrays, nested objects) without parsing its contents.
- **`CANONICALIZE_FIELD_NAMES` and `INTERN_FIELD_NAMES` are disabled** on the static `JsonFactory`. The default behavior builds a `ByteQuadsCanonicalizer` symbol table and interns field names via `String.intern()` — useful for repeated parsing of the same schema, but pure overhead for our single-pass stream-through where no field name is ever looked up again.

### Static JsonFactory

The `JsonFactory` is declared `static final` at the class level. It is fully thread-safe and expensive to construct (buffer pool initialization, feature flag resolution). A single instance is shared across all SMT instances and all Kafka Connect worker threads.

## Prerequisites

- **Java 11+**
- **Apache Kafka Connect 3.6.0+** (provided at runtime)

## Build

```bash
# Fat JAR with relocated Jackson (avoids classpath conflicts in Kafka Connect)
mvn clean package

# Run tests
mvn test

# Build without tests
mvn clean package -DskipTests
```

The output JAR is produced by `maven-shade-plugin` with Jackson relocated to `br.com.datastreambrasil.shaded.jackson.*` to prevent version conflicts with other Kafka Connect plugins.

## Connector Configuration

Add the SMT to any Debezium source connector:

```properties
transforms=removeCronograma
transforms.removeCronograma.type=br.com.datastreambrasil.RemoveCronogramaParcelas
```

No additional configuration properties are required. The transform targets:
- **Struct field:** `after.jsn_simulacao_motor`
- **JSON key removed:** `CronogramaParcelas` (root level only)

## Performance Guardrails

> **Warning for maintainers:** The following patterns are load-bearing performance optimizations validated for high-throughput CDC pipelines. Do not modify them without running benchmarks under production-equivalent load.

| Pattern | Why It Exists | Risk If Removed |
|---------|---------------|-----------------|
| `indexOf` pre-flight check | Avoids Jackson parser/generator allocation for records without the target field | Every record pays full parsing cost regardless of whether removal is needed |
| `ThreadLocal<StringBuilder>` | Eliminates per-record `StringBuffer` allocation and its synchronized `write()` overhead | GC pressure from millions of short-lived `StringWriter` instances; lock contention under concurrency |
| `StringBuilderWriter` | Bypasses `StringWriter` → `StringBuffer` synchronization; small vtable for JIT inlining | Falls back to synchronized writes; potential deoptimization of the generator write loop |
| `static final JsonFactory` with disabled canonicalization | Single shared instance; no symbol table or `String.intern()` overhead | Per-instance factory allocation; CPU wasted on field name interning that is never reused |
| `copyCurrentStructure` / `skipChildren` | Zero-materialization pass-through of non-target fields; zero-parse discard of target value | Intermediate `JsonNode` or `Map` allocation for every field in every record |
