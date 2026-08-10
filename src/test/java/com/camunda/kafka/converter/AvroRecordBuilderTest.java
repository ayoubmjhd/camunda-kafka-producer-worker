package com.camunda.kafka.converter;

import com.camunda.kafka.exception.KafkaProducerException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link AvroRecordBuilder}.
 * Focuses on the ENUM handling fix and all type conversions.
 */
class AvroRecordBuilderTest {

    private AvroRecordBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new AvroRecordBuilder();
    }

    // =========================================================================
    // Test Schemas
    // =========================================================================

    private static final String SIMPLE_SCHEMA = """
            {
              "type": "record",
              "name": "SimpleRecord",
              "namespace": "test",
              "fields": [
                {"name": "name", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "score", "type": "double"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """;

    private static final String ENUM_SCHEMA = """
            {
              "type": "record",
              "name": "OrderRecord",
              "namespace": "test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {
                  "name": "status",
                  "type": {
                    "type": "enum",
                    "name": "OrderStatus",
                    "symbols": ["CREATED", "APPROVED", "REJECTED", "SHIPPED", "DELIVERED"]
                  }
                }
              ]
            }
            """;

    private static final String NULLABLE_ENUM_SCHEMA = """
            {
              "type": "record",
              "name": "OrderRecord",
              "namespace": "test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {
                  "name": "priority",
                  "type": ["null", {
                    "type": "enum",
                    "name": "Priority",
                    "symbols": ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
                  }],
                  "default": null
                }
              ]
            }
            """;

    private static final String NESTED_RECORD_SCHEMA = """
            {
              "type": "record",
              "name": "OrderRecord",
              "namespace": "test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {
                  "name": "address",
                  "type": {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                      {"name": "street", "type": "string"},
                      {"name": "city", "type": "string"}
                    ]
                  }
                }
              ]
            }
            """;

    private static final String ARRAY_SCHEMA = """
            {
              "type": "record",
              "name": "OrderRecord",
              "namespace": "test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "productId", "type": "string"},
                        {"name": "quantity", "type": "int"}
                      ]
                    }
                  }
                }
              ]
            }
            """;

    private static final String FULL_SCHEMA = """
            {
              "type": "record",
              "name": "OrderEvent",
              "namespace": "test",
              "fields": [
                {"name": "orderId", "type": "string"},
                {"name": "amount", "type": "double"},
                {
                  "name": "status",
                  "type": {
                    "type": "enum",
                    "name": "OrderStatus",
                    "symbols": ["CREATED", "APPROVED", "REJECTED"]
                  }
                },
                {
                  "name": "priority",
                  "type": ["null", {
                    "type": "enum",
                    "name": "Priority",
                    "symbols": ["LOW", "MEDIUM", "HIGH"]
                  }],
                  "default": null
                },
                {
                  "name": "items",
                  "type": {
                    "type": "array",
                    "items": {
                      "type": "record",
                      "name": "Item",
                      "fields": [
                        {"name": "productId", "type": "string"},
                        {"name": "quantity", "type": "int"}
                      ]
                    }
                  }
                },
                {
                  "name": "address",
                  "type": ["null", {
                    "type": "record",
                    "name": "Address",
                    "fields": [
                      {"name": "street", "type": "string"},
                      {"name": "city", "type": "string"}
                    ]
                  }],
                  "default": null
                }
              ]
            }
            """;

    // =========================================================================
    // Simple types
    // =========================================================================

    @Nested
    @DisplayName("Simple field types")
    class SimpleTypes {

        @Test
        @DisplayName("Should convert string, int, double, boolean fields")
        void shouldConvertSimpleFields() {
            Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA);
            Map<String, Object> data = Map.of(
                    "name", "John",
                    "age", 30,
                    "score", 95.5,
                    "active", true
            );

            GenericRecord record = builder.buildRecord(schema, data);

            assertEquals("John", record.get("name").toString());
            assertEquals(30, record.get("age"));
            assertEquals(95.5, record.get("score"));
            assertEquals(true, record.get("active"));
        }

        @Test
        @DisplayName("Should coerce numeric types (e.g., double to int)")
        void shouldCoerceNumericTypes() {
            Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA);
            // JSON deserializes integers > MAX_INT as Long, and all decimals as Double
            Map<String, Object> data = Map.of(
                    "name", "John",
                    "age", 30.0,  // Double instead of int
                    "score", 95,   // Integer instead of double
                    "active", true
            );

            GenericRecord record = builder.buildRecord(schema, data);

            assertEquals(30, record.get("age"));
            assertEquals(95.0, record.get("score"));
        }
    }

    // =========================================================================
    // ENUM handling — THE MAIN FIX
    // =========================================================================

    @Nested
    @DisplayName("ENUM handling (the fix)")
    class EnumHandling {

        @Test
        @DisplayName("Should convert string to EnumSymbol for ENUM fields")
        void shouldConvertEnumField() {
            Schema schema = new Schema.Parser().parse(ENUM_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "status", "APPROVED"
            );

            GenericRecord record = builder.buildRecord(schema, data);

            Object statusValue = record.get("status");
            assertInstanceOf(GenericData.EnumSymbol.class, statusValue);
            assertEquals("APPROVED", statusValue.toString());
        }

        @Test
        @DisplayName("Should throw on invalid enum value")
        void shouldThrowOnInvalidEnumValue() {
            Schema schema = new Schema.Parser().parse(ENUM_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "status", "INVALID_STATUS"
            );

            KafkaProducerException ex = assertThrows(
                    KafkaProducerException.class,
                    () -> builder.buildRecord(schema, data)
            );

            assertTrue(ex.getMessage().contains("Invalid enum value"));
            assertTrue(ex.getMessage().contains("INVALID_STATUS"));
            assertTrue(ex.getMessage().contains("OrderStatus"));
        }

        @Test
        @DisplayName("Should handle all valid enum symbols")
        void shouldHandleAllEnumSymbols() {
            Schema schema = new Schema.Parser().parse(ENUM_SCHEMA);

            for (String symbol : List.of("CREATED", "APPROVED", "REJECTED", "SHIPPED", "DELIVERED")) {
                Map<String, Object> data = Map.of(
                        "orderId", "order-001",
                        "status", symbol
                );

                GenericRecord record = builder.buildRecord(schema, data);
                assertEquals(symbol, record.get("status").toString());
            }
        }
    }

    // =========================================================================
    // UNION types with ENUM
    // =========================================================================

    @Nested
    @DisplayName("Nullable ENUM (union with null)")
    class NullableEnum {

        @Test
        @DisplayName("Should handle non-null enum in union")
        void shouldHandleNonNullEnumInUnion() {
            Schema schema = new Schema.Parser().parse(NULLABLE_ENUM_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "priority", "HIGH"
            );

            GenericRecord record = builder.buildRecord(schema, data);

            Object priorityValue = record.get("priority");
            assertInstanceOf(GenericData.EnumSymbol.class, priorityValue);
            assertEquals("HIGH", priorityValue.toString());
        }

        @Test
        @DisplayName("Should handle null enum in union")
        void shouldHandleNullEnumInUnion() {
            Schema schema = new Schema.Parser().parse(NULLABLE_ENUM_SCHEMA);
            Map<String, Object> data = new HashMap<>();
            data.put("orderId", "order-001");
            data.put("priority", null);

            GenericRecord record = builder.buildRecord(schema, data);

            assertNull(record.get("priority"));
        }

        @Test
        @DisplayName("Should handle missing nullable field (uses default)")
        void shouldHandleMissingNullableField() {
            Schema schema = new Schema.Parser().parse(NULLABLE_ENUM_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001"
                    // priority not provided — should use default null
            );

            GenericRecord record = builder.buildRecord(schema, data);

            // Field not set, Avro will use the default (null)
            assertNull(record.get("priority"));
        }
    }

    // =========================================================================
    // Nested records
    // =========================================================================

    @Nested
    @DisplayName("Nested records")
    class NestedRecords {

        @Test
        @DisplayName("Should convert nested record")
        void shouldConvertNestedRecord() {
            Schema schema = new Schema.Parser().parse(NESTED_RECORD_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "address", Map.of(
                            "street", "123 Main St",
                            "city", "Munich"
                    )
            );

            GenericRecord record = builder.buildRecord(schema, data);

            Object addressValue = record.get("address");
            assertInstanceOf(GenericRecord.class, addressValue);

            GenericRecord address = (GenericRecord) addressValue;
            assertEquals("123 Main St", address.get("street").toString());
            assertEquals("Munich", address.get("city").toString());
        }
    }

    // =========================================================================
    // Arrays
    // =========================================================================

    @Nested
    @DisplayName("Array of records")
    class ArrayHandling {

        @Test
        @DisplayName("Should convert array of records")
        void shouldConvertArrayOfRecords() {
            Schema schema = new Schema.Parser().parse(ARRAY_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "items", List.of(
                            Map.of("productId", "SKU-100", "quantity", 2),
                            Map.of("productId", "SKU-200", "quantity", 1)
                    )
            );

            GenericRecord record = builder.buildRecord(schema, data);

            @SuppressWarnings("unchecked")
            List<GenericRecord> items = (List<GenericRecord>) record.get("items");
            assertEquals(2, items.size());
            assertEquals("SKU-100", items.get(0).get("productId").toString());
            assertEquals(2, items.get(0).get("quantity"));
            assertEquals("SKU-200", items.get(1).get("productId").toString());
        }

        @Test
        @DisplayName("Should handle empty array")
        void shouldHandleEmptyArray() {
            Schema schema = new Schema.Parser().parse(ARRAY_SCHEMA);
            Map<String, Object> data = Map.of(
                    "orderId", "order-001",
                    "items", List.of()
            );

            GenericRecord record = builder.buildRecord(schema, data);

            @SuppressWarnings("unchecked")
            List<GenericRecord> items = (List<GenericRecord>) record.get("items");
            assertTrue(items.isEmpty());
        }
    }

    // =========================================================================
    // Full integration: all types combined
    // =========================================================================

    @Nested
    @DisplayName("Full schema integration")
    class FullIntegration {

        @Test
        @DisplayName("Should handle full schema with enum, nullable enum, array, nested record")
        void shouldHandleFullSchema() {
            Schema schema = new Schema.Parser().parse(FULL_SCHEMA);
            Map<String, Object> data = new HashMap<>();
            data.put("orderId", "order-001");
            data.put("amount", 150.99);
            data.put("status", "APPROVED");
            data.put("priority", "HIGH");
            data.put("items", List.of(
                    Map.of("productId", "SKU-100", "quantity", 2)
            ));
            data.put("address", Map.of(
                    "street", "123 Main St",
                    "city", "Munich"
            ));

            GenericRecord record = builder.buildRecord(schema, data);

            // Verify all fields
            assertEquals("order-001", record.get("orderId").toString());
            assertEquals(150.99, record.get("amount"));

            // ENUM field
            assertInstanceOf(GenericData.EnumSymbol.class, record.get("status"));
            assertEquals("APPROVED", record.get("status").toString());

            // Nullable ENUM
            assertInstanceOf(GenericData.EnumSymbol.class, record.get("priority"));
            assertEquals("HIGH", record.get("priority").toString());

            // Array of records
            @SuppressWarnings("unchecked")
            List<GenericRecord> items = (List<GenericRecord>) record.get("items");
            assertEquals(1, items.size());
            assertEquals("SKU-100", items.get(0).get("productId").toString());

            // Nullable nested record
            GenericRecord address = (GenericRecord) record.get("address");
            assertEquals("123 Main St", address.get("street").toString());
        }

        @Test
        @DisplayName("Should handle full schema with nulls for optional fields")
        void shouldHandleFullSchemaWithNulls() {
            Schema schema = new Schema.Parser().parse(FULL_SCHEMA);
            Map<String, Object> data = new HashMap<>();
            data.put("orderId", "order-002");
            data.put("amount", 50.0);
            data.put("status", "CREATED");
            data.put("priority", null);  // nullable enum → null
            data.put("items", List.of());
            data.put("address", null);   // nullable record → null

            GenericRecord record = builder.buildRecord(schema, data);

            assertEquals("order-002", record.get("orderId").toString());
            assertEquals("CREATED", record.get("status").toString());
            assertNull(record.get("priority"));
            assertNull(record.get("address"));
        }
    }
}
