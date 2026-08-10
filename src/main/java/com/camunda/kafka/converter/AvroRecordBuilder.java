package com.camunda.kafka.converter;

import com.camunda.kafka.exception.KafkaProducerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a {@code Map<String, Object>} (from Camunda process variables) into
 * an Avro {@link GenericRecord} using a given schema.
 *
 * <p>This is a fixed version of the official Camunda connector's
 * {@code GenericRecordConverter}, which does NOT handle:
 * <ul>
 *   <li>ENUM fields (causes AvroTypeException)</li>
 *   <li>ENUM inside UNION types (e.g., ["null", "EnumType"])</li>
 *   <li>MAP types</li>
 *   <li>Numeric type coercion (int vs long vs double)</li>
 * </ul>
 */
@Slf4j
@Component
public class AvroRecordBuilder {

    /**
     * Build a GenericRecord from a map of values using the provided Avro schema.
     *
     * @param schema the Avro schema to use
     * @param data   the map of field values (typically from process variables)
     * @return a populated GenericRecord
     * @throws KafkaProducerException if a value is incompatible with the schema
     */
    public GenericRecord buildRecord(Schema schema, Map<String, Object> data) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new KafkaProducerException(
                    "Expected RECORD schema but got " + schema.getType());
        }

        GenericRecord record = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            String fieldName = field.name();
            Object rawValue = data.get(fieldName);

            if (rawValue == null && !data.containsKey(fieldName)) {
                // Field not provided — use default if available, otherwise skip
                if (field.hasDefaultValue()) {
                    continue; // Avro will use the default
                }
                // For required fields without defaults, let Avro validation catch it
                continue;
            }

            try {
                Object convertedValue = convertValue(field.schema(), rawValue);
                record.put(fieldName, convertedValue);
            } catch (KafkaProducerException e) {
                throw new KafkaProducerException(
                        "Error converting field '" + fieldName + "': " + e.getMessage(), e);
            }
        }

        return record;
    }

    /**
     * Convert a value to the appropriate Avro type based on the schema.
     * This is the core method that handles ALL Avro types including ENUM.
     */
    @SuppressWarnings("unchecked")
    private Object convertValue(Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        return switch (schema.getType()) {
            case NULL -> null;

            case STRING -> value.toString();

            case INT -> convertToInt(value);

            case LONG -> convertToLong(value);

            case FLOAT -> convertToFloat(value);

            case DOUBLE -> convertToDouble(value);

            case BOOLEAN -> convertToBoolean(value);

            case ENUM -> convertToEnum(schema, value);

            case RECORD -> {
                if (!(value instanceof Map)) {
                    throw new KafkaProducerException(
                            "Expected Map for RECORD type but got " + value.getClass().getSimpleName());
                }
                yield buildRecord(schema, (Map<String, Object>) value);
            }

            case ARRAY -> {
                if (!(value instanceof List)) {
                    throw new KafkaProducerException(
                            "Expected List for ARRAY type but got " + value.getClass().getSimpleName());
                }
                yield convertArray(schema, (List<?>) value);
            }

            case MAP -> {
                if (!(value instanceof Map)) {
                    throw new KafkaProducerException(
                            "Expected Map for MAP type but got " + value.getClass().getSimpleName());
                }
                yield convertMap(schema, (Map<String, ?>) value);
            }

            case UNION -> convertUnion(schema, value);

            case BYTES -> {
                if (value instanceof byte[]) {
                    yield ByteBuffer.wrap((byte[]) value);
                } else if (value instanceof String) {
                    yield ByteBuffer.wrap(((String) value).getBytes());
                }
                throw new KafkaProducerException(
                        "Cannot convert " + value.getClass().getSimpleName() + " to BYTES");
            }

            case FIXED -> {
                byte[] bytes;
                if (value instanceof byte[]) {
                    bytes = (byte[]) value;
                } else if (value instanceof String) {
                    bytes = ((String) value).getBytes();
                } else {
                    throw new KafkaProducerException(
                            "Cannot convert " + value.getClass().getSimpleName() + " to FIXED");
                }
                yield new GenericData.Fixed(schema, bytes);
            }
        };
    }

    // ==========================================================================
    // ENUM handling — THE FIX for the official connector bug
    // ==========================================================================

    /**
     * Convert a value to an Avro EnumSymbol, with validation.
     * The official connector passes raw strings which causes AvroTypeException.
     */
    private GenericData.EnumSymbol convertToEnum(Schema schema, Object value) {
        String enumValue = value.toString();

        if (!schema.getEnumSymbols().contains(enumValue)) {
            throw new KafkaProducerException(
                    "Invalid enum value '" + enumValue + "' for type '" + schema.getName()
                            + "'. Allowed values: " + schema.getEnumSymbols());
        }

        log.debug("Converting enum value '{}' for type '{}'", enumValue, schema.getName());
        return new GenericData.EnumSymbol(schema, enumValue);
    }

    // ==========================================================================
    // UNION handling — also fixes missing ENUM support in unions
    // ==========================================================================

    /**
     * Handle UNION types by finding the matching schema type.
     * The official connector only handles RECORD and ARRAY inside unions,
     * missing ENUM and other types.
     */
    @SuppressWarnings("unchecked")
    private Object convertUnion(Schema unionSchema, Object value) {
        if (value == null) {
            // Check if null is a valid union member
            for (Schema memberSchema : unionSchema.getTypes()) {
                if (memberSchema.getType() == Schema.Type.NULL) {
                    return null;
                }
            }
            throw new KafkaProducerException("NULL is not allowed in this union: " + unionSchema);
        }

        Object resolvedValue = value;
        Schema matchedSchema = null;

        // Check if value is wrapped in standard Avro JSON union format: {"fullyQualifiedName": value} or {"primitiveType": value}
        if (value instanceof Map<?, ?> map && map.size() == 1) {
            Object keyObj = map.keySet().iterator().next();
            if (keyObj instanceof String key) {
                for (Schema memberSchema : unionSchema.getTypes()) {
                    boolean isMatch = false;
                    if (memberSchema.getType() == Schema.Type.RECORD 
                            || memberSchema.getType() == Schema.Type.ENUM 
                            || memberSchema.getType() == Schema.Type.FIXED) {
                        isMatch = memberSchema.getFullName().equals(key);
                    } else if (memberSchema.getType() != Schema.Type.NULL) {
                        isMatch = memberSchema.getType().getName().toLowerCase().equals(key.toLowerCase());
                    }
                    
                    if (isMatch) {
                        resolvedValue = map.get(key);
                        matchedSchema = memberSchema;
                        break;
                    }
                }
            }
        }

        if (matchedSchema != null) {
            try {
                return convertValue(matchedSchema, resolvedValue);
            } catch (Exception e) {
                // If it fails, fallback to trying all union types just in case
                log.trace("Failed to convert union with matched schema {}, falling back", matchedSchema, e);
            }
        }

        // Try each non-null type in the union
        List<Schema> nonNullTypes = unionSchema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .toList();

        for (Schema memberSchema : nonNullTypes) {
            try {
                return convertValue(memberSchema, value);
            } catch (KafkaProducerException e) {
                log.trace("Union type {} did not match for value: {}", memberSchema.getType(), e.getMessage());
                // Try next type in union
            } catch (ClassCastException | NumberFormatException e) {
                log.trace("Union type {} casting failed: {}", memberSchema.getType(), e.getMessage());
                // Try next type in union
            }
        }

        throw new KafkaProducerException(
                "No matching type in union " + unionSchema + " for value: " + value
                        + " (type: " + value.getClass().getSimpleName() + ")");
    }

    // ==========================================================================
    // Collection types
    // ==========================================================================

    private List<Object> convertArray(Schema arraySchema, List<?> values) {
        Schema elementSchema = arraySchema.getElementType();
        List<Object> result = new ArrayList<>(values.size());

        for (Object item : values) {
            result.add(convertValue(elementSchema, item));
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> convertMap(Schema mapSchema, Map<String, ?> values) {
        Schema valueSchema = mapSchema.getValueType();
        Map<String, Object> result = new HashMap<>(values.size());

        for (Map.Entry<String, ?> entry : values.entrySet()) {
            result.put(entry.getKey(), convertValue(valueSchema, entry.getValue()));
        }

        return result;
    }

    // ==========================================================================
    // Numeric type coercion
    // ==========================================================================

    private int convertToInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return Integer.parseInt(value.toString());
    }

    private long convertToLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    private float convertToFloat(Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return Float.parseFloat(value.toString());
    }

    private double convertToDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    private boolean convertToBoolean(Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }
}
