package com.aerospike.comparator.metadata;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Determine the name of the field
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface FieldName {
    /**
     * The name of the bin to use. If not specified, the field name is used for the bin name.
     */
    String value();
    String defValue() default "";
}

