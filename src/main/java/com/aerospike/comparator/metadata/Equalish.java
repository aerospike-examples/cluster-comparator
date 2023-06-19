package com.aerospike.comparator.metadata;

/**
 * This interface is used to define classes which are "equalish", ie only compare fields which 
 * should be consistent between clusters. For example, the names of a Set should be the same,
 * but a set's truncate LUT isn't necessarily the same.
 * @author tfaulkes
 *
 */
public interface Equalish {
    boolean isEqualish(Object other);
}
