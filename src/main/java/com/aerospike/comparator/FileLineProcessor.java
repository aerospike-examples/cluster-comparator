package com.aerospike.comparator;

import com.aerospike.comparator.dbaccess.AerospikeClientAccess;

public interface FileLineProcessor {
    void process(AerospikeClientAccess client1, AerospikeClientAccess client2, FileLine line);
}
