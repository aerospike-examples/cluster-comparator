package com.aerospike.comparator;

import com.aerospike.comparator.dbaccess.AerospikeClientAccess;

public interface FileLineProcessor {
    void process(AerospikeClientAccess[] clients, FileLine line);
}
