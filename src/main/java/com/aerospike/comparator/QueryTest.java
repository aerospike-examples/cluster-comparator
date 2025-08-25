package com.aerospike.comparator;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

public class QueryTest {
    public static void main(String[] args) {
        IAerospikeClient client = new AerospikeClient("localhost", 3100);
        Statement stmt = new Statement();
        stmt.setNamespace("test");
        stmt.setSetName("testset");
        stmt.setFilter(Filter.equal("gender", "f"));
        QueryPolicy qp = new QueryPolicy();
        qp.maxRetries = 5;  //Let's use our QueryPolicy object, qp
        qp.maxConcurrentNodes = 1;

        try {
            RecordSet rs = client.query(qp, stmt);   //Use QueryPolicy object instead of null
            while (rs.next()){
              Record r = rs.getRecord();
              System.out.println(r);
            }
        }
        catch(AerospikeException e){
            System.out.println(e.getResultCode());
            System.out.println(e.getClass());
            System.out.println(e.getMessage());
        }
        
    }
}
