package com.aerospike.comparator;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import com.aerospike.client.Key;
import com.aerospike.client.command.Buffer;
import com.aerospike.comparator.dbaccess.RecordMetadata;

public class ConsoleDifferenceHandler implements MissingRecordHandler, RecordDifferenceHandler {
    private final ClusterNameResolver resolver;
    public ConsoleDifferenceHandler(ClusterNameResolver resolver) {
        this.resolver = resolver;
    }
    
    @Override
    public void handle(int partitionId, Key key, List<Integer> missingFromClusters, boolean hasRecordLevelDifferences, RecordMetadata[] recordMetadatas) throws IOException {
        String recordMetadataDesc = "";
        if (recordMetadatas != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(" LUTS: ");
            for (int i = 0; i < recordMetadatas.length; i++) {
                RecordMetadata metadata = recordMetadatas[i];
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(resolver.clusterIdToName(i)).append("->");
                if (metadata == null) {
                    sb.append("null");
                }
                else {
                    sb.append(metadata.getLastUpdateMs());
                }
            }
            recordMetadataDesc = sb.toString();
        }
        System.out.printf("MISSING RECORD:(%s,%s,%s,%s) Missing from clusters %s%s\n",key.namespace,key.setName, key.userKey, Buffer.bytesToHexString(key.digest), 
                missingFromClusters.stream().map(i->i+1).collect(Collectors.toList()), recordMetadataDesc);
    }

    @Override
    public void handle(int partitionId, Key key, DifferenceCollection differences, List<Integer> missingFromClusters, RecordMetadata[] recordMetadatas)
            throws IOException {
        
        if (differences.isQuickCompare()) {
            System.out.printf("DIFFERENCES: %s,%s,%s,%s\n", key.namespace, key.setName, key.userKey, Buffer.bytesToHexString(key.digest));
        }
        else {
            System.out.printf("DIFFERENCES: %s,%s,%s,%s,%s\n", key.namespace, key.setName, key.userKey, Buffer.bytesToHexString(key.digest), differences.toString());
        }
    }
}
