# cluster-comparator
## Purpose
This is a utility which allows 2 distinct Aerospike clusters to be compared and the results output to a CSV file.

### Usage
```
usage: com.aerospike.comparator.ClusterComparator [<options>]
options:
-a,--action <arg>          Action to take. Options are: 'scan' (scan for differences), 'touch'
                           (touch the records specified in the file), 'read' (read the records in
                           specified file), , 'scan_touch' (scan for differences, if any differences
                           then automatically touch the records), 'scan_read' (scan for differences,
                           if any differences then automatically read the record), 'scan_ask' (scan
                           for differences, if any differences then prompt the user as to whether to
                           touch or read the records or not. Every options besides 'scan' MUST
                           specify the 'file' option too. (Default: scan)
-a1,--authMode1 <arg>      Set the auth mode of cluster1. Default: INTERNAL
-a2,--authMode2 <arg>      Set the auth mode of cluster2. Default: INTERNAL
-C,--compareMode <arg>     Determine which sort of comparison to use. The options are:
                           QUICK_NAMESPACE - Perform a quick (partition by partition count)
                           comparison of an entire namespace. Cannot be used if migrations are going
                           on or not all partitions are available. NOTE:This method compares object
                           counts at a partition level, so is not always accurate. A partition which
                           has record A on one side and record B on the other side would compareas
                           equal for example. Also, since this compares partition by partition,
                           comparison must be at the namesapce level, not the set level.
                           MISSING_RECORDS (default) -- Check digests on both sides to find missing
                           records. Does not compare record contents and does not need to read
                           records off the drive. This is fast but will not detect if the contents
                           of the records are different.
                           RECORDS_DIFFERENT -- Runs through all records and detects both missing
                           records on either side and if record contents themselves are different.
                           This will read all the records off the drives to be able to compare
                           contents. This will only detect that records are different and not show
                           the record differences
                           RECORD_DIFFERENCES -- Similar to RECORDS_DIFFERENT but will
                           comprehensively inspect record pairs to determine the differences and
                           show them.
-c,--console               Output differences to the console. 'quiet' flag does not affect what is
                           output. Can be used in conjunction with 'file' flag for dual output
-cn1,--clusterName1 <arg>  Set the cluster name of cluster 1
-cn2,--clusterName2 <arg>  Set the cluster name of cluster 2
-db,--beginDate <arg>      Specify the begin date of the scan. Any records whose last update time is
                           this time or greater will be included in the scan. The format of the date
                           is by default yyyy/MM/dd-hh:mm:ssZ but can be changed with -df flag. If
                           the parameter is a just a number this will be treated as the epoch since
                           1/1/1970. If the end date is also specified, only records falling between
                           the 2 dates will be scanned. Default: scan from the start of time.
-de,--endDate <arg>        Specify the end date of the scan. Any records whose last update time is
                           less than or equal to this time will be included in the scan. The format
                           of the date is by default yyyy/MM/dd-hh:mm:ssZ but can be changed with
                           -df flag. If the parameter is a just a number this will be treated as the
                           epoch since 1/1/1970. If the start date is also specified, only records
                           falling between the 2 dates will be scanned. Default: scan until the end
                           of time.
-df,--dateFormat <arg>     Format used to convert the dates passed with the -db and -de flags.
                           Should conform to the spec of SimpleDateFormat.
-E,--endPartition <arg>    Partition to end the comparison at. The comparsion will not include this
                           partition. (Default: 4096)
-f,--file <arg>            Path to a CSV file. If a comparison is run, this file will be overwritten
                           if present.
-h1,--hosts1 <arg>         List of seed hosts for first cluster in format:
                           hostname1[:tlsname][:port1],...
                           The tlsname is only used when connecting with a secure TLS enabled
                           server. If the port is not specified, the default port is used. IPv6
                           addresses must be enclosed in square brackets.
                           Default: localhost
                           Examples:
                           host1
                           host1:3000,host2:3000
                           192.168.1.10:cert1:3000,[2001::1111]:cert2:3000
-h2,--hosts2 <arg>         List of seed hosts for second cluster in format:
                           hostname1[:tlsname][:port1],...
                           The tlsname is only used when connecting with a secure TLS enabled
                           server. If the port is not specified, the default port is used. IPv6
                           addresses must be enclosed in square brackets.
                           Default: localhost
                           Examples:
                           host1
                           host1:3000,host2:3000
                           192.168.1.10:cert1:3000,[2001::1111]:cert2:3000
-l,--limit <arg>           Limit the number of differences to the passed value. Pass 0 for
                           unlimited. (Default: 0)
-n,--namespaces <arg>      Namespaces to scan for differences. Multiple namespaces can be specified
                           in a comma-separated list. Must include at least one namespace.
-P1,--password1 <arg>      Password for cluster 1
-P2,--password2 <arg>      Password for cluster 2
-q,--quiet                 Do not output spurious information like progress.
-r,--rps <arg>             Limit requests per second on the cluster to this value. Use 0 for
                           unlimited. (Default: 0)
-S,--startPartition <arg>  Partition to start the comparison at. (Default: 0)
-s,--setNames <arg>        Set name to scan for differences. Multiple sets can be specified in a
                           comma-separated list. If not specified, all sets will be scanned.
-t,--threads <arg>         Number of threads to use. Use 0 to use 1 thread per core. (Default: 1)
-t1,--tls1 <arg>           Set the TLS Policy options on cluster 1. The value passed should be a
                           JSON string. Valid keys in thisstring inlcude 'protocols', 'ciphers',
                           'revokeCerts' and 'loginOnly'
-t2,--tls2 <arg>           Set the TLS Policy options on cluster 2. The value passed should be a
                           JSON string. Valid keys in thisstring inlcude 'protocols', 'ciphers',
                           'revokeCerts' and 'loginOnly'. For example: -tls2
                           '{protocols:"TLSv1,TLSv1.2",loginOnly:false}
-u,--usage                 Display the usage and exit.
-U1,--user1 <arg>          User name for cluster 1
-U2,--user2 <arg>          User name for cluster 2

```
The most significant options are:
* -h1, h2: Specify the clusters to connect to
* -n: The namespace(s) to compare, a comma separated list
* -s: The sets to compare, if desired. This is an optional comma separated list
* -C: The compare mode to use. This can be quick (comparing record counts on a per-partition basis and only compare those that are different) all the way throught to a complete records level comparison)
* -a: The action to take. Should the records be scanned, or scanned and then touched if they're missing for example
* -t: The number of threads. 

## Architecture
This comparator uses the `scanPartitions` method to go through all 4,096 partitions in a cluster and scan them. `scanPartitions` is very useful as it returns the digests for a particular partition in digest order. If we consider comparing a single partition, we have a connection to each one of the 2 clusters and we know that the same record (digest) will always appear in the same parition on both clusters. Since the digests are returned in sorted order, we effectively have 2 very large sorted lists of items we need to compare. 

The internals of the comparison are therefore very easy. This is in the `comparePartition` method whose code resembles:

```java
boolean side2Valid = getNextRecord(recordSet2, 2);
boolean side1Valid = getNextRecord(recordSet1, 1);
while ((side1Valid || side2Valid)) {
    Key key1 = side1Valid ? recordSet1.getKey() : null;
    Key key2 = side2Valid ? recordSet2.getKey() : null;
    if (key1 == null) {
        missingRecord(client2, partitionId, key2, true);
        side2Valid = getNextRecord(recordSet2, 2);
    }
    else if (key2 == null) {
        missingRecord(client1, partitionId,key1, false);
        side1Valid = getNextRecord(recordSet1, 1);
    }
    else {
        int result = compare(key1.digest, key2.digest);
        if (result < 0) {
            // The digests go down as we go through the partition, so if side 2 is > side 1
            // it means that side 1 has missed this one and we need to advance side2
            missingRecord(client2, partitionId, key2, true);
            side2Valid = getNextRecord(recordSet2, 2);
        }
        else if (result > 0) {
            // The digests go down as we go through the partition, so if side 1 is > side 2
            // it means that side 2 has missed this one and we need to advance side1
            missingRecord(client1, partitionId, key1, false);
            side1Valid = getNextRecord(recordSet1, 1);
        }
        else {
            if (options.isRecordLevelCompare()) {
                Record record1 = recordSet1.getRecord();
                Record record2 = recordSet2.getRecord();
                DifferenceSet compareResult = comparator.compare(record1, record2, 
                        options.getCompareMode() == CompareMode.RECORDS_DIFFERENT);
                if (compareResult.areDifferent()) {
                    differentRecords(partitionId, key2, null, null, compareResult);
                }
            }
            // The keys are equal, move on.
            side2Valid = getNextRecord(recordSet2, 2);
            side1Valid = getNextRecord(recordSet1, 1);
        }
    }
}
```

Scanning a single partition requires only a single server-side thread -- there is no parallelization across a partition by multiple query threads. It would be possible for the client to perform a scan of all 4,096 server partitions concurrently which would utilize more server-side query threads but this would make the client more complex as all concurrent scans will be interleaved in the client result set. 

Hence, concurrency is controlled by the client. Each thread will scan one partition before moving onto the next partition. There can be multiple concurrent client threads scanning different partitions, each of which will consume one server-side scan thread for each of the clusters.

**Note**: if you have a node 

### Examples

```
-h1 172.17.0.2 -h2 172.17.0.3 -n test -s cars -a scan -f /tmp/output.csv -c -C RECORD_DIFFERENCES -t 0
```

Compare the `cars` set in the `test` namespace, using one thread per client-side core. Differences are output to `/tmp/output.csv`