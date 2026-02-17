# Troubleshooting & Performance

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [📋 Use Cases](use-cases.md) | [🏗️ Architecture](architecture.md) | [🔍 Comparison Modes](comparison-modes.md) | [⚙️ Configuration](configuration.md) | [📋 Reference](reference.md) |
|---|---|---|---|---|---|

---

This document covers common issues, performance optimization, security best practices, and debugging techniques.

## 🚨 Troubleshooting

### Common Issues and Solutions

#### "No cluster hosts specified"
```bash
# ❌ Missing connection details
java -jar cluster-comparator.jar --namespaces test

# ✅ Correct
java -jar cluster-comparator.jar --hosts1 cluster1:3000 --hosts2 cluster2:3000 --namespaces test --action scan
```

#### "Quick compare cannot be run, passed user does not have authorization"
```bash
# Solution: Use a different comparison mode or grant info permissions
--action scan \
--compareMode MISSING_RECORDS  # Instead of QUICK_NAMESPACE
```

#### "Migrations are happening, quick compare is not available"
```bash
# Wait for migrations to complete, or use a different mode
--action scan \
--compareMode MISSING_RECORDS
```

#### "Connection timeouts" in large comparisons
```bash
# Increase timeouts and use fewer threads
--threads 2
--configFile network-config.yaml

# network-config.yaml:
# ---
# network:
#   query:
#     socketTimeout: 60000
#     totalTimeout: 300000
```

#### Remote server connection issues
```bash
# Verify remote server is listening
telnet remote-host 8080

# Check TLS configuration matches
--remoteServerTls '{"context":{"certChain":"same-cert.pem",...}}'
```

#### Out of memory errors
```bash
# Reduce concurrency and enable remote caching
--threads 2
--remoteCacheSize 100
--limit 50000  # Process in smaller batches
```

### Debugging Commands

#### Test Configuration
```bash
# Verify cluster connectivity
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --limit 1 \
  --console

# Test with debug output
java -jar cluster-comparator.jar \
  --debug \
  --action scan \
  --limit 10 \
  # ... other parameters
```

#### Progressive Debugging
```bash
# 1. Basic connectivity test
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console

# 2. Small scope test
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --startPartition 0 --endPartition 10 \
  --limit 100 \
  --console

# 3. Full comparison
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --file results.csv
```

## ⚡ Performance Optimization

### Performance Guidelines
| Cluster Size | Thread Approach | Expected Duration | Memory Usage |
|--------------|-----------------|------------------|--------------|
| < 1M records | Start with `--threads 0` (auto) | Minutes | < 1GB |
| 1M-10M records | Auto or moderate count | 10-30 minutes | 1-2GB |
| 10M-100M records | Consider cluster capacity | 30min-2hours | 2-4GB |
| > 100M records | Use partition chunking* | Hours | 4-8GB |

*Use `--startPartition`/`--endPartition` to process in batches

**Thread Selection Strategy:**
1. **Start with auto-detection**: `--threads 0` (one per CPU core)
2. **Monitor cluster load**: Reduce if clusters become overwhelmed
3. **Adjust based on results**: Increase if comparisons are slow and clusters can handle more load

### For Large Datasets (millions of records)
```bash
# Start with auto-detection, then adjust based on cluster performance
--threads 0   # Auto-detect cores
--rps 5000    # Rate limit to avoid overloading

# Process in partition chunks for very large datasets
--startPartition 0 --endPartition 1000  # First batch
--startPartition 1000 --endPartition 2000  # Second batch
```

### For Geographically Distributed Clusters
```bash
# Use remote server mode to minimize cross-region traffic
# Enable compression and caching
--remoteCacheSize 10000
--remoteServerHashes true
```

### For High-Throughput Clusters
```bash
# Compare only recent data to reduce load
--beginDate "2026/02/17-00:00:00Z"
--rps 1000  # Limit impact on production

# Use read replicas if available
--hosts1 read-replica1:3000
--hosts2 read-replica2:3000
```

### Network Optimization
```bash
# For cross-region comparisons
--remoteCacheSize 5000  # Reduce round trips
--remoteServerHashes true  # Send hashes instead of full records

# For high-latency networks
--configFile network-optimized.yaml
```

**network-optimized.yaml:**
```yaml
---
network:
  query:
    socketTimeout: 30000
    totalTimeout: 120000
    recordQueueSize: 10000
  read:
    socketTimeout: 10000
    totalTimeout: 60000
```

### Memory Optimization
```bash
# Reduce memory usage for large comparisons
--threads 2   # Lower than auto-detect if memory constrained
--remoteCacheSize 1000  # Smaller cache
--limit 100000  # Process in batches

# JVM tuning for large datasets
java -Xmx8g -XX:+UseG1GC -jar cluster-comparator.jar \
  --threads 0 \  # Start with auto-detect, reduce if needed
  # ... other parameters
```

### CPU Optimization
```bash
# Optimize for CPU-bound operations
--threads 0  # Auto-detect: one thread per CPU core
--sortMaps false  # Disable map sorting unless needed
--compareMode MISSING_RECORDS  # Avoid expensive record comparison
```

## 🔐 Security Best Practices

### Authentication
```bash
# Use dedicated read-only users
--user1 cluster_comparator --password1 readonly_password
--user2 cluster_comparator --password2 readonly_password

# For production: use environment variables
export AS_USER1=readonly_user
export AS_PASSWORD1=secure_password
java -jar cluster-comparator.jar --user1 $AS_USER1 --password1 $AS_PASSWORD1
```

### TLS Configuration
```bash
# Always use TLS for production clusters
--tls1 '{"context":{"certChain":"client.pem","privateKey":"client.key","caCertChain":"ca.pem"}}'

# For remote server mode
--remoteServerTls '{"context":{"certChain":"server.pem","privateKey":"server.key","caCertChain":"ca.pem"}}'
```

### Data Privacy
```bash
# When comparing sensitive data, avoid detailed output
--compareMode MISSING_RECORDS  # Only digests, no content
--binsOnly  # If using RECORD_DIFFERENCES, show only bin names

# Consider using separate comparison environment
# Copy subset of data for validation instead of full production comparison
```

### Network Security
```bash
# Use specific ports and TLS for remote server mode
--remoteServer 8443  # Use non-standard port
--remoteServerTls '{"protocols":["TLSv1.3"]}'  # Latest TLS only

# Firewall considerations - allow specific ports only
# Controller → Worker: port 8443 (or configured port)
# Optional: Heartbeat port for load balancer health checks
```

### Access Control
```bash
# Create dedicated service accounts with minimal permissions
# Required permissions for comparison:
# - read (for record access)
# - info (for QUICK_NAMESPACE mode)
# - No write permissions needed for read-only operations

# Example Aerospike user creation:
# CREATE USER cluster_comparator PASSWORD 'secure_password' ROLES read-only
```

## 🛡️ Safety Features

### Confirmation Prompts
For destructive operations, the tool requires confirmation:
```bash
# This will prompt for confirmation before deleting
--action custom --customActions 1:delete

# To skip confirmation (dangerous!)
--skipChallenge
```

### Dry Run Validation
Always test with limited scope first:
```bash
# Test with small partition range
--startPartition 0 --endPartition 10 --limit 100

# Test with recent data only
--beginDate "$(date -d '1 hour ago' '+%Y/%m/%d-%H:%M:%SZ')" --limit 100
```

### Backup Recommendations
Before any destructive operations:
1. Take cluster snapshots
2. Test on non-production data first
3. Run with `--limit` parameter initially
4. Validate with `scan` action before using `touch` or `delete`

## 📊 Monitoring and Logging

### Progress Monitoring
```bash
# Enable console output for progress tracking
--console

# Use verbose mode for detailed connection information
--verbose

# Enable debug mode for troubleshooting
--debug
```

### Output Interpretation
```bash
# Progress output format:
# 1000ms: [0-4096, remaining 3456, complete:[0-100,200-300]], active threads: 4, 
# records processed: {cluster1: 12,345, cluster2: 12,340} 
# throughput: {last second: 1,200 rps, overall: 1,100 rps}
```

### Log Analysis
```bash
# Redirect output for later analysis
java -jar cluster-comparator.jar \
  --action scan \
  --console \
  2>&1 | tee comparison.log

# Extract key metrics
grep "throughput" comparison.log
grep "ERROR" comparison.log
grep "Missing records" comparison.log
```

## 🔧 Advanced Troubleshooting

### Connection Issues

**TLS Certificate Problems:**
```bash
# Verify certificate chain
openssl verify -CAfile ca.pem client.pem

# Test TLS connection manually
openssl s_client -connect cluster:4333 -cert client.pem -key client.key

# Debug TLS in comparator
--debug --verbose
```

**Network Connectivity:**
```bash
# Test basic connectivity
telnet cluster-host 3000

# Test with timeout
nc -v -w5 cluster-host 3000

# Check DNS resolution
nslookup cluster-host
```

**Authentication Issues:**
```bash
# Test with asinfo tool
asinfo -h cluster-host:3000 -U username -P password -v namespaces

# Check user permissions
asinfo -h cluster-host:3000 -U username -P password -v users
```

### Performance Issues

**Slow Comparison:**
```bash
# Profile with timing
time java -jar cluster-comparator.jar \
  --action scan \
  --compareMode MISSING_RECORDS \
  --limit 10000

# Monitor system resources
top -p $(pgrep -f cluster-comparator)
```

**High Memory Usage:**
```bash
# Monitor JVM memory
java -Xmx4g -XX:+PrintGCDetails -jar cluster-comparator.jar \
  --action scan

# Reduce memory footprint
--threads 2
--remoteCacheSize 100
--limit 50000
```

### Data Consistency Issues

**Hash Collision Investigation (extremely rare):**
```bash
# If hash-based comparison shows false positives, disable hashing
--remoteServerHashes false

# Use full record comparison instead
--compareMode RECORD_DIFFERENCES
--limit 1000
```

**Timestamp-Related Issues:**
```bash
# Account for clock skew between clusters
--pathOptionsFile ignore-timestamps.yaml

# ignore-timestamps.yaml:
# paths:
# - path: /**/last_update_time
#   action: ignore
# - path: /**/created_at
#   action: ignore
```

## ⚠️ Common Pitfalls

### Configuration Mistakes
```bash
# ❌ Don't mix config file and command line cluster params
--configFile clusters.yaml --hosts1 host:3000  # Error!

# ✅ Use config file OR command line, not both
--configFile clusters.yaml  # Correct
# OR
--hosts1 host1:3000 --hosts2 host2:3000  # Correct
```

### Performance Pitfalls
```bash
# ❌ Too many threads can overwhelm clusters
--threads 50  # Likely too high for most environments

# ✅ Start with auto-detection, then tune
--threads 0   # Auto-detect, monitor cluster load
# If clusters are overwhelmed, reduce: --threads 4
# If clusters can handle more: --threads 16

# ❌ No rate limiting on busy clusters  
# (no --rps parameter)

# ✅ Rate limiting to prevent overload
--rps 1000
```

### Security Pitfalls
```bash
# ❌ Passwords in command line (visible in process list)
--password1 secret123

# ✅ Environment variables
export CLUSTER1_PASSWORD=secret123
--password1 "$CLUSTER1_PASSWORD"

# ❌ No TLS on production clusters
--hosts1 prod-cluster:3000

# ✅ TLS enabled
--hosts1 prod-cluster:tls1:4333 --tls1 '{"context":{...}}'
```

## 📋 Troubleshooting Checklist

### Pre-Comparison Checks
- [ ] Cluster connectivity verified
- [ ] Authentication working
- [ ] Appropriate permissions granted
- [ ] TLS certificates valid (if using TLS)
- [ ] Network latency acceptable
- [ ] Sufficient memory available
- [ ] Cluster not under heavy load

### During Comparison
- [ ] Monitor progress output
- [ ] Watch system resources
- [ ] Check for error messages
- [ ] Verify expected throughput
- [ ] Monitor cluster load

### Post-Comparison Analysis
- [ ] Review results file
- [ ] Check for failed partitions
- [ ] Analyze performance metrics
- [ ] Validate unexpected results
- [ ] Plan follow-up actions

---

**Next:** See the [Quick Reference](reference.md) for command patterns and parameter summaries.