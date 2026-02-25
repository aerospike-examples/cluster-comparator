# Quick Reference

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [📋 Use Cases](use-cases.md) | [🏗️ Architecture](architecture.md) | [🔍 Comparison Modes](comparison-modes.md) | [⚙️ Configuration](configuration.md) | [🚨 Troubleshooting](troubleshooting.md) |
|---|---|---|---|---|---|

---

This document provides quick reference information, command patterns, and parameter summaries for the Aerospike Cluster Comparator.

## 📚 Quick Command Patterns

### Health Check
```bash
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 --hosts2 replica:3000 \
  --namespaces production \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console
```

### Find Missing Records
```bash
java -jar cluster-comparator.jar \
  --hosts1 source:3000 --hosts2 target:3000 \
  --namespaces userdata \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file missing.csv \
  --console
```

### Content Validation
```bash
java -jar cluster-comparator.jar \
  --hosts1 source:3000 --hosts2 target:3000 \
  --namespaces app \
  --action scan \
  --compareMode RECORDS_DIFFERENT \
  --file content-diffs.csv \
  --threads 4
```

### Detailed Troubleshooting
```bash
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces production \
  --action scan \
  --compareMode RECORD_DIFFERENCES \
  --file detailed-diffs.csv \
  --limit 1000 \
  --binsOnly
```

### Auto-Fix Missing Records
```bash
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 --hosts2 replica:3000 \
  --namespaces production \
  --action scan_touch \
  --file scan-results.csv
```

## 📋 Complete Command Line Options

### Connection Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-h1`, `--hosts1` | First cluster connection string | `cluster1.com:3000` |
| `-h2`, `--hosts2` | Second cluster connection string | `cluster2.com:3000` |
| `--port1` | Port for first cluster (if not in hosts) | `3000` |
| `--port2` | Port for second cluster (if not in hosts) | `3000` |
| `-t1`, `--tls1` | TLS configuration JSON for cluster 1 | `'{"context":{"certChain":"client.pem"}}'` |
| `-t2`, `--tls2` | TLS configuration JSON for cluster 2 | `'{"context":{"certChain":"client.pem"}}'` |
| `-sa1`, `--useServicesAlternate1` | Use services alternative for cluster 1 | _(flag, no value)_ |
| `-sa2`, `--useServicesAlternate2` | Use services alternative for cluster 2 | _(flag, no value)_ |

### Authentication Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-U1`, `--user1` | Username for first cluster | `readonly` |
| `-P1`, `--password1` | Password for first cluster | `secret123` |
| `-U2`, `--user2` | Username for second cluster | `readonly` |
| `-P2`, `--password2` | Password for second cluster | `secret123` |
| `-a1`, `--authMode1` | Authentication mode for cluster 1 | `INTERNAL` |
| `-a2`, `--authMode2` | Authentication mode for cluster 2 | `INTERNAL` |
| `-cn1`, `--clusterName1` | Expected cluster name for cluster 1 | `production` |
| `-cn2`, `--clusterName2` | Expected cluster name for cluster 2 | `replica` |

### Scope & Filtering Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-n`, `--namespaces` | Comma-separated list of namespaces | `production,staging` |
| `-s`, `--setNames` | Comma-separated list of set names | `users,orders,cache` |
| `-db`, `--beginDate` | Start date for record filtering | `2026/02/17-00:00:00Z` |
| `-de`, `--endDate` | End date for record filtering | `2026/02/18-23:59:59Z` |
| `-df`, `--dateFormat` | Custom date format pattern | `yyyy-MM-dd HH:mm:ss` |
| `-S`, `--startPartition` | Starting partition number | `0` |
| `-E`, `--endPartition` | Ending partition number | `1000` |
| `-pl`, `--partitionList` | Specific partitions to scan (alternative to start/end) | `0,5,10,15` |

### Action & Comparison Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-a`, `--action` | Operation mode (scan, touch, read, etc.) | `scan` |
| `-C`, `--compareMode` | Comparison strategy | `RECORDS_DIFFERENT` |
| `--binsOnly` | Compare only bins (skip metadata) | _(flag, no value)_ |
| `-sm`, `--sortMaps` | Sort map contents for comparison | `true` |

### Configuration & Advanced Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-cf`, `--configFile` | YAML configuration file path | `config.yaml` |
| `-pf`, `--pathOptionsFile` | YAML file for field-specific actions | `path-options.yaml` |
| `--customActions` | Custom actions per cluster on differences (see below) | `1:touch,2:delete` |
| `-sm`, `--sortMaps` | Sort map contents for consistent comparison | `true` |
| `-m`, `--metadataCompare` | Perform metadata-only comparison | _(flag, no value)_ |
| `--skipChallenge` | Skip deletion confirmations | _(flag, no value)_ |

### File & Output Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-f`, `--file` | Output CSV file path | `differences.csv` |
| `-i`, `--inputFile` | Input CSV file (for read/touch actions) | `missing-records.csv` |
| `-c`, `--console` | Display results in console | _(flag, no value)_ |
| `--binsOnly` | Show only bin names that differ (not values) | _(flag, no value)_ |
| `--showMetadata` | Include metadata in output (impacts performance) | _(flag, no value)_ |
| `-q`, `--quiet` | Suppress progress information | _(flag, no value)_ |

### Performance & Limits Options  
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-t`, `--threads` | Number of threads (0 = auto-detect) | `0` |
| `-r`, `--rps` | Rate limit (requests per second) | `1000` |
| `-l`, `--limit` | Maximum records per partition | `10000` |
| `-rl`, `--recordLimit` | Maximum total records to process | `1000000` |
| `-rcs`, `--remoteCacheSize` | Remote server cache size | `5000` |

### Remote Server Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-rs`, `--remoteServer` | Start remote server on port | `8080` |
| `-rst`, `--remoteServerTls` | TLS configuration for remote server | `'{"context":{"certChain":"server.pem"}}'` |
| `-rsh`, `--remoteServerHashes` | Use hashed comparisons in remote mode | `true` |

### Debug & Utility Options
| Option (Short/Long) | Description | Example |
|---------------------|-------------|---------|
| `-D`, `--debug` | Enable debug output | _(flag, no value)_ |
| `-V`, `--verbose` | Enable verbose logging | _(flag, no value)_ |
| `-u`, `--usage` | Show help and exit | _(flag, no value)_ |
| `--version` | Show version and exit | _(flag, no value)_ |

## 📋 Parameter Quick Reference (Common Combinations)

| Purpose | Parameters (Short/Long) | Example Values |
|---------|-------------------------|----------------|
| **Connection** | `-h1`, `-h2` / `--hosts1`, `--hosts2` | `cluster1.com:3000` |
| **Authentication** | `-U1`, `-P1` / `--user1`, `--password1` | `admin`, `secret123` |
| **Scope** | `-n`, `-s` / `--namespaces`, `--setNames` | `prod,staging`, `users,cache` |
| **Performance** | `-t`, `-r` / `--threads`, `--rps` | `0` (auto), `1000` |
| **Time Range** | `-db`, `-de` / `--beginDate`, `--endDate` | `2026/02/17-00:00:00Z` |
| **Limits** | `-l`, `-rl` / `--limit`, `--recordLimit` | `10000`, `1000000` |
| **Output** | `-f`, `-c` / `--file`, `--console` | `differences.csv` |
| **Partitions** | `-S`, `-E` / `--startPartition`, `--endPartition` | `0`, `1000` |

## 🔄 Action Reference

| Action | Input File | Output File | Description |
|--------|------------|-------------|-------------|
| `scan` | ❌ | ✅ | Compare and report differences |
| `scan_touch` | ❌ | ✅ | Compare + automatically touch missing records |
| `scan_read` | ❌ | ✅ | Compare + automatically read missing records |
| `scan_ask` | ❌ | ✅ | Compare + prompt user for action |
| `scan_custom` | ❌ | ✅ | Compare + perform custom actions during scan |
| `touch` | ✅ | ❌ | Touch records from input file |
| `read` | ✅ | ❌ | Read records from input file |
| `rerun` | ✅ | ✅ | Re-compare records from input file |
| `custom` | ✅ | ❌ | Perform custom actions on records from input file |

### Custom Actions (`--customActions`)

Used with `custom` and `scan_custom` actions. Specify per-cluster actions in the format `<clusterId>:<action>`, comma-separated. Cluster IDs are 1-based ordinals or cluster names.

| Custom Action | Description |
|---------------|-------------|
| `NONE` | No action (default) |
| `TOUCH` | Touch the record if it exists on this cluster |
| `DELETE` | Delete the record from this cluster. **DANGEROUS:** deletes propagate via XDR to downstream clusters by default |
| `DURABLE_DELETE` | Same as `DELETE`, but uses durable deletes. Ensures the delete is persisted and not undone the cold start of a node |

```bash
# Delete records from cluster 1 only
--action custom --customActions 1:delete

# Touch on cluster 1, durable delete on cluster 2
--action custom --customActions 1:touch,2:durable_delete

# Using cluster names
--action custom --customActions primary:touch,replica:delete
```

## 🔍 Comparison Mode Reference

| Mode | Speed | Network | Detail | Best For |
|------|-------|---------|--------|----------|
| `QUICK_NAMESPACE` | ⚡⚡⚡ | Minimal | Low | Health checks, bulk validation |
| `MISSING_RECORDS` | ⚡⚡ | Low | Medium | Migration validation, XDR monitoring |
| `RECORDS_DIFFERENT` | ⚡ | Medium | Medium | Content verification, integrity checks |
| `RECORD_DIFFERENCES` | 🐌 | High | High | Detailed debugging, field analysis |
| `FIND_OVERLAP` | ⚡⚡ | Low | Medium | Data partitioning, merge preparation |

## 🌐 Network Architecture Reference

### Standard Mode
```bash
# Single machine with access to both clusters
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --action scan
```

### Remote Server Mode
```bash
# Worker (machine with isolated cluster access)
java -jar cluster-comparator.jar \
  --hosts1 isolated-cluster:3000 \
  --remoteServer 8080

# Controller (machine with accessible cluster)
java -jar cluster-comparator.jar \
  --hosts1 remote:worker-ip:8080 \
  --hosts2 accessible-cluster:3000 \
  --action scan
```

## ⚙️ Configuration File Reference

### Basic Multi-Cluster
```yaml
---
clusters:
- hostName: cluster1:3000
  clusterName: primary
- hostName: cluster2:3000
  clusterName: replica
- hostName: cluster3:3000
  clusterName: backup
```

### With Authentication & TLS
```yaml
---
clusters:
- hostName: secure-cluster:tls1:4333
  clusterName: secure
  userName: readonly
  password: secret
  tls:
    ssl:
      certChain: client.pem
      privateKey: client.key
      caCertChain: ca.pem
```

### Namespace Mapping
```yaml
---
clusters:
- hostName: old-system:3000
  clusterName: legacy
- hostName: new-system:3000
  clusterName: modern

namespaceMapping:
- namespace: old_data
  mappings:
  - clusterName: modern
    name: new_data
```

### Path Options
```yaml
---
paths:
- path: /**/timestamp
  action: ignore
- path: /app/users/tags
  action: compareUnordered
```

## 📊 Output Format Reference

### CSV Output Columns
```csv
namespace,set,digest,difference_type,cluster1,cluster2,bin_name,path,value1,value2,lut1,lut2
```

### Example Output
```csv
prod,users,abc123def456,MISSING,1,2,,,,,1708123456789,
prod,users,def456ghi789,DIFFERENT,1,2,email,/,old@email.com,new@email.com,1708123456789,1708123456790
prod,orders,ghi789abc123,DIFFERENT,1,2,items,/0/quantity,5,6,1708123456789,1708123456789
```

### Difference Types
- `MISSING` - Record exists on one cluster but not another
- `DIFFERENT` - Record exists on both but content differs
- `OVERLAP` - Record exists on both clusters (FIND_OVERLAP mode)

## 🔐 Security Reference

### Environment Variables
```bash
export CLUSTER1_USER=readonly_user
export CLUSTER1_PASSWORD=secure_password
export CLUSTER2_USER=readonly_user
export CLUSTER2_PASSWORD=secure_password

java -jar cluster-comparator.jar \
  --user1 "$CLUSTER1_USER" --password1 "$CLUSTER1_PASSWORD" \
  --user2 "$CLUSTER2_USER" --password2 "$CLUSTER2_PASSWORD" \
  --action scan
```

### TLS Configuration
```bash
# Basic TLS
--tls1 '{"context":{"certChain":"client.pem","privateKey":"key.pem","caCertChain":"ca.pem"}}'

# Advanced TLS with restrictions
--tls1 '{"protocols":["TLSv1.3"],"loginOnly":false,"context":{...}}'
```

## ⚡ Performance Reference

### Thread Configuration
```bash
# Auto-detect: Use one thread per CPU core
--threads 0  # Recommended starting point

# Manual specification based on your environment
--threads 4  # Specify exact number
```

**Factors to consider when choosing thread count:**
- **Cluster capacity**: More threads increase load on clusters
- **Network latency**: Higher latency may benefit from more threads
- **Comparison mode**: `RECORD_DIFFERENCES` is more resource-intensive
- **Available CPU cores**: `--threads 0` uses one thread per core
- **Memory constraints**: Each thread uses additional memory
- **Rate limiting**: Higher thread count may require `--rps` limits

### Rate Limiting
```bash
# Conservative (production clusters)
--rps 500

# Moderate (dedicated comparison)
--rps 2000  

# Aggressive (test environments)
--rps 10000
```

### Memory Settings
```bash
# Standard settings
java -Xmx4g -jar cluster-comparator.jar

# Large dataset settings  
java -Xmx8g -XX:+UseG1GC -jar cluster-comparator.jar

# Memory-constrained environments
java -Xmx2g -jar cluster-comparator.jar --threads 2 --limit 100000
```

## 🚨 Exit Codes Reference

| Exit Code | Meaning | Action |
|-----------|---------|---------|
| `0` | Success | Comparison completed (may include differences) |
| `1` | Configuration error | Check parameters and configuration |
| `2` | Connection error | Verify cluster connectivity |
| `3` | Authentication error | Check credentials and permissions |
| `4` | Runtime error | Check logs and cluster state |

## 📅 Date Format Reference

### Supported Formats
```bash
# Default format
--beginDate "2026/02/17-10:30:00Z"

# Custom format (specify with --dateFormat)
--dateFormat "yyyy-MM-dd HH:mm:ss"
--beginDate "2026-02-17 10:30:00"

# Unix timestamp (milliseconds)
--beginDate 1771833600000

# Relative format (using shell)
--beginDate "$(date -d '1 day ago' '+%Y/%m/%d-%H:%M:%SZ')"
```

## 🔧 Common Flag Combinations

### Development/Testing
```bash
--action scan \
--compareMode RECORD_DIFFERENCES \
--limit 1000 \
--binsOnly \
--console \
--debug
```

### Production Monitoring  
```bash
--action scan \
--compareMode MISSING_RECORDS \
--threads 4 \
--rps 1000 \
--console \
--file monitoring-results.csv
```

### Migration Validation
```bash
--action scan_touch \
--compareMode MISSING_RECORDS \
--threads 8 \
--console \
--file migration-results.csv
```

### Network-Restricted
```bash
--hosts1 remote:worker-host:8080 \
--hosts2 local-cluster:3000 \
--remoteCacheSize 5000 \
--remoteServerHashes true \
--action scan
```

## 🆘 Emergency Commands

### Stop Long-Running Comparison
```bash
# Find process
ps aux | grep cluster-comparator

# Graceful stop (allows cleanup)
kill -TERM <pid>

# Force stop (if graceful doesn't work)
kill -KILL <pid>
```

### Quick Health Check
```bash
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console
```

### Emergency Re-sync
```bash
# Find differences
java -jar cluster-comparator.jar \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file emergency-diffs.csv \
  --limit 10000

# Auto-fix (careful!)
java -jar cluster-comparator.jar \
  --action touch \
  --inputFile emergency-diffs.csv
```

## 📞 Getting Help

### Built-in Help
```bash
# Show all available options
java -jar cluster-comparator.jar --usage

# Test configuration without running comparison
java -jar cluster-comparator.jar \
  --configFile test-config.yaml \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --limit 1
```

### Debug Information
```bash
# Enable debug output for troubleshooting
java -jar cluster-comparator.jar \
  --debug \
  --verbose \
  --action scan \
  --limit 10
```

### Version Information
```bash
# Check version
java -jar cluster-comparator.jar --version

# Or look for version in output header
java -jar cluster-comparator.jar --action scan --console | head -1
```

---

**🏠 Return to [Home](../README.md) for overview and getting started.**