# Comparison Modes

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [📋 Use Cases](use-cases.md) | [🏗️ Architecture](architecture.md) | [⚙️ Configuration](configuration.md) | [🚨 Troubleshooting](troubleshooting.md) | [📋 Reference](reference.md) |
|---|---|---|---|---|---|

---

This document explains the different comparison modes and when to use each one.

## 🔍 Comparison Modes Overview

Choose the right comparison mode for your use case:

| Mode | Speed | Detail | Network | Use Case |
|------|-------|--------|---------|----------|
| `QUICK_NAMESPACE` | ⚡⚡⚡ | Low | Minimal | Health checks |
| `MISSING_RECORDS` | ⚡⚡ | Medium | Low | Migration validation |
| `RECORDS_DIFFERENT` | ⚡ | Medium | Medium | Content verification |  
| `RECORD_DIFFERENCES` | 🐌 | High | High | Detailed debugging |
| `FIND_OVERLAP` | ⚡⚡ | Medium | Low | Data partitioning |

## 1. QUICK_NAMESPACE ⚡

**Best for:** Fast health checks and bulk load validation  
**Performance:** Fastest (seconds to minutes)  
**Limitations:** Cannot use with sets, no record-level details

```bash
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 --hosts2 cluster2:3000 \
  --namespaces production \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console
```

### What it does:
- Compares record counts partition by partition
- Uses Aerospike info commands to get partition statistics
- Perfect for quickly verifying if clusters have roughly the same data volume
- Cannot identify which specific records differ

### Example output:
```
Quick record counts:
    cluster 1: (1,234,567 records, 45 tombstones)
    cluster 2: (1,234,520 records, 52 tombstones)
Quick compare found 23 partitions different: [45, 67, 89, ...]
```

### When to use:
- ✅ Post-bulk-load validation
- ✅ High-level health monitoring
- ✅ Quick migration sanity checks
- ❌ When you need to know which records differ
- ❌ With specific sets (namespace-level only)
- ❌ During migrations (data movement invalidates counts)

## 2. MISSING_RECORDS 🔍 (Default)

**Best for:** Data migration validation, XDR monitoring  
**Performance:** Fast (minutes for millions of records)  
**Network:** Minimal (only digests transferred)

```bash
java -jar cluster-comparator.jar \
  --hosts1 source:3000 --hosts2 target:3000 \
  --namespaces userdata \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file missing-records.csv \
  --console
```

### What it does:
- Compares record digests to find missing records
- Doesn't read actual record data, making it very efficient
- Identifies which records exist on which clusters
- Can be combined with actions like `scan_touch` for auto-repair

### Example output:
```csv
namespace,set,digest,difference_type,cluster1,cluster2
userdata,profiles,abc123def456,MISSING,1,2
userdata,sessions,def456ghi789,MISSING,2,1
```

### When to use:
- ✅ Verifying XDR replication completeness
- ✅ Post-migration validation
- ✅ Regular consistency monitoring
- ✅ Finding records for touch/read operations
- ❌ When record content differences matter
- ❌ For debugging data corruption

### Performance characteristics:
```
Throughput: ~100K-1M records/second (depends on cluster)
Network: <1KB per 1000 records
Memory: <100MB for millions of records
```

## 3. RECORDS_DIFFERENT 📊

**Best for:** Content integrity validation  
**Performance:** Slower (reads all records)  
**Detail level:** Detects differences but doesn't show details

```bash
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 --hosts2 replica:3000 \
  --namespaces app \
  --action scan \
  --compareMode RECORDS_DIFFERENT \
  --file content-diffs.csv \
  --threads 4
```

### What it does:
- Reads and compares actual record content using hashes or full comparison
- Identifies both missing records AND records with different content
- Shows that records differ but not the specific differences
- More thorough than MISSING_RECORDS but faster than RECORD_DIFFERENCES

### Example output:
```csv
namespace,set,digest,difference_type,cluster1,cluster2,bin_name
app,users,abc123def456,DIFFERENT,1,2,profile
app,users,def456ghi789,MISSING,1,2,
```

### When to use:
- ✅ Validating data transformation pipelines
- ✅ Ensuring application-level consistency
- ✅ Detecting data corruption
- ✅ Content verification after complex operations
- ❌ When you need to see exactly what differs
- ❌ For large datasets where speed is critical

### Performance characteristics:
```
Throughput: ~10K-100K records/second
Network: Full record transfer or hashes
Memory: Medium (record buffering)
```

## 4. RECORD_DIFFERENCES 🔬

**Best for:** Detailed troubleshooting and debugging  
**Performance:** Slowest (detailed comparison)  
**Detail level:** Shows exactly what fields differ

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

### What it does:
- Performs detailed field-by-field comparison
- Reports specific differences with paths and values
- Supports nested data structures (maps, lists)
- Can be configured to show only bin names or full values

### Example output:
```csv
namespace,set,digest,difference_type,cluster1,cluster2,bin_name,path,value1,value2
prod,users,abc123,DIFFERENT,1,2,last_login,/,2026-01-15,2026-01-16
prod,users,def456,DIFFERENT,1,2,profile,/settings/theme,dark,light
prod,orders,ghi789,DIFFERENT,1,2,items,/0/quantity,5,6
```

### When to use:
- ✅ Debugging application data issues
- ✅ Understanding exactly what changed
- ✅ Validating complex data transformations
- ✅ Troubleshooting XDR conflicts
- ❌ For large-scale comparisons (too slow)
- ❌ When only existence matters

### Performance characteristics:
```
Throughput: ~1K-10K records/second
Network: Full record transfer required
Memory: Higher (detailed comparison structures)
```

### Options for RECORD_DIFFERENCES:
```bash
# Show only bin names that differ (faster)
--binsOnly

# Show metadata (timestamps, sizes)
--showMetadata

# Use path options to ignore specific fields
--pathOptionsFile ignore-timestamps.yaml
```

## 5. FIND_OVERLAP 🤝

**Best for:** Data partitioning and merge operations  
**Performance:** Fast (digest comparison)  
**Use case:** Specialized for data set operations

```bash
java -jar cluster-comparator.jar \
  --hosts1 dataset1:3000 --hosts2 dataset2:3000 \
  --namespaces partition_test \
  --action scan \
  --compareMode FIND_OVERLAP \
  --file overlapping-records.csv
```

### What it does:
- Finds records that exist on ALL clusters (opposite of MISSING_RECORDS)
- Useful for validating data partitioning strategies
- Helps identify merge conflicts before combining datasets

### Example output:
```csv
namespace,set,digest,difference_type,cluster1,cluster2
partition_test,users,abc123def456,OVERLAP,1,2
partition_test,orders,def456ghi789,OVERLAP,1,2
```

### When to use:
- ✅ Preparing non-overlapping data sets for merge
- ✅ Validating data partitioning strategies
- ✅ Finding duplicate data across clusters
- ✅ Merge conflict resolution planning
- ❌ For standard replication validation
- ❌ When looking for missing data

## 🎯 Mode Selection Guide

### By Primary Goal:

**Health Monitoring:**
1. `QUICK_NAMESPACE` - Overall cluster health
2. `MISSING_RECORDS` - Replication monitoring  
3. `RECORDS_DIFFERENT` - Content integrity

**Data Migration:**
1. `QUICK_NAMESPACE` - Initial validation
2. `MISSING_RECORDS` - Detailed validation
3. `RECORD_DIFFERENCES` - Troubleshooting issues

**Debugging:**
1. `RECORDS_DIFFERENT` - Identify problem areas
2. `RECORD_DIFFERENCES` - Detailed analysis
3. Path options - Ignore irrelevant fields

**Data Operations:**
1. `FIND_OVERLAP` - Pre-merge analysis
2. `MISSING_RECORDS` - Post-operation validation
3. Custom actions - Corrective measures

### By Performance Requirements:

**Speed Priority:**
```
QUICK_NAMESPACE → MISSING_RECORDS → FIND_OVERLAP → RECORDS_DIFFERENT → RECORD_DIFFERENCES
```

**Detail Priority:**
```
RECORD_DIFFERENCES → RECORDS_DIFFERENT → MISSING_RECORDS → FIND_OVERLAP → QUICK_NAMESPACE
```

## ⚙️ Action Modes

Control what happens when differences are found:

| Action | Input File Required | Output File | Description |
|--------|-------------------|-------------|-------------|
| `scan` | ❌ | ✅ | Just compare and report (default) |
| `scan_touch` | ❌ | ✅ | Compare, then auto-touch missing records |
| `scan_read` | ❌ | ✅ | Compare, then auto-read missing records |
| `scan_ask` | ❌ | ✅ | Compare, then prompt for action |
| `touch` | ✅ | ❌ | Touch records from previous scan |
| `read` | ✅ | ❌ | Read records from previous scan |
| `rerun` | ✅ | ✅ | Re-compare records from previous scan |
| `custom` | ✅ | ❌ | Custom action per cluster |
| `scan_custom` | ❌ | ✅ | Compare + custom action per cluster during scan |

The `custom` and `scan_custom` actions support per-cluster actions via `--customActions`: `NONE`, `TOUCH`, `DELETE`, and `DURABLE_DELETE`. See [Quick Reference - Custom Actions](reference.md#custom-actions---customactions) for details.

### Workflow Examples

**Two-step process:**
```bash
# Step 1: Identify differences
java -jar cluster-comparator.jar \
  --compareMode MISSING_RECORDS \
  --action scan \
  --file differences.csv

# Step 2: Take corrective action
java -jar cluster-comparator.jar \
  --action touch \
  --inputFile differences.csv
```

**One-step process:**
```bash
# Automatically touch missing records
java -jar cluster-comparator.jar \
  --compareMode MISSING_RECORDS \
  --action scan_touch \
  --file differences.csv
```

## 🔧 Advanced Options

### Date/Time Filtering
Available for all modes except `QUICK_NAMESPACE`:
```bash
# Only records updated since a specific date
--beginDate "2026/02/17-00:00:00Z"

# Records updated in specific window  
--beginDate "2026/02/01-00:00:00Z" --endDate "2026/02/17-23:59:59Z"

# Unix timestamp format
--beginDate 1771833600000  # Milliseconds since epoch (2026-02-17)
```

### Rate Limiting
```bash
# Limit requests per second to avoid overloading clusters
--rps 1000

# Use multiple threads for better throughput
--threads 8
```

### Result Limits  
```bash
# Stop after finding specified number of differences
--limit 10000

# Stop after comparing specified number of records
--recordLimit 1000000
```

---

**Next:** Learn about [Configuration](configuration.md) options for advanced setups.