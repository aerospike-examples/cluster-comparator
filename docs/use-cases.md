# Use Cases & Scenarios

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [🏗️ Architecture](architecture.md) | [🔍 Comparison Modes](comparison-modes.md) | [⚙️ Configuration](configuration.md) | [🚨 Troubleshooting](troubleshooting.md) | [📋 Reference](reference.md) |
|---|---|---|---|---|---|

---

This document provides real-world scenarios and step-by-step workflows for using the Aerospike Cluster Comparator.

## 📋 Real-World Use Cases

### 1. **Data Migration Verification**
*Scenario: You've migrated data from one cluster to another and need to verify completeness.*

```bash
# Step 1: Find missing records
java -jar cluster-comparator.jar \
  --hosts1 source-cluster:3000 \
  --hosts2 target-cluster:3000 \
  --namespaces production \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file migration-missing.csv \
  --console

# Step 2: If differences found, touch missing records to trigger XDR
java -jar cluster-comparator.jar \
  --hosts1 source-cluster:3000 \
  --hosts2 target-cluster:3000 \
  --action touch \
  --inputFile migration-missing.csv
```

**Why this works:** MISSING_RECORDS mode quickly identifies records that exist in one cluster but not the other, without reading record contents.

### 2. **XDR Replication Validation**
*Scenario: Validate that XDR is working correctly between clusters.*

```bash
# Check for recent data differences
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 \
  --hosts2 replica:3000 \
  --namespaces userdata \
  --action scan \
  --compareMode RECORDS_DIFFERENT \
  --beginDate "2026/02/17-00:00:00Z" \
  --file xdr-differences.csv \
  --threads 4
```

**Why this approach:** RECORDS_DIFFERENT mode verifies both record existence and content integrity, with date filtering to focus on recent changes.

### 3. **Development vs Production Consistency**
*Scenario: Ensure your staging environment has the same data structure as production.*

```bash
# Compare specific sets with detailed differences
java -jar cluster-comparator.jar \
  --hosts1 prod-cluster:3000 \
  --hosts2 staging-cluster:3000 \
  --namespaces app \
  --setNames users,sessions,cache \
  --action scan \
  --compareMode RECORD_DIFFERENCES \
  --file staging-sync.csv \
  --limit 1000  # Limit to first 1000 differences
```

**Best practice:** Use RECORD_DIFFERENCES to see exactly what fields differ between environments.

### 4. **Multi-Datacenter Validation**
*Scenario: Compare data across multiple geographic clusters.*

Create a config file `multi-cluster-config.yaml`:
```yaml
---
clusters:
- hostName: us-east-cluster:3000
  clusterName: us-east
- hostName: us-west-cluster:3000
  clusterName: us-west  
- hostName: eu-cluster:3000
  clusterName: europe
```

```bash
java -jar cluster-comparator.jar \
  --configFile multi-cluster-config.yaml \
  --namespaces global \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file multi-dc-comparison.csv
```

**Why multiple clusters:** Validates data consistency across all your geographic regions simultaneously.

### 5. **Namespace Migration Between Clusters**
*Scenario: Data lives in different namespaces on different clusters.*

Create `namespace-mapping.yaml`:
```yaml
---
clusters:
- hostName: legacy-system:3000
  clusterName: legacy
- hostName: new-system:3000
  clusterName: modern

namespaceMapping:
- namespace: old_customer_data
  mappings:
  - clusterName: modern
    name: customers
    
- namespace: old_product_data  
  mappings:
  - clusterName: modern
    name: catalog
```

**Command:**
```bash
java -jar cluster-comparator.jar \
  --configFile migration-mapping.yaml \
  --namespaces old_customer_data,old_product_data \
  --action scan \
  --compareMode RECORDS_DIFFERENT
```

**When to use:** When your namespace names don't match between clusters but contain equivalent data.

### 6. **Data Cleanup Operations**
*Scenario: Remove duplicate records that exist only on specific clusters.*

```bash
# Step 1: Find records that exist only on cluster1
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 \
  --hosts2 cluster2:3000 \
  --namespaces test \
  --action scan \
  --compareMode MISSING_RECORDS \
  --file cluster1-only.csv

# Step 2: Delete records that exist only on cluster1 (DANGEROUS!)
java -jar cluster-comparator.jar \
  --hosts1 cluster1:3000 \
  --hosts2 cluster2:3000 \
  --action custom \
  --customActions 1:delete \
  --inputFile cluster1-only.csv
```

**⚠️ WARNING:** Delete operations are irreversible and propagate via XDR to downstream clusters by default. Always test first!

To ensure deletes are durable (persisted and not undo by a cold-start of a node), use `DURABLE_DELETE`:
```bash
--action custom --customActions 1:durable_delete --inputFile cluster1-only.csv
```

### 7. **Performance Testing Validation**
*Scenario: Ensure test data was loaded correctly across clusters.*

```bash
# Quick validation of record counts
java -jar cluster-comparator.jar \
  --hosts1 load-target1:3000 \
  --hosts2 load-target2:3000 \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console
```

**Use case:** QUICK_NAMESPACE provides fastest validation for bulk load operations.

### 8. **Validate Recent XDR Replication**
*Scenario: You want to verify that records recently written to a primary cluster have been replicated to the replica.*

```bash
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 --hosts2 replica:3000 \
  --namespaces production \
  --action scan \
  --compareMode MISSING_RECORDS \
  --beginDate "2026/02/16-00:00:00Z" \
  --file recent-replication.csv \
  --console
```

**Why this works:** Both clusters are scanned for records modified since the begin date. If a record is found on one cluster but not the other within the date range, the tool automatically re-reads it from the missing cluster without the date filter. This distinguishes between records that truly don't exist on the other cluster (replication failure) and records that exist but were last updated outside the date range (normal). Only truly missing records are reported.

**Tip:** Use `--skipDateRangeVerify` if you want to skip the follow-up reads and only see what records match the date range on each cluster independently.

### 9. **Cross-Set Comparison**
*Scenario: Data was migrated from set "users" on the primary cluster to set "accounts" on the replica cluster, and you need to verify completeness.*

Create `set-mapping.yaml`:
```yaml
---
clusters:
- hostName: primary:3000
  clusterName: primary
- hostName: replica:3000
  clusterName: replica

setMapping:
- set: users
  mappings:
  - clusterName: replica
    name: accounts
```

```bash
java -jar cluster-comparator.jar \
  --configFile set-mapping.yaml \
  --namespaces production \
  --setNames users \
  --action scan \
  --compareMode MISSING_RECORDS \
  --sourceCluster 1 \
  --file cross-set-results.csv \
  --console
```

**Why this works:** The tool scans set "users" on the source cluster (cluster 1), then for each record it reconstructs a key using the mapped set name "accounts" and looks it up on the replica via batch reads.

**Important:** Cross-set comparison requires that the user key is stored on the source records (written with `sendKey = true`). Without the user key, a new digest cannot be computed for the mapped set name, and those records will be skipped with a warning.

### 10. **Firewall/Network Restricted Environments**
*Scenario: No single machine can reach both clusters.*

**On machine with access to cluster1:**
```bash
# Start remote server
java -jar cluster-comparator.jar \
  --hosts1 cluster1-internal:3000 \
  --remoteServer 8080
```

**On machine with access to cluster2:**
```bash
# Connect to remote server
java -jar cluster-comparator.jar \
  --hosts1 remote:cluster1-machine-ip:8080 \
  --hosts2 cluster2-internal:3000 \
  --namespaces production \
  --compareMode RECORDS_DIFFERENT \
  --file network-restricted.csv
```

**Why this works:** The remote server mode allows comparison across network boundaries where direct connectivity isn't possible.

## 🔄 Common Workflows

### Two-Step Validation Workflow
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

### One-Step Auto-Fix Workflow
```bash
# Automatically touch missing records
java -jar cluster-comparator.jar \
  --compareMode MISSING_RECORDS \
  --action scan_touch \
  --file differences.csv
```

### Progressive Comparison Strategy
```bash
# 1. Start with quick health check
java -jar cluster-comparator.jar \
  --compareMode QUICK_NAMESPACE \
  --action scan \
  --console

# 2. If issues found, drill down with missing records check
java -jar cluster-comparator.jar \
  --compareMode MISSING_RECORDS \
  --action scan \
  --file missing.csv

# 3. If needed, detailed content comparison
java -jar cluster-comparator.jar \
  --compareMode RECORD_DIFFERENCES \
  --action scan \
  --file detailed.csv \
  --limit 1000
```

## 📊 Use Case Decision Matrix

| Goal | Recommended Mode | Action | Performance | Detail Level |
|------|------------------|--------|-------------|--------------|
| **Quick health check** | `QUICK_NAMESPACE` | `scan` | Fastest | Partition-level |
| **Migration validation** | `MISSING_RECORDS` | `scan` | Fast | Record existence |
| **XDR monitoring** | `RECORDS_DIFFERENT` | `scan` | Medium | Content integrity |
| **Data debugging** | `RECORD_DIFFERENCES` | `scan` | Slow | Field-level |
| **Auto-repair** | `MISSING_RECORDS` | `scan_touch` | Fast | + Corrective action |
| **Data cleanup** | `MISSING_RECORDS` | `custom` | Fast | + Custom actions |

## 🎯 Best Practices by Scenario

### Data Migration
- Start with `QUICK_NAMESPACE` for overall validation
- Use `MISSING_RECORDS` for detailed record validation
- Enable `--console` output for progress monitoring
- Use `scan_touch` for automatic XDR triggering

### Production Monitoring
- Use date filtering to focus on recent changes
- Set appropriate `--limit` to avoid overwhelming results
- Use `--threads` based on cluster capacity
- Monitor with `RECORDS_DIFFERENT` for content verification

### Development/Testing
- Use `RECORD_DIFFERENCES` to see exactly what differs
- Limit scope with specific `--setNames`
- Use `--binsOnly` for high-level field comparison
- Test with small `--limit` values first

### Cross-Environment Validation
- Use configuration files for consistent settings
- Implement namespace mapping for different naming conventions
- Use `--showMetadata` for troubleshooting timestamp issues
- Consider `--pathOptionsFile` to ignore environment-specific fields

---

**Next:** Learn about [Architecture & Deployment](architecture.md) options for different network configurations.