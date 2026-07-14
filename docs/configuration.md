# Configuration

## 📚 Documentation Navigation
| [🏠 Home](../README.md) | [📋 Use Cases](use-cases.md) | [🏗️ Architecture](architecture.md) | [🔍 Comparison Modes](comparison-modes.md) | [🚨 Troubleshooting](troubleshooting.md) | [📋 Reference](reference.md) |
|---|---|---|---|---|---|

---

This document covers configuration files, path options, multi-cluster setups, and advanced configuration scenarios.

## 📝 Configuration Files

### Multi-Cluster Configuration

For comparing 3 or more clusters, use configuration files instead of command-line parameters:

#### Basic Multi-Cluster Setup
```yaml
---
clusters:
- hostName: primary.us-east.com:3000
  clusterName: primary
- hostName: replica.us-west.com:3000
  clusterName: replica
- hostName: backup.eu-west.com:3000
  clusterName: backup
```

```bash
java -jar cluster-comparator.jar \
  --configFile multi-cluster.yaml \
  --namespaces production \
  --action scan \
  --compareMode MISSING_RECORDS
```

**Performance Note:** With N clusters, the tool performs N×(N-1)/2 comparisons. For 4 clusters (A,B,C,D), it compares: A↔B, A↔C, A↔D, B↔C, B↔D, C↔D.

#### Complete Configuration Example
```yaml
---
# Cluster connection details
clusters:
- hostName: secure-primary:tls1:4333
  clusterName: primary
  userName: readonly_user
  password: secure_password
  authMode: INTERNAL
  useServicesAlternate: false
  tls:
    loginOnly: false
    protocols: ["TLSv1.2", "TLSv1.3"]
    ssl:
      certChain: /etc/ssl/client.pem
      privateKey: /etc/ssl/client.key
      caCertChain: /etc/ssl/ca.pem

- hostName: replica-cluster:3000
  clusterName: replica
  userName: readonly_user
  password: replica_password

- hostName: backup-cluster:3000
  clusterName: backup
  userName: readonly_user
  password: backup_password

# Network performance tuning
network:
  query:
    socketTimeout: 30000
    totalTimeout: 120000
    recordQueueSize: 5000
  read:
    socketTimeout: 10000
    totalTimeout: 60000
  write:
    socketTimeout: 10000
    totalTimeout: 60000
```

#### Available Configuration Parameters

**Cluster Parameters:**
- `hostName` - Host and port (required)
- `clusterName` - Logical cluster name (recommended)
- `userName` / `password` - Authentication credentials
- `authMode` - Authentication mode (INTERNAL, EXTERNAL, PKI)
- `useServicesAlternate` - Use alternate access address
- `tls` - TLS configuration object

**TLS Parameters:**
- `protocols` - Allowed TLS versions
- `ciphers` - Allowed cipher suites  
- `revokeCertificates` - Certificate revocation list
- `loginOnly` - Use TLS only for authentication
- `ssl` - SSL certificate configuration

**SSL Certificate Parameters:**
- `certChain` - Client certificate path
- `privateKey` - Private key path
- `caCertChain` - CA certificate path
- `keyPassword` - Private key password

⚠️ **Important:** When using configuration files, you cannot use command-line connection parameters (`--hosts1`, `--user1`, etc.).

## 🌐 Multi-Cluster Configuration
For 3+ clusters, use a config file:

```yaml
---
clusters:
- hostName: primary.dc1.com:3000
  clusterName: primary
  userName: readonly
  password: secret1
- hostName: replica.dc2.com:3000  
  clusterName: replica
  userName: readonly
  password: secret2
- hostName: backup.dc3.com:3000
  clusterName: backup
  userName: readonly
  password: secret3
```

```bash
java -jar cluster-comparator.jar \
  --configFile multi-cluster.yaml \
  --namespaces production \
  --action scan \
  --compareMode MISSING_RECORDS
```

### Namespace Mapping

When equivalent data exists in different namespaces across clusters:

#### Migration Scenario Example
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

**Output:**
```
Namespace "old_customer_data" is known as "customers" on cluster "modern"
Namespace "old_product_data" is known as "catalog" on cluster "modern"
```

#### Multi-Environment Mapping
```yaml
---
clusters:
- hostName: dev-cluster:3000
  clusterName: development
- hostName: staging-cluster:3000  
  clusterName: staging
- hostName: prod-cluster:3000
  clusterName: production

namespaceMapping:
- namespace: app
  mappings:
  - clusterName: development
    name: dev_app
  - clusterName: staging
    name: stage_app
  - clusterName: production
    name: prod_app
```

### Date Range Verification Batch Tuning

When date filtering is active (`--beginDate`/`--endDate`), records that appear to be missing from some clusters are verified by re-reading them without the date filter. These follow-up reads are performed in batches controlled by `--lookupBatchSize`:

```bash
# Default: batch 100 records at a time
--lookupBatchSize 100

# Smaller batches for lower per-node impact
--lookupBatchSize 25

# Larger batches for high-throughput networks
--lookupBatchSize 500
```

**Guidance:**
- The default of 100 is appropriate because all records in a batch target the same server node (same partition).
- Larger batch sizes reduce network round trips but increase per-node load.
- When record content is not needed (e.g., `MISSING_RECORDS` mode), the tool uses `batch exists` instead of `batch get` to minimize network traffic.
- Each partition thread manages its own batch buffer independently.
- To disable verification entirely (report missing records immediately), use `--skipDateRangeVerify`.

### Set Mapping

When equivalent data exists in different sets across clusters (e.g., after a set rename during migration), use `setMapping` to tell the comparator how to resolve set names:

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

This means: when comparing set `users`, records on cluster `replica` should be looked up in set `accounts` instead.

**Requirements:**
- Set mapping requires `--sourceCluster` to identify which cluster to scan (the source side).
- Source records **must** have the user key stored (written with `sendKey = true`). Without the user key, a new digest cannot be computed for the mapped set name and those records will be skipped.
- Set mapping uses the same cluster identification as namespace mapping (either `clusterName` or `clusterIndex`).

**Command example:**
```bash
java -jar cluster-comparator.jar \
  --configFile set-mapping.yaml \
  --namespaces production \
  --setNames users \
  --action scan \
  --compareMode MISSING_RECORDS \
  --sourceCluster 1 \
  --console
```

Set mapping and namespace mapping can be combined in the same configuration file when both set names and namespace names differ across clusters.

### Configuration File Validation

The tool validates configuration files and provides helpful error messages:

```bash
# Test configuration without running comparison
java -jar cluster-comparator.jar \
  --configFile my-config.yaml \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console
```

**Common validation errors:**
- ❌ "Invalid cluster ordinal: 5. Ordinals must be in the range 1->3"
- ❌ "Namespace 'test' mapping refers to unknown cluster 'nonexistent'"
- ❌ "TLS configuration error: certificate file not found"

💡 **Tip:** Always use cluster names instead of numeric indexes for better maintainability.

## 🎯 Advanced Field-Level Control

### Path Options for Fine-Grained Comparison

When comparing complex records with nested data structures, you may want to ignore certain fields or handle lists differently. Use path options files for precise control.

#### Creating Path Options Files

Create a YAML file (e.g., `path-options.yaml`) to control field-level comparison:

```yaml
---
paths:
- path: /userdata/profiles/last_login_timestamp
  action: ignore
- path: /userdata/profiles/preferences/theme_settings
  action: compareUnordered
- path: /**/debug_info
  action: ignore
- path: /metrics/*/counters
  action: compareUnordered
```

Use with:
```bash
java -jar cluster-comparator.jar \
  --pathOptionsFile path-options.yaml \
  --action scan \
  --compareMode RECORD_DIFFERENCES
```

#### Path Syntax

| Path Pattern | Matches | Example |
|--------------|---------|---------|
| `/namespace/set/bin` | Specific bin | `/prod/users/email` |
| `/namespace/set/bin/key` | Map key in bin | `/prod/users/profile/name` |
| `/namespace/set/bin/0` | List index in bin | `/prod/users/tags/0` |
| `/**/field` | Any field named "field" | `/**/timestamp` |
| `/prod/*/temp_data` | Any set's temp_data | `/prod/cache/temp_data` |

#### Actions Available

**`ignore`** - Skip this field completely in comparisons
```yaml
- path: /app/sessions/last_activity
  action: ignore
```
**Use case:** Ignore frequently changing timestamps or debug fields

**`compareUnordered`** - Compare lists without considering order
```yaml
- path: /app/users/favorite_categories
  action: compareUnordered  
```
**Use case:** Lists where order doesn't matter (tags, categories, permissions). So `[1,2,3]` and `[2,3,1]` are considered identical using this option. THe default is that list ordering is important and items which are not the same as the item in the other list at the same position will be marked as different.

#### Real-World Examples

**Ignoring Application-Generated Fields:**
```yaml
---
paths:
# Ignore all timestamp fields
- path: /**/created_at
  action: ignore
- path: /**/updated_at  
  action: ignore
- path: /**/last_accessed
  action: ignore

# Ignore session and temporary data
- path: /app/*/session_data
  action: ignore
- path: /**/temp_*
  action: ignore
  
# Ignore debug and internal fields
- path: /**/debug_info
  action: ignore
- path: /**/internal_id
  action: ignore
```

**Handling Unordered Collections:**
```yaml
---
paths:
# User permissions and roles (order doesn't matter)
- path: /users/*/permissions
  action: compareUnordered
- path: /users/*/roles
  action: compareUnordered

# Product tags and categories
- path: /catalog/products/tags
  action: compareUnordered
- path: /catalog/products/categories
  action: compareUnordered

# Any list field named "items" across all namespaces/sets
- path: /**/items
  action: compareUnordered
```

### Complex Nested Data Example

For a record structure like:
```json
{
  "user_id": "12345",
  "profile": {
    "name": "John Doe",
    "settings": {
      "theme": "dark",
      "notifications": ["email", "push", "sms"]
    },
    "login_history": [
      {"timestamp": "2026-02-17T10:30:00Z", "ip": "192.168.1.1"},
      {"timestamp": "2026-02-16T09:15:00Z", "ip": "192.168.1.2"}
    ]
  },
  "debug_trace": "internal_data"
}
```

Path options file:
```yaml
---
paths:
# Ignore debug information
- path: /users/profiles/debug_trace
  action: ignore

# Notification preferences order doesn't matter  
- path: /users/profiles/profile/settings/notifications
  action: compareUnordered

# Ignore timestamp fields in login history
- path: /users/profiles/profile/login_history/*/timestamp
  action: ignore
```

## 🔧 Essential Parameters

### Connection Parameters
```bash
# Basic connection
--hosts1 cluster1.com:3000
--hosts2 cluster2.com:3000

# With authentication
--hosts1 cluster1.com:3000 --user1 admin --password1 secret
--hosts2 cluster2.com:3000 --user2 admin --password2 secret

# With TLS
--hosts1 secure-cluster:tls1:4333 
--tls1 '{"context":{"certChain":"cert.pem","privateKey":"key.pem","caCertChain":"ca.pem"}}'
```

### Scope Parameters
```bash
# Multiple namespaces
--namespaces namespace1,namespace2,namespace3

# Specific sets only
--setNames users,sessions,cache

# Partition range (for large clusters)
--startPartition 0 --endPartition 1000

# Specific partitions
--partitionList 45,67,89,123
```

### Performance Parameters
```bash
# Concurrency (recommended: start with the default)
--threads 0    # Auto: min(CPU cores, 32)
--threads -1   # Use all CPU cores, with no cap
--threads 8    # Manual: specify exact count

# Rate limiting
--rps 1000  # Limit to 1000 requests per second

# Result limits
--limit 10000  # Stop after finding 10000 differences
--recordLimit 1000000  # Stop after comparing 1M records
```

### Date/Time Filtering
```bash
# Only records updated since a specific date
--beginDate "2026/02/17-00:00:00Z"

# Records updated in specific window
--beginDate "2026/02/01-00:00:00Z" --endDate "2026/02/17-23:59:59Z"

# Unix timestamp format
--beginDate 1771833600000  # Milliseconds since epoch (2026-02-17)
```

## 🔐 Security Configuration

### TLS Configuration Examples

**Basic TLS:**
```bash
--tls1 '{"context":{"certChain":"client.pem","privateKey":"client.key","caCertChain":"ca.pem"}}'
```

**Advanced TLS with protocol restrictions:**
```yaml
clusters:
- hostName: secure-cluster:tls1:4333
  tls:
    protocols: ["TLSv1.3"]
    ciphers: ["TLS_AES_256_GCM_SHA384"]
    loginOnly: false
    ssl:
      certChain: /path/to/client.pem
      privateKey: /path/to/client.key
      caCertChain: /path/to/ca.pem
      keyPassword: certificate_password
```

### Authentication Configuration

**Multiple authentication modes:**
```yaml
clusters:
- hostName: ldap-cluster:3000
  authMode: EXTERNAL
  userName: domain\\user
  password: ldap_password
  
- hostName: pki-cluster:3000
  authMode: PKI
  tls:
    ssl:
      certChain: client_cert.pem
      privateKey: client_key.pem
```

## 📊 Network Performance Tuning

### High-Latency Networks
```yaml
---
network:
  query:
    socketTimeout: 60000      # 60 second socket timeout
    totalTimeout: 300000      # 5 minute total timeout
    recordQueueSize: 10000    # Larger buffer
  read:
    socketTimeout: 30000
    totalTimeout: 120000
  write:
    socketTimeout: 30000
    totalTimeout: 120000
```

### High-Throughput Clusters
```yaml
---
network:
  query:
    socketTimeout: 5000       # Faster timeouts
    totalTimeout: 30000
    recordQueueSize: 1000     # Smaller buffer, faster processing
  read:
    socketTimeout: 2000
    totalTimeout: 10000
```

### Remote Server Optimization
```bash
# High-performance remote server setup
java -jar cluster-comparator.jar \
  --hosts1 remote:worker:8080 \
  --hosts2 local-cluster:3000 \
  --remoteCacheSize 10000 \
  --remoteServerHashes true \
  --sortMaps true \
  --action scan
```

## 🧪 Testing and Validation

### Configuration Testing
```bash
# Test with minimal scope
java -jar cluster-comparator.jar \
  --configFile test-config.yaml \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --limit 1 \
  --console
```

### Incremental Validation
```bash
# 1. Test connection only
java -jar cluster-comparator.jar \
  --configFile my-config.yaml \
  --namespaces test \
  --action scan \
  --compareMode QUICK_NAMESPACE \
  --console

# 2. Test small partition range
java -jar cluster-comparator.jar \
  --configFile my-config.yaml \
  --namespaces production \
  --action scan \
  --startPartition 0 --endPartition 10 \
  --console

# 3. Full comparison
java -jar cluster-comparator.jar \
  --configFile my-config.yaml \
  --namespaces production \
  --action scan \
  --file full-comparison.csv
```

---

**Next:** Learn about [Troubleshooting & Performance](troubleshooting.md) for optimizing your comparisons.