# Aerospike Cluster Comparator

## Overview

The Aerospike Cluster Comparator is a powerful utility for comparing data between two or more Aerospike clusters. It helps you verify data consistency, identify missing or different records, and take corrective actions across distributed environments.

## 🚀 Quick Start

### Basic Comparison
Compare records between two clusters and output differences to a CSV file:

```bash
java -jar cluster-comparator.jar \
  --hosts1 cluster1.example.com:3000 \
  --hosts2 cluster2.example.com:3000 \
  --namespaces test \
  --action scan \
  --file differences.csv \
  --console
```

### Common Use Cases at a Glance

| Scenario | Command | When to Use |
|----------|---------|-------------|
| **Quick Health Check** | `--action scan --compareMode QUICK_NAMESPACE` | Fast partition-level comparison |
| **Find Missing Records** | `--action scan --compareMode MISSING_RECORDS` | Default mode, finds missing records |
| **Verify Data Integrity** | `--action scan --compareMode RECORDS_DIFFERENT` | Detects content differences |
| **Detailed Differences** | `--action scan --compareMode RECORD_DIFFERENCES` | Shows specific field differences |
| **Fix Missing Data** | `--action scan_touch` | Automatically touch missing records |

### Essential Parameters
```bash
# Connection
--hosts1 cluster1:3000 --hosts2 cluster2:3000

# Authentication  
--user1 readonly --password1 secret

# Scope
--namespaces production --setNames users,cache

# Performance
--threads 4 --rps 1000

# Output
--file differences.csv --console
```

## 📚 Documentation

| Document | Description |
|----------|-------------|
| **[Use Cases & Scenarios](docs/use-cases.md)** | Real-world examples and step-by-step workflows |
| **[Architecture & Deployment](docs/architecture.md)** | Network architectures and deployment patterns |
| **[Comparison Modes](docs/comparison-modes.md)** | Detailed explanation of all comparison modes |
| **[Configuration](docs/configuration.md)** | Configuration files, path options, multi-cluster setup |
| **[Troubleshooting & Performance](docs/troubleshooting.md)** | Common issues, optimization, and security |
| **[Quick Reference](docs/reference.md)** | Command patterns, parameters, and examples |

## 🔄 Basic Workflow

1. **Choose your comparison mode** based on your needs:
   - `QUICK_NAMESPACE` for fast health checks
   - `MISSING_RECORDS` for finding missing data (default)
   - `RECORDS_DIFFERENT` for content verification
   - `RECORD_DIFFERENCES` for detailed analysis

2. **Select an action**:
   - `scan` to identify differences
   - `scan_touch` to automatically fix missing records
   - `touch`/`read` to process existing difference files

3. **Configure scope and performance**:
   - Set namespaces and optionally specific sets
   - Adjust threads and rate limiting for your environment

## 🚨 Important Notes

⚠️ **Before Using Delete Actions:**
- Always test with `--limit` first
- Use `scan` action before any destructive operations
- Understand XDR propagation implications
- Consider using `--skipChallenge false` for safety prompts

🔐 **Security Best Practices:**
- Use read-only accounts when possible
- Enable TLS for production environments
- Store credentials in environment variables
- Validate configurations with test runs

## 📋 Quick Command Examples

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

### Auto-Fix Missing Data
```bash
java -jar cluster-comparator.jar \
  --hosts1 primary:3000 --hosts2 replica:3000 \
  --namespaces production \
  --action scan_touch \
  --file results.csv
```

## 🔗 Getting Help

- **All command line options**: See [📋 Complete Command Line Options](docs/reference.md#-complete-command-line-options) for comprehensive list
- **Built-in help**: `java -jar cluster-comparator.jar --usage`
- **Debug mode**: Add `--debug` flag for detailed logging
- **Test configuration**: Use `--limit 10` for small test runs

For detailed documentation on specific topics, see the links above.

---

**📖 For comprehensive examples and advanced usage, start with [Use Cases & Scenarios](docs/use-cases.md)**