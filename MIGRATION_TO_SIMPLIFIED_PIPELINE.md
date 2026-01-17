# Migration Guide: Simplify to Financial Reporting Pipeline

**Objective**: Migrate from complex enterprise pipeline to **clean, focused financial reporting** with strict data quality controls.

---

## Phase 1: Assessment (TODAY - Week 1)

### 1.1 Identify What to Keep ✅
- [x] Bronze zone (raw SAP data ingestion)
- [x] Silver zone (Delta Lake with deduplication)
- [x] Gold zone (ClickHouse for analytics)
- [x] dbt transformations
- [x] Airflow orchestration
- [x] PostgreSQL (metadata)
- [x] Sales Order (VBAK, VBAP) + Customer Master (KNA1)
- [x] Data quality tests (26 total)

### 1.2 Identify What to Remove ❌
- [ ] Kafka/Debezium (real-time CDC not needed)
- [ ] MongoDB (use PostgreSQL instead)
- [ ] Elasticsearch (use dbt + ClickHouse)
- [ ] FICO module (GL accounts, only need Sales)
- [ ] COPA module (CO-PA, only need Sales)
- [ ] Jupyter notebooks (dev only, not production)
- [ ] Extra SAP modules (streamline to Sales + Master Data)

### 1.3 Current State Check
```bash
# View current services
docker compose ps

# Count DAGs
find dags/ -name "*.py" | wc -l

# Count SAP tables
find sap_dbt/models/ -name "*.sql" | wc -l

# Check Kafka topics (if in use)
docker exec kafka kafka-topics --list
```

---

## Phase 2: Data Quality Foundation (Week 1-2)

### 2.1 Deploy Quality Tests

**Create test files** (already done):
```bash
✓ sap_dbt/tests/bronze/test_bronze_data_quality.sql (6 tests)
✓ sap_dbt/tests/silver/test_silver_data_quality.sql (10 tests)
✓ sap_dbt/tests/gold/test_gold_data_quality.sql (10 tests)
```

**Run initial tests** (identify current issues):
```bash
make dev-dbt-test-gold

# Check which tests fail
dbt test --select tag:bronze_quality
dbt test --select tag:silver_quality  
dbt test --select tag:gold_quality

# Document failures in spreadsheet
```

### 2.2 Fix Bronze Data Quality Issues

**Common Bronze issues:**
| Issue | Fix | Time |
|-------|-----|------|
| Duplicates | Remove dupes, keep latest | 1 hour |
| Null fields | Filter incomplete records | 30 min |
| Stale data | Re-export from SAP | 2 hours |
| Future dates | Fix SAP system time | SAP team |
| Low row count | Verify SAP export | 1 hour |

**Actions:**
1. Run: `dbt test --select tag:bronze_quality --debug`
2. Note failed tests
3. Check `DATA_QUALITY_PLAYBOOK.md` for fixes
4. Apply fixes using SQL from `data_cleaning_procedures.sql`
5. Re-run tests until all pass

### 2.3 Fix Silver Data Quality Issues

**Common Silver issues:**
| Issue | Fix | Time |
|-------|-----|------|
| Duplicate keys | Check dedup logic | 1 hour |
| Amount mismatch | Reconcile header vs lines | 2 hours |
| Missing dimensions | Load master data first | 1 hour |
| Null amounts | Investigate source | 2 hours |
| Bad dates | Fix date logic in dbt | 1 hour |

**Actions:**
1. Run: `dbt test --select tag:silver_quality --debug`
2. Check Silver models in `sap_dbt/models/silver/`
3. Apply fixes from `data_cleaning_procedures.sql`
4. Re-run until all 10 tests pass

### 2.4 Fix Gold Data Quality Issues

**Common Gold issues:**
| Issue | Fix | Time |
|-------|-----|------|
| Count mismatch | Check filters in dbt | 1 hour |
| Revenue mismatch | Verify calculations | 2 hours |
| Orphaned facts | Check dimension joins | 1 hour |
| Anomalies | Flag business logic | 1 hour |

**Actions:**
1. Run: `dbt test --select tag:gold_quality --debug`
2. Check Gold models in `sap_dbt/models/gold/`
3. Fix reconciliation issues
4. Re-run until all 10 tests pass

**Timeline: Week 1-2**
- Day 1: Identify issues (test runs)
- Days 2-3: Fix Bronze issues
- Days 4-5: Fix Silver issues
- Days 6-7: Fix Gold issues
- Day 8: Validation & sign-off

---

## Phase 3: Remove Unnecessary Components (Week 2-3)

### 3.1 Stop Kafka/Debezium

**Why**: Reporting doesn't need real-time data
- Real-time updates add complexity
- Daily batch refresh sufficient for financial reports
- Removes: Kafka, Zookeeper, Debezium, 3 containers

**Actions:**
```bash
# 1. Stop Kafka services
docker compose down kafka zookeeper debezium

# 2. Remove Kafka DAGs
rm dags/bronze/kafka_cdc_ingestion.py

# 3. Remove Kafka models (if any)
rm sap_dbt/models/bronze/stg_kafka_*.sql

# 4. Remove Kafka configs
rm spark-apps/pyspark/src/kafka_*.py

# 5. Update docker-compose.yml (delete kafka service)
# OR just don't start it: comment out in .env
```

### 3.2 Remove MongoDB

**Why**: Reference data fits in PostgreSQL
- Simpler infrastructure
- No data duplication
- Removes: MongoDB, 1 container

**Actions:**
```bash
# 1. Migrate MongoDB data to PostgreSQL
# Copy ref data to: init-sql/seed_reference_data.sql

# 2. Update dbt connections (use PostgreSQL source)
# Edit: sap_dbt/profiles/profiles.yml

# 3. Stop MongoDB
docker compose down mongo

# 4. Update applications
# Change connection strings: MongoDB → PostgreSQL
```

### 3.3 Remove Elasticsearch

**Why**: Use dbt + ClickHouse for structured queries
- Elasticsearch for log search, not financial data
- ClickHouse provides search + analytics
- Removes: Elasticsearch, Kibana, 2 containers

**Actions:**
```bash
# 1. Stop Elasticsearch
docker compose -f docker-compose.elk.yml down

# 2. Remove ELK configs
rm -rf docker/elasticsearch/
rm docker-compose.elk.yml

# 3. Remove Elasticsearch integrations
# Delete any ES logging configs
```

### 3.4 Simplify SAP Modules

**Current scope**: All of SAP (SD, FICO, COPA, MM, HR, etc.)
**New scope**: Sales (SD) + Master Data only

**Actions:**
```bash
# 1. Keep only these SAP tables:
# Sales: VBAK, VBAP, VBEP
# Customer: KNA1, ADRC, ADR6
# Material: MARA, MARC
# Remove everything else

# 2. Delete unnecessary DAGs
rm dags/bronze/sap_financial_accounting_ingestion.py
rm dags/bronze/sap_copa_ingestion.py
rm dags/bronze/sap_vendor_partner_ingestion.py

# 3. Delete unnecessary dbt models
rm sap_dbt/models/silver/sap_gl_*
rm sap_dbt/models/silver/sap_copa_*

# 4. Update source definition
# Edit: sap_dbt/models/gold/sources.yml
# Remove non-Sales entries
```

### 3.5 Remove Jupyter (from Production)

**Why**: Development tool, not production
- Keep locally for ad-hoc analysis
- Don't run in production Airflow environment
- Removes: Jupyter container, 1 service

**Actions:**
```bash
# 1. Stop Jupyter service
docker compose down jupyter

# 2. Keep notebooks locally (for analysis)
# Keep: notebooks/ folder on dev machine only

# 3. Update docker-compose.yml
# Remove or comment out jupyter service

# 4. Document analysis process
# Update README: "For analysis, use local Jupyter"
```

**Timeline: Week 2-3**
- Days 1-2: Stop Kafka/Debezium
- Days 3: Migrate MongoDB → PostgreSQL
- Days 4: Remove Elasticsearch
- Days 5: Simplify SAP modules
- Days 6: Remove Jupyter
- Days 7: Test streamlined pipeline

---

## Phase 4: Deploy Simplified Pipeline (Week 3)

### 4.1 Update docker-compose.yml

**Remove services:**
```yaml
# DELETE these sections:
- kafka
- zookeeper
- debezium
- mongo
- elasticsearch
- kibana
- jupyter
```

**Keep services:**
```yaml
# KEEP:
- postgres (metadata)
- airflow-webserver, airflow-scheduler
- redis (Celery)
- spark-master, spark-worker
- dbt
- clickhouse (NEW - Gold layer)
- minio (S3 for staging, optional)
```

**Result**: From 16 services → 10 essential services

### 4.2 Update Docker Build

```bash
# Clean up old images
docker compose down -v
docker system prune -a

# Rebuild only needed images
docker compose build --no-cache postgres airflow spark-master dbt clickhouse

# Start fresh
docker compose up -d
```

### 4.3 Deploy New Airflow DAG

**Replace old DAGs with new simplified ones:**
```bash
# Keep: Bronze ingestion (Sales only)
sap_address_ingestion.py → Keep, simplify to KNA1 only
sap_sales_distribution_ingestion.py → Keep

# Replace: All transformation DAGs
# Old: Multiple Kafka, CDC, streaming DAGs
# New: Single daily batch DAG

dags/gold/clickhouse_gold_transformation.py
  ├─ Bronze ingest (Sales tables)
  ├─ Silver transform (cleanup + dedup)
  └─ Gold materialize (ClickHouse)
```

### 4.4 Schedule New Pipeline

**Daily schedule:**
```
02:00 UTC - Airflow starts
02:05 - Bronze ingest + 6 quality tests
02:30 - Silver transform + 10 quality tests
03:00 - Gold materialize + 10 quality tests
03:30 - Done, dashboards ready
```

**Airflow configuration:**
```python
dag = DAG(
    'financial_reporting_pipeline',
    schedule_interval='0 2 * * *',  # Daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['financial', 'reporting', 'daily']
)
```

---

## Phase 5: Validation & Handoff (Week 4)

### 5.1 Data Validation Checklist

**Bronze Zone:**
- [ ] All 6 quality tests pass
- [ ] No duplicate records
- [ ] All required fields populated
- [ ] Data < 90 days old
- [ ] Row counts reasonable

**Silver Zone:**
- [ ] All 10 quality tests pass
- [ ] 0 duplicate records
- [ ] Header-line amount reconciliation perfect
- [ ] All customer lookups successful
- [ ] All material lookups successful

**Gold Zone:**
- [ ] All 10 quality tests pass
- [ ] Fact record counts match source
- [ ] Revenue totals reconcile exactly
- [ ] All fact records have dimensions
- [ ] No unexpected anomalies

### 5.2 Performance Validation

```bash
# Bronze ingestion performance
# Target: Complete in < 15 minutes
docker logs spark-master | grep "Task completed"

# Silver transformation performance
# Target: Complete in < 15 minutes
docker logs dbt | grep "Completed successfully"

# Gold materialization performance
# Target: Complete in < 10 minutes
docker exec clickhouse clickhouse-client --query "SHOW TABLES"
```

### 5.3 Documentation Update

**Update these files:**
- [ ] README.md - Reflect simplified scope
- [ ] QUICKSTART.md - Point to new pipeline
- [ ] Copilot instructions - Remove old components
- [ ] Architecture diagrams - Simplify to 3 zones
- [ ] Runbooks - Update for new DAGs

### 5.4 Team Training

```
Session 1 (30 min): New simplified pipeline
├─ What changed (removed Kafka, Mongo, ES)
├─ Why (unnecessary for reporting)
└─ Benefits (faster, cleaner, easier to maintain)

Session 2 (30 min): Data quality framework
├─ 26 tests across 3 stages
├─ What each test validates
└─ How to fix failures

Session 3 (30 min): Operational procedures
├─ Daily pipeline execution
├─ Monitoring & alerts
└─ Troubleshooting guide

Session 4 (30 min): BI & Reporting
├─ ClickHouse tables available
├─ Query examples
└─ Dashboard access
```

### 5.5 Handoff to Ops

**Provide Ops team with:**
- [ ] Simplified architecture diagram
- [ ] Operational runbook (daily checks)
- [ ] Alert configuration (test failures)
- [ ] Troubleshooting guide
- [ ] Contact list for escalations

---

## Rollback Plan (If Needed)

### If Phase 2 (Quality) Fails
- Keep old pipeline running in parallel
- Fix data quality issues before cutover
- Timeline: +1-2 weeks

### If Phase 3 (Removal) Fails
- Re-add removed components (Kafka, Mongo, etc.)
- Keep simplified pipeline alongside old one
- Timeline: +1 week to stabilize

### If Phase 4 (Deployment) Fails
- Revert to old DAGs
- Keep simplified pipeline for reference
- Timeline: Immediate revert, 1 week to diagnose

---

## Success Metrics

### By End of Migration:
- [ ] All 26 data quality tests passing
- [ ] 0 duplicate records
- [ ] 100% dimension coverage (no orphans)
- [ ] Revenue reconciliation < 0.01% variance
- [ ] Daily pipeline completes in < 1 hour
- [ ] 0 Kafka/MongoDB dependencies
- [ ] 10 Docker services (down from 16)
- [ ] 10 dbt models (down from 30+)

### Ongoing (Monthly):
- [ ] 100% quality test pass rate
- [ ] 0 data anomalies
- [ ] Data freshness < 24 hours
- [ ] BI dashboards up-to-date
- [ ] 0 unplanned downtime

---

## Timeline Summary

| Phase | Week | Tasks | Owner |
|-------|------|-------|-------|
| Assessment | 1 | Identify components to remove | Data Lead |
| Quality | 1-2 | Deploy & fix 26 tests | Data Engineer |
| Removal | 2-3 | Remove Kafka, Mongo, ES | Data Engineer |
| Deployment | 3 | Deploy simplified pipeline | DevOps |
| Validation | 4 | Test & document | Data Lead |

**Total Duration: 4 weeks**

---

## Files to Review

1. **SIMPLIFIED_ARCHITECTURE.md** - New design
2. **DATA_QUALITY_PLAYBOOK.md** - Quality tests & fixes
3. **data_cleaning_procedures.sql** - Cleaning SQL
4. **CLICKHOUSE_EXECUTION_GUIDE.md** - How to run
5. **QUICKSTART_QUALITY_CONTROL.md** - Quick reference

---

## Questions?

**Q: Can we run old and new pipeline in parallel?**
A: Yes, for 1-2 weeks. Use feature flags to switch.

**Q: What if we need real-time data?**
A: Add Kafka back if needed, but not recommended for financial reporting.

**Q: How do we handle FICO/COPA data?**
A: Keep in separate pipeline if needed, don't mix with Sales reporting.

**Q: Will dashboards break?**
A: No, ClickHouse tables same structure. Update data source, no query changes.

**Q: What about historical data?**
A: Migrate to Delta Lake, add `start_date` for SCD2 tracking.

---

## Next Step

**START Phase 1:** Run quality tests today
```bash
dbt test --select tag:bronze_quality
dbt test --select tag:silver_quality
dbt test --select tag:gold_quality
```

Document failures → Use `DATA_QUALITY_PLAYBOOK.md` → Fix → Re-test
