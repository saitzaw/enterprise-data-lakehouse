# Data Quality Implementation Checklist

**Goal**: Deploy clean financial reporting pipeline with data quality controls
**Timeline**: 4 weeks
**Owner**: Data Engineering Team

---

## WEEK 1: Assessment & Testing

### Monday - Assessment
- [ ] Read `SIMPLIFIED_ARCHITECTURE.md` (30 min)
- [ ] Read `DATA_QUALITY_PLAYBOOK.md` (45 min)
- [ ] Understand 26 quality tests overview (30 min)
- [ ] Document current data quality issues
- [ ] Schedule team sync meeting

### Tuesday - Deploy Quality Tests
- [ ] Verify dbt installed: `dbt --version`
- [ ] Deploy Bronze tests: `sap_dbt/tests/bronze/test_bronze_data_quality.sql` ✓
- [ ] Deploy Silver tests: `sap_dbt/tests/silver/test_silver_data_quality.sql` ✓
- [ ] Deploy Gold tests: `sap_dbt/tests/gold/test_gold_data_quality.sql` ✓
- [ ] Run Bronze tests: `dbt test --select tag:bronze_quality`
- [ ] Document all failures in spreadsheet
- [ ] Create issue tickets for each failure

### Wednesday - Bronze Quality Fixes
- [ ] Fix duplicate detection issues
  - [ ] Review `data_cleaning_procedures.sql` Section 1.1
  - [ ] Apply dedup logic
  - [ ] Re-run tests
- [ ] Fix null field issues
  - [ ] Review `data_cleaning_procedures.sql` Section 1.2
  - [ ] Apply filtering
  - [ ] Re-run tests
- [ ] Fix data freshness issues
  - [ ] Check SAP export dates
  - [ ] Validate against source system
- [ ] Fix numeric/date validation issues
  - [ ] Apply bounds checking
  - [ ] Re-run tests

### Thursday - Silver Quality Fixes
- [ ] Run Silver tests: `dbt test --select tag:silver_quality`
- [ ] Fix deduplication issues
  - [ ] Review Silver models in `sap_dbt/models/silver/`
  - [ ] Check ROW_NUMBER() logic
  - [ ] Test again
- [ ] Fix amount reconciliation
  - [ ] Check header vs lines join
  - [ ] Validate calculations
  - [ ] Re-test
- [ ] Fix dimension lookup issues
  - [ ] Ensure customer master loaded
  - [ ] Ensure material master loaded
  - [ ] Run INNER JOIN validation

### Friday - Gold Quality Fixes
- [ ] Run Gold tests: `dbt test --select tag:gold_quality`
- [ ] Fix reconciliation issues
  - [ ] Verify count matches source
  - [ ] Reconcile revenue totals
  - [ ] Document any filtered records
- [ ] Fix dimension completeness
  - [ ] Check for orphaned fact records
  - [ ] Ensure all dimensions populated
- [ ] Fix anomaly detection
  - [ ] Review YoY changes
  - [ ] Flag business logic issues
- [ ] **Goal**: All 26 tests passing ✓

### Friday - Team Sync
- [ ] Review week 1 results with team
- [ ] Prioritize remaining issues
- [ ] Plan week 2 approach

---

## WEEK 2: Dimension Loading & Data Cleaning

### Monday - Load Reference Data
- [ ] Verify PostgreSQL running: `make dev-postgres-shell`
- [ ] Load customer master data (KNA1)
  - [ ] Verify `init-sql/SEED_CRM/02_dml_seed_crm_customers_data.sql`
  - [ ] Run: `psql -f seed_customers.sql`
  - [ ] Verify: `SELECT COUNT(*) FROM customers;`
- [ ] Load material master data (MARA)
  - [ ] Ensure data available from SAP
  - [ ] Load into PostgreSQL or Delta
  - [ ] Verify record count
- [ ] Re-run Silver tests
  - [ ] Check dimension validation tests pass

### Tuesday-Wednesday - Apply Cleaning Procedures
- [ ] Review `data_cleaning_procedures.sql` Section 2 (Silver cleaning)
- [ ] Apply deduplication with SCD2
  - [ ] Test on sample data
  - [ ] Verify change tracking works
  - [ ] Deploy to production
- [ ] Apply amount reconciliation
  - [ ] Create temporary comparison table
  - [ ] Identify and document differences
  - [ ] Resolve with data team/SAP
- [ ] Apply data standardization
  - [ ] Trim whitespace
  - [ ] Uppercase codes
  - [ ] Round amounts
  - [ ] Standardize dates

### Thursday - Validate Cleaned Data
- [ ] Run all Silver tests again: `dbt test --select tag:silver_quality`
- [ ] Target: 9-10/10 tests passing
- [ ] Document any remaining issues
- [ ] Create remediation plan for open issues

### Friday - Prepare for Gold Layer
- [ ] Verify Gold models exist
  - [ ] `sap_dbt/models/gold/fact_sales_orders_ch.sql` ✓
  - [ ] `sap_dbt/models/gold/dim_customers_ch.sql` ✓
  - [ ] `sap_dbt/models/gold/dim_date_ch.sql` ✓
- [ ] Review Gold model logic
  - [ ] Surrogate key calculations
  - [ ] Aggregation logic
  - [ ] Segmentation rules
- [ ] Check ClickHouse configuration
  - [ ] `docker-compose.yml` has ClickHouse service ✓
  - [ ] `clickhouse/config/config.xml` exists ✓
  - [ ] Ports configured (8123, 9000, 9009)

---

## WEEK 3: Gold Layer Materialization

### Monday - Start ClickHouse
- [ ] Start ClickHouse service: `make dev-clickhouse-up`
- [ ] Wait 30 seconds for startup
- [ ] Verify running: `docker compose ps | grep clickhouse`
- [ ] Test connectivity: `make dev-clickhouse-shell`
  - [ ] Inside ClickHouse: `SELECT 1;`
  - [ ] Verify connection works
  - [ ] Exit: `QUIT`

### Tuesday - Run dbt Gold Models
- [ ] Start dbt container: `make dev-dbt-shell`
- [ ] Navigate to dbt project: `cd /opt/airflow/sap_dbt`
- [ ] Run Gold models: `dbt run --select tag:gold --target clickhouse`
  - [ ] Wait for completion (should take < 15 min)
  - [ ] Verify all 3 models created
  - [ ] Check for errors in output
- [ ] If errors:
  - [ ] Review error logs
  - [ ] Check ClickHouse connectivity
  - [ ] Verify profiles.yml has ClickHouse target
  - [ ] Re-run with `--debug` flag

### Wednesday - Verify Data Population
- [ ] Access ClickHouse: `make dev-clickhouse-shell`
- [ ] List tables: `SHOW TABLES;`
- [ ] Count records in fact table:
  ```sql
  SELECT COUNT(*) as row_count FROM fact_sales_orders_ch;
  ```
- [ ] Count customer dimension:
  ```sql
  SELECT COUNT(*) as customer_count FROM dim_customers_ch;
  ```
- [ ] Count date dimension:
  ```sql
  SELECT COUNT(*) as date_count FROM dim_date_ch;
  ```
- [ ] Target: fact_sales_orders_ch > 0, dim_customers_ch > 0, dim_date_ch > 0

### Thursday - Run Gold Quality Tests
- [ ] Execute: `make dev-dbt-test-gold`
- [ ] Or: `dbt test --select tag:gold_quality --target clickhouse`
- [ ] Expected: 8-10/10 tests passing
- [ ] For failures:
  - [ ] Check `DATA_QUALITY_PLAYBOOK.md` for resolution
  - [ ] Apply fixes from `data_cleaning_procedures.sql`
  - [ ] Re-run tests

### Friday - Validation & Reconciliation
- [ ] Run reconciliation queries in ClickHouse
  ```sql
  -- Revenue comparison
  SELECT 
    (SELECT SUM(order_amount) FROM fact_sales_orders_ch) as fact_revenue,
    (SELECT SUM(document_amount) FROM silver.sales_orders) as silver_revenue;
  ```
- [ ] Compare record counts
- [ ] Document any discrepancies
- [ ] Target: < 0.01% variance
- [ ] Generate dbt documentation: `make dev-dbt-docs-gold`

---

## WEEK 4: Deployment & Handoff

### Monday - Airflow DAG Deployment
- [ ] Verify DAG file exists: `dags/gold/clickhouse_gold_transformation.py` ✓
- [ ] Check Airflow recognizes DAG
  - [ ] Run: `make dev-airflow-dags-list`
  - [ ] Look for: `clickhouse_gold_transformation`
- [ ] Trigger DAG manually (test run)
  - [ ] In Airflow UI: http://localhost:8088
  - [ ] Find DAG, click "Trigger DAG"
  - [ ] Monitor execution
  - [ ] Target: All tasks succeed
- [ ] If failures:
  - [ ] Check logs: `docker logs airflow-scheduler`
  - [ ] Fix issues, re-trigger

### Tuesday - Schedule Configuration
- [ ] Edit DAG schedule: `schedule_interval='0 2 * * *'` (Daily 02:00 UTC)
- [ ] Or change to your preferred time
- [ ] Verify ClickHouse starts automatically
- [ ] Verify dbt and test containers available
- [ ] Test dry-run: Verify services can start

### Wednesday - Documentation & Training
- [ ] Update README.md
  - [ ] Reference `SIMPLIFIED_ARCHITECTURE.md`
  - [ ] Add ClickHouse section
  - [ ] Update service list
- [ ] Create Runbook:
  - [ ] Daily startup procedure
  - [ ] Monitoring checklist
  - [ ] Troubleshooting steps
  - [ ] Alert responses
- [ ] Document access credentials
  - [ ] ClickHouse connection string
  - [ ] Airflow UI URL
  - [ ] Usernames & passwords (securely)
- [ ] Schedule team training session

### Thursday - Monitoring Setup
- [ ] Create data quality dashboard
  - [ ] 26 test results (pass/fail)
  - [ ] Row counts by stage
  - [ ] Data freshness metric
  - [ ] Revenue reconciliation
- [ ] Configure alerts
  - [ ] Email on DAG failure
  - [ ] Slack notification on test failures
  - [ ] Warning if data > 24 hours old
- [ ] Test alert delivery
  - [ ] Trigger test failure intentionally
  - [ ] Verify notification received

### Friday - Team Handoff
- [ ] Team training session (1 hour)
  - [ ] Architecture overview
  - [ ] Quality tests explanation
  - [ ] Operations procedures
  - [ ] Troubleshooting guide
  - [ ] Escalation process
- [ ] Assign operational ownership
  - [ ] Daily monitoring: [Person]
  - [ ] On-call support: [Person]
  - [ ] Data quality issues: [Person]
  - [ ] ClickHouse support: [Person]
- [ ] Schedule weekly sync meetings
  - [ ] Review data quality metrics
  - [ ] Discuss any issues
  - [ ] Plan improvements
- [ ] Document handoff completion

---

## Quality Gates Checklist

### Phase 1: Bronze ✓ (Required before Silver)
- [ ] All 6 Bronze quality tests passing
- [ ] No duplicate records
- [ ] All required fields populated
- [ ] Data freshness acceptable
- [ ] Row counts reasonable

### Phase 2: Silver ✓ (Required before Gold)
- [ ] All 10 Silver quality tests passing
- [ ] Perfect header-line reconciliation
- [ ] All customers in master
- [ ] All materials in master
- [ ] No unexpected nulls
- [ ] Data freshness < 24 hours

### Phase 3: Gold ✓ (Required for reporting)
- [ ] All 10 Gold quality tests passing
- [ ] Fact counts match source
- [ ] Revenue totals reconcile exactly
- [ ] All fact records have dimensions
- [ ] No data anomalies
- [ ] Dashboards accurate

---

## Monitoring Checklist (Weekly)

Every Monday morning:
- [ ] Data Quality Scorecard
  - [ ] Bronze: tests passing (6/6)?
  - [ ] Silver: tests passing (10/10)?
  - [ ] Gold: tests passing (10/10)?
- [ ] Data Freshness
  - [ ] Bronze: < 90 days old?
  - [ ] Silver: < 24 hours old?
  - [ ] Gold: < 24 hours old?
- [ ] Volume Check
  - [ ] Bronze: row count reasonable?
  - [ ] Silver: 0 duplicates?
  - [ ] Gold: facts + dimensions populated?
- [ ] Reconciliation
  - [ ] Revenue variance < 0.01%?
  - [ ] Record counts match?
- [ ] Performance
  - [ ] DAG completes < 1 hour?
  - [ ] No long-running tasks?
  - [ ] ClickHouse queries < 2 seconds?

---

## Success Criteria (Sign-Off)

### Technical ✓
- [ ] All 26 quality tests passing
- [ ] Daily DAG executing successfully
- [ ] Data freshness < 24 hours
- [ ] Revenue reconciliation < 0.01% variance
- [ ] ClickHouse queries < 2 seconds
- [ ] Zero unplanned downtime

### Operational ✓
- [ ] Team trained on new pipeline
- [ ] Runbooks documented
- [ ] Alerts configured & tested
- [ ] Monitoring dashboard active
- [ ] On-call procedures established
- [ ] Escalation contacts documented

### Business ✓
- [ ] BI dashboards updated & accurate
- [ ] Data users satisfied with accuracy
- [ ] No data quality complaints
- [ ] Financial reports validated
- [ ] Sign-off from Finance team

---

## Post-Deployment (Month 1+)

### Week 1 Post-Deployment
- [ ] Daily check: All tests passing
- [ ] Review any alerts or issues
- [ ] Fine-tune alert thresholds if needed
- [ ] Gather team feedback

### Week 2 Post-Deployment
- [ ] Verify data users happy with quality
- [ ] Confirm BI dashboards accurate
- [ ] Performance acceptable?
- [ ] Adjust schedule if needed (too early/late?)

### Week 3 Post-Deployment
- [ ] Conduct retrospective meeting
- [ ] Document lessons learned
- [ ] Plan any improvements
- [ ] Update runbooks based on experience

### Month 1 Recap
- [ ] All success criteria met?
- [ ] Users satisfied?
- [ ] Team confident?
- [ ] Ready for scale/enhancement?

---

## Key References

| Document | When to Use |
|----------|------------|
| SIMPLIFIED_ARCHITECTURE.md | Understand overall design |
| DATA_QUALITY_PLAYBOOK.md | Fix test failures |
| data_cleaning_procedures.sql | Apply cleaning logic |
| CLICKHOUSE_EXECUTION_GUIDE.md | Run the pipeline |
| MIGRATION_TO_SIMPLIFIED_PIPELINE.md | Decommission old components |
| QUICKSTART_QUALITY_CONTROL.md | Quick reference |

---

## Sign-Off

**Project Manager**: _________________ Date: _______
**Data Lead**: _________________ Date: _______
**DevOps Lead**: _________________ Date: _______
**Finance Lead**: _________________ Date: _______

---

**Status**: ✅ READY FOR DEPLOYMENT
**Next Action**: Begin Week 1 assessment
