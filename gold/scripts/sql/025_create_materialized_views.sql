-- =============================================================================
-- MATERIALIZED VIEWS - Job Task & Timesheet Details
-- =============================================================================
-- Run AFTER 01_create_views.sql, 015_create_materialized_views.sql, and 021_create_materialized_views.sql
-- These views extend job task and timesheet data with details and lookups.
-- For daily refresh use 03_refresh_materialized_views.sql instead.
--
-- Creation order (dependency chain):
--   1. 1_Job_Task_Details_Table_base       (depends on TOCHECK_ClientDetails from 015)
--   2. 1_Job_Task_Details_Table            (depends on #1, key04_task_name, key06_job_table from 01)
--   3. 4_Timesheet_Table_base              (depends on key03_staff_table from 01)
--   4. 4_Timesheet_Table_base_1            (depends on #2, EXCEL06 from 021, #3)
--   5. 4_Timesheet_Table_base_2            (depends on #4, SUPPORT_Invoice_Task_Table from 015)
--   6. 4_Timesheet_Table_base_3            (depends on #5, SUPPORT_Invoice_Task_Table from 015, EXCEL06 from 021)
--   7. 4_Timesheet_Table                   (depends on #6, key06_job_table from 01)
--   8. KEYS_TIME                           (depends on #7)
--   9. KEY02_Job_Task_Staff_ID_base        (depends on #2, #8)
--  10. KEY02_Job_Task_Staff_ID_base_1      (depends on #9, #2, key06_job_table from 01)
--  11. KEY02_Job_Task_Staff_ID_base_2      (depends on #10, key05_task_type from 01)
--  12. KEY02_Job_Task_Staff_ID             (depends on #11)
--  13. SUPPORT_Staff_Leave_Allocation_byDay_base (depends on #12, key01_calendar_date from 01)
--  14. SUPPORT_Staff_Leave_Allocation_byDay_base_1 (depends on #13, #2, EXCEL01_Staff_Workable_Days from 021)
--  15. SUPPORT_Staff_Leave_Allocation_byDay_base_2 (depends on #14)
--  16. SUPPORT_Job_Leave_Task_Details_Table_base (depends on base tables jobtask, jobtaskassignee, jobdetails, TOCHECK_ClientDetails from 015)
--  17. SUPPORT_Job_Leave_Task_Details_Table_base_1 (depends on #16, key06_job_table from 01)
--
-- Note: SUPPORT_Job_Leave_Task_Details_Table, SUPPORT_Staff_Leave_Allocation_byDay are created in 026_create_materialized_views.sql
--
-- Note: View layers 2_Staff_Task_Allocation_byDay_base, 2_Staff_Task_Allocation_byDay_base_1, 2_Staff_Task_Allocation_byDay, and 3_Staff_Performance_Table_base are created in 026_create_materialized_views.sql
--
-- TODO: Optimization refactoring needed (Phase 1 - Quick wins):
--   - Task_Category: Move from base_2 to base_1 (only depends on Task_Name, Client_Name available in base_1)
--   - Month_Time_Invoiced: Precompute in base_3 to avoid 6× subquery re-execution in Month_Invoiced_On
--   - Month_Invoiced_On: Simplify to reference precomputed Month_Time_Invoiced instead of running subquery 6 times
--   - Is_Billable/Billable_Selector: Reference b."Task_Type" directly instead of re-querying (available in base_1)
--
-- KNOWN ISSUES & OPTIMIZATION CANDIDATES (Phase 2 - Strategic refactoring):
--
--   1. DEEP CHAINING: 4_Timesheet_Table is 6 levels deep (base→base_1→base_2→base_3→final)
--      - Challenge: Long refresh cascades, hard to pinpoint which layer introduces bugs
--      - Impact: If layer 4 has error, it corrupts 6+ downstream views
--      - Candidate: Consolidate base_1/base_2/base_3 into 2-3 strategic intermediate views
--      - Affected views: 4_Timesheet_Table_base, base_1, base_2, base_3, final, KEYS_TIME, KEY02_base, KEY02_final
--
--   2. REPEATED SUBQUERIES: Multiple identical lookups across layers
--      - base_1 queries 1_Job_Task_Details_Table 4 times (Task_Name, Client_Name, Task_Type, Task_Completed)
--      - base_2 re-queries Task_Type despite already available from base_1
--      - base_3 executes Month_Invoiced_On subquery 6× within single CASE expression
--      - KEY02_final re-queries 1_Job_Task_Details_Table for Task_Name despite already in base
--      - Impact: Same lookup table scanned dozens of times at refresh
--      - Candidate: Use pre-computed columns from upstream layers instead of inline subqueries
--
--   3. COLUMN DUPLICATION: Core columns repeat through all 10 views
--      - Job_Task_Staff_ID, Job_ID, Task_ID, Staff_Name in every layer
--      - Task_Name appears in 8+ layers
--      - Task_Type in 7+ layers
--      - Client_Name in 6+ layers
--      - Impact: Maintenance nightmare, storage overhead, schema refactoring touches all 10 views
--      - Candidate: Document which layer "owns" each column, eliminate downstream duplication
--
--   4. CIRCULAR DEPENDENCY WORKAROUND: 4_Timesheet_Table doesn't use KEY02[Job_Name]
--      - Reason: Would create cycle (4_Timesheet_Table → KEYS_TIME → KEY02_base → KEY02_final)
--      - Current: 4_Timesheet_Table queries key06_job_table directly
--      - Impact: Job_Name computed twice (both in 4_Timesheet_Table and KEY02), risk of divergence
--      - Candidate: Restructure KEY02 to break cycle, use single trusted source
--
--   5. SPARSE COLUMNS FROM UNION ALL: KEY02_base uses NULLs as placeholders
--      - 1_Job_Task_Details_Table branch: NULL Task_Name
--      - KEYS_TIME branch: NULL Task_Type, StartDateAdjusted, DueDateAdjusted
--      - Impact: Half the rows missing critical columns, downstream re-queries sources
--      - Candidate: Pre-fill sparse columns in upstream views or merge UNION ALL branches
--
--   6. REFRESH CASCADE COSTS: Any upstream change triggers full downstream rebuild
--      - 1_Job_Task_Details_Table_base change → 8 cascading rebuilds
--      - 4_Timesheet_Table_base change → 7 cascading rebuilds
--      - Impact: No parallelization, refresh time compounds, daily updates are slow
--      - Candidate: Evaluate REFRESH MATERIALIZED VIEW CONCURRENTLY strategy
--
--   7. TESTING & VALIDATION COMPLEXITY: Hard to verify correctness across 10 layers
--      - Which layer is source of truth for Task_Name?
--      - If KEY02_base and KEY02_final disagree, which is correct?
--      - Impact: Manual spot-checking unreliable, integration tests must cover all 10 views
--      - Candidate: Add data lineage/traceability tests, document column sources
--
--   8. SOURCE TABLE FRAGMENTATION: Dependencies scattered across 3 files (01, 015, 021)
--      - key03/04/06_* from 01_create_views.sql
--      - TOCHECK_* and SUPPORT_* from 015_create_materialized_views.sql
--      - EXCEL06_* from 021_create_materialized_views.sql
--      - Impact: Hard to trace impact of source changes, refactoring one file may break others
--      - Candidate: Create dependency map, consider consolidating view definitions
--
--   9. KEYS_TIME AS PASS-THROUGH: Step 8 adds no computation, only feeds KEY02_base UNION
--      - It's a simple SELECT from 4_Timesheet_Table with no transformations
--      - Current chain: 4_Timesheet_Table → KEYS_TIME → KEY02_base (3 layers for 1 transformation)
--      - Impact: Extra refresh step, hard to understand architectural purpose
--      - Candidate: Merge KEYS_TIME logic into KEY02_base or eliminate if not needed downstream
--
--  10. INDEX EXPLOSION: ~35-40 indexes across 10 views, all rebuilt on refresh
--      - Each view has 3-4 indexes (Job_Task_Staff_ID, Job_ID, Staff_Name, Date)
--      - Impact: Storage overhead, slower refreshes, index strategy hard to maintain
--      - Candidate: Profile which indexes are actually used, eliminate redundant ones
--
--  11. TYPE CASTING INCONSISTENCIES: Mixed casting strategy across layers
--      - Some columns cast (Job_ID::text), others left as-is
--      - UUID columns sometimes cast to text, sometimes left as UUID
--      - DATE_TRUNC with ::DATE cast inconsistently applied
--      - Impact: Potential index bypass, performance issues, type coercion overhead
--      - Candidate: Standardize type strategy: define what each column's "canonical" type is
--
--  12. NULL PROPAGATION RISK: NULLs flow through 10 layers silently
--      - If Task_Name lookup fails in base_1, stays NULL through base_2/3/final
--      - Impact: Hard to debug, no visibility into "which layer lost the data"
--      - Candidate: Add data quality checks between layers, flag unexpected NULLs in logs
--
-- ADDITIONAL CHALLENGES WITH CURRENT 12-LAYER HIERARCHY:
--
--  13. TASK_TYPE COMPUTED MULTIPLE TIMES: Task_Type undergoes 3 transformations
--      - 4_Timesheet_Table_base_1: Looks up Task_Type from 1_Job_Task_Details_Table (LOOKUPVALUE)
--      - KEY02_Job_Task_Staff_ID_base_2: Computes Task_Type1 by searching key05_task_type (CONCATENATEX pattern match)
--      - KEY02_Job_Task_Staff_ID: Computes Task_Type by choosing between Task_Name and Task_Type1 (IF CONTAINSSTRING)
--      - Impact: Task classification logic spread across 3 views, hard to maintain, redundant computation
--      - Challenge: Each layer operates on previous result, creating dependency on intermediate columns
--      - Candidate: Consolidate Task_Type logic into single view, pre-compute once instead of 3 times
--
--  14. COLUMN PROPAGATION DEPTH: Task_Name propagates through 9+ views
--      - Flow: 1_Job_Task_Details_Table_base → 1_Job_Task_Details_Table → base_1 → base_2 → base_3 → final
--              → KEYS_TIME → KEY02_base → KEY02_base_1 → KEY02_base_2 → KEY02_final
--      - Impact: Single column change requires updating 9 views, massive refactoring cost
--      - Schema fragility: Can't rename/modify columns without cascade updates
--      - Candidate: Pre-compute stable columns early, don't propagate through all layers
--
--  15. DUAL VIEW CONSUMER PROBLEM: Downstream needs columns from BOTH 4_Timesheet_Table AND KEY02
--      - 4_Timesheet_Table provides: Invoice_Number, Month_Time_Invoiced, Recorded_Hours_invoiced, Job_Name
--      - KEY02_Job_Task_Staff_ID provides: Task_Type, Task_Category, Task_Type1, StartDateAdjusted, DueDateAdjusted
--      - Issue: Reports may need both sets of columns but they're split across separate view hierarchies
--      - Current workaround: Must JOIN 4_Timesheet_Table with KEY02_Job_Task_Staff_ID on Job_Task_Staff_ID
--      - Risk: Cartesian products if rows aren't 1:1 matched
--      - Candidate: Create unified "final enriched view" combining both hierarchies
--
--  16. ILIKE PATTERN MATCHING ACROSS LAYERS: Task categorization logic duplicated
--      - Task_Category in KEY02_base_2: ILIKE checks on Task_Name ('Holiday', 'Leave', 'Admin')
--      - Task_Type in KEY02_final: ILIKE checks on Task_Name (for 'Online content' preference)
--      - 4_Timesheet_Table_base_2 also has Task_Category with same ILIKE logic
--      - Impact: Categorization rules scattered, risk of divergence, hard to test
--      - Candidate: Create single "Task Classification" view that owns all task categorization, used by both pipelines
--
--  17. REFRESH WATERFALL EFFECT WITH 12 LAYERS:
--      - Step-by-step sequential refresh:
--        1. Recreate 1_Job_Task_Details_Table_base (depends on jobtask + jobtaskassignee)
--        2. Recreate 1_Job_Task_Details_Table (depends on #1 + key04_task_name + key06_job_table)
--        3. Recreate 4_Timesheet_Table_base (depends on time + key03_staff_table)
--        4. Recreate 4_Timesheet_Table_base_1 (depends on #2 + EXCEL06 + #3)
--        5-7. Cascade through base_2, base_3, 4_Timesheet_Table
--        8. Recreate KEYS_TIME (depends on #7)
--        9. Recreate KEY02_base (depends on #2 + #8)
--        10-12. Cascade through KEY02_base_1, base_2, KEY02_final
--      - All 12 views must be rebuilt even if only 1 source table changed
--      - No parallelization possible due to linear dependency chain
--      - Impact: Daily refresh could take 30+ minutes for 12 sequential rebuilds
--      - Candidate: Split into 2-3 independent pipelines, refresh in parallel
--
--  18. POWER BI/REPORT QUERY COMPLEXITY: Multi-view JOINs required for complete data
--      - Example query for Power BI:
--        ```
--        SELECT
--            ts."Job_Task_Staff_ID", ts."Timesheet_UUID", ts."Date",
--            ts."Recorded_Minutes", ts."Invoice_Number", ts."Recorded_Hours_invoiced",
--            k2."Task_Type", k2."Task_Category", k2."Task_Type1", k2."StartDateAdjusted", k2."DueDateAdjusted"
--        FROM "4_Timesheet_Table" ts
--        LEFT JOIN "KEY02_Job_Task_Staff_ID" k2
--            ON ts."Job_Task_Staff_ID" = k2."Job_Task_Staff_ID"
--        WHERE ts."Date" >= '2024-01-01'
--        ```
--      - Problem: Power BI must write JOINs manually, can't use direct table reference
--      - Solution: Create a unified "Final_Enriched_Timesheet" view combining both
--      - Impact: Without unified view, every report/dashboard must contain JOIN logic
--
--  19. INTERMEDIATE COLUMN BLOAT: Keeping partial columns in base views
--      - KEY02_base has sparse columns (Task_Name NULL when from 1_Job_Task_Details, Task_Type NULL when from KEYS_TIME)
--      - This is intentional but creates rows with 50% of columns as NULL
--      - Impact: Storage inefficiency, indexing less effective, query performance degradation
--      - Candidate: Separate INTO two distinct views (job_task_only and timesheet_only), or pre-fill NULLs
--
--  20. ARCHITECTURAL INSTABILITY: Still has TODO items indicating design is incomplete
--      - Month_Invoiced_On still has 6× subquery re-execution
--      - Task_Category exists in both 4_Timesheet_Table AND KEY02 (should be single source)
--      - Is_Billable logic duplicated across timesheet tables
--      - Impact: Adding Task_Type/Task_Category to KEY02 didn't solve root issues
--      - Candidate: Pause column additions, focus on consolidation and de-duplication first
--
-- =============================================================================
-- VIEW CREATION & QUERY EXECUTION FLOW
-- =============================================================================
--
-- CREATION FLOW (Sequential, must follow dependency order):
--
--   Phase 1: Base Lookup Tables (01_create_views.sql)
--     key03_staff_table, key04_task_name, key05_task_type, key06_job_table
--
--   Phase 2: Invoice & Client Lookups (015_create_materialized_views.sql)
--     TOCHECK_ClientDetails, TOCHECK_Invoice, SUPPORT_Invoice_Task_Table
--
--   Phase 3: Excel Reference Data (021_create_materialized_views.sql)
--     EXCEL06_Staff_Recorded_vs_Invoiced_Hours
--
--   Phase 4A: JOB TASK PIPELINE (025_create_materialized_views.sql - LEFT SIDE)
--     1. DROP IF EXISTS (handles CASCADE dependencies)
--     2. CREATE 1_Job_Task_Details_Table_base
--     3. CREATE 1_Job_Task_Details_Table ← adds Task_Type lookup
--
--   Phase 4B: TIMESHEET PIPELINE (025_create_materialized_views.sql - RIGHT SIDE)
--     1. DROP IF EXISTS (CASCADE)
--     2. CREATE 4_Timesheet_Table_base ← raw timesheet
--     3. CREATE 4_Timesheet_Table_base_1 ← + Task_Name/Client_Name/Task_Type lookups
--     4. CREATE 4_Timesheet_Table_base_2 ← + Invoice/Billable/Task_Category columns
--     5. CREATE 4_Timesheet_Table_base_3 ← + Invoice timing columns
--     6. CREATE 4_Timesheet_Table ← + Recorded_Hours_invoiced & Job_Name
--
--   Phase 4C: KEYS & KEY02 PIPELINE (025_create_materialized_views.sql - UNION BRIDGE)
--     7. CREATE KEYS_TIME ← subset of 4_Timesheet_Table for KEY02 UNION
--     8. CREATE KEY02_Job_Task_Staff_ID_base ← UNION of 1_Job_Task_Details_Table + KEYS_TIME
--     9. CREATE KEY02_Job_Task_Staff_ID_base_1 ← + Task_Name/Client_Name/Job_Name lookups
--    10. CREATE KEY02_Job_Task_Staff_ID_base_2 ← + Task_Category & Task_Type1 (type search)
--    11. CREATE KEY02_Job_Task_Staff_ID ← + Task_Type (with online content preference)
--
-- CURRENT ARCHITECTURE DIAGRAM:
--
--   Base Tables (jobtask, jobtaskassignee, "time", clientdetails, invoice, invoicetask)
--        |
--        ├─── Job Task Path ─────────────────────┐
--        │                                         │
--        ↓                                         │
--   1_Job_Task_Details_Table_base                 │
--        ↓                                         │
--   1_Job_Task_Details_Table ←─────────────┐      │
--        |                                 │      │
--        |    ┌─────────────────────────────┼──────┼─────────────────┐
--        |    |                             │      │                 │
--        ↓    ↓                             ↓      ↓                 ↓
--   4_Timesheet_Table_base ──→ base_1 ──→ base_2 ──→ base_3 ──→ 4_Timesheet_Table
--   (raw timesheet)    (lookups)   (invoice,billable,category)  (final enriched)
--                                                                      |
--                                                                      ↓
--                                                                  KEYS_TIME
--                                                                      |
--        ┌────────────────────────────────────────────────────────────┘
--        |
--        ↓
--   KEY02_Job_Task_Staff_ID_base (UNION: job_task + timesheet)
--        ↓
--   KEY02_Job_Task_Staff_ID_base_1 (+ lookups: Task_Name, Client_Name, Job_Name)
--        ↓
--   KEY02_Job_Task_Staff_ID_base_2 (+ Task_Category, Task_Type1)
--        ↓
--   KEY02_Job_Task_Staff_ID (+ Task_Type with online content preference)
--
-- QUERY PATTERNS FOR DOWNSTREAM CONSUMERS:
--
-- PATTERN 1: Timesheet-focused query (using 4_Timesheet_Table alone)
--   Purpose: Get timesheet records with all enrichment (invoicing, categorization)
--   Use case: Timesheet dashboards, hour tracking reports
--   ```sql
--   SELECT
--       "Date", "Staff_Name", "Task_Name", "Client_Name",
--       "Recorded_Minutes", "Invoice_Number", "Recorded_Hours_invoiced",
--       "Task_Category", "Is_Billable", "Month_Invoiced_On"
--   FROM "4_Timesheet_Table"
--   WHERE "Date" >= '2024-01-01' AND "Task_Category" = 'Billable Tasks'
--   ORDER BY "Date", "Staff_Name"
--   ```
--   Performance: Direct table scan, all columns pre-computed ✓
--   Note: Includes Task_Category (computed in base_2), Task_Type is NOT here
--
-- PATTERN 2: Job task lookup (using KEY02_Job_Task_Staff_ID alone)
--   Purpose: Get unique job-task-staff combinations with task types
--   Use case: Job task master data, resource allocation
--   ```sql
--   SELECT
--       "Job_ID", "Job_Name", "Task_Name", "Staff_Name",
--       "Task_Type", "Task_Category", "Task_Type1",
--       "StartDateAdjusted", "DueDateAdjusted"
--   FROM "KEY02_Job_Task_Staff_ID"
--   WHERE "StartDateAdjusted" >= '2024-01-01'
--   ORDER BY "Job_ID", "Task_Type"
--   ```
--   Performance: Direct table scan, all columns pre-computed ✓
--   Note: Has Task_Type and Task_Category, but NO Invoice/Billable columns
--
-- PATTERN 3: COMBINED query (requires JOIN - PROBLEM CASE)
--   Purpose: Get timesheet data with task details and job classification
--   Use case: Comprehensive reporting, capacity planning, profitability analysis
--   ```sql
--   SELECT
--       ts."Date", ts."Staff_Name", ts."Recorded_Minutes",
--       ts."Invoice_Number", ts."Recorded_Hours_invoiced",
--       ts."Task_Category",
--       k2."Task_Type", k2."Job_Name", k2."StartDateAdjusted", k2."DueDateAdjusted"
--   FROM "4_Timesheet_Table" ts
--   LEFT JOIN "KEY02_Job_Task_Staff_ID" k2
--       ON ts."Job_Task_Staff_ID" = k2."Job_Task_Staff_ID"
--   WHERE ts."Date" >= '2024-01-01'
--   ORDER BY ts."Date"
--   ```
--   Performance: Hash join on Job_Task_Staff_ID (indexed on both sides) ✓
--   Cost: Materialized views pre-computed, but JOIN happens at query time
--   Risk: If rows don't match 1:1, could get row multiplication
--   Limitation: Task_Category exists in BOTH views - which one does report use?
--
-- POWER BI CONNECTION EXAMPLE:
--
--   1. Create two separate Power BI queries:
--      Query1: SELECT * FROM "4_Timesheet_Table"
--      Query2: SELECT * FROM "KEY02_Job_Task_Staff_ID"
--
--   2. Create a relationship in Power BI:
--      Timesheet[Job_Task_Staff_ID] ← many-to-one → KEY02[Job_Task_Staff_ID]
--
--   3. Power BI report visual pulls from both:
--      Rows: Timesheet[Date], Timesheet[Staff_Name]
--      Values: SUM(Timesheet[Recorded_Minutes])
--      Filters: KEY02[Task_Category] = 'Billable Tasks'
--              KEY02[Task_Type] ≠ 'Coaching'
--
--   Problem: Reports must write JOINs logic, Power BI doesn't automatically combine
--            If a field exists in BOTH tables (Task_Category), confusing which to use
--
-- IDEAL PATTERN: Single Unified View (MISSING)
--
--   Proposed view: "Final_Timesheet_Enriched" or "Fact_Timesheet"
--   Would contain:
--     - All 4_Timesheet_Table columns (Date, Recorded_Minutes, Invoice data, etc.)
--     - All KEY02 columns (Task_Type, Job_Name, StartDateAdjusted, Task_Category, etc.)
--   ```sql
--   CREATE MATERIALIZED VIEW "Final_Timesheet_Enriched" AS
--   SELECT
--       ts.*,
--       k2."Task_Type", k2."Task_Category", k2."Task_Type1",
--       k2."Job_Name", k2."StartDateAdjusted", k2."DueDateAdjusted"
--   FROM "4_Timesheet_Table" ts
--   LEFT JOIN "KEY02_Job_Task_Staff_ID" k2
--       ON ts."Job_Task_Staff_ID" = k2."Job_Task_Staff_ID"
--   ```
--   Benefits:
--     - Single view for Power BI/reports to query
--     - No JOIN logic in reports, all pre-computed
--     - Single source of truth for all timesheet data
--     - Eliminates Task_Category duplication
--     - Reduces query complexity
--   Cost:
--     - Adds one more materialized view
--     - Materialization cost (but worth it for 100+ report queries)
--
-- REFRESH EXECUTION TIMELINE:
--
--   Current 12-layer sequential refresh:
--   [1_base] ─→ [1] ─────┐
--                         ├─→ [base1] ─→ [base2] ─→ [base3] ─→ [final] ─→ [KEYS] ─→ [KEY02_base] ─→ [KEY02_1] ─→ [KEY02_2] ─→ [KEY02]
--                         ├─→ [4_base] ┘
--                         └─→ [lookups] (already created)
--
--   Estimated timing:
--     - 1_Job_Task_Details_Table_base: ~2-5s (1000s of rows)
--     - 1_Job_Task_Details_Table: ~5-10s (with key04/key06 lookups)
--     - 4_Timesheet_Table_base: ~10-15s (100k+ rows)
--     - 4_Timesheet_Table_base_1: ~30-60s (4 subquery lookups per row)
--     - 4_Timesheet_Table_base_2: ~30-60s (Invoice lookups)
--     - 4_Timesheet_Table_base_3: ~60-120s (Month_Invoiced_On with 6× subqueries)
--     - 4_Timesheet_Table: ~10-15s (simple aggregations)
--     - KEYS_TIME: ~5-10s (simple SELECT)
--     - KEY02_base: ~20-30s (UNION + DISTINCT)
--     - KEY02_base_1: ~20-30s (3 lookup subqueries)
--     - KEY02_base_2: ~30-60s (STRING_AGG over key05_task_type per row)
--     - KEY02_final: ~10-15s (CASE expressions)
--   Total: ~275-500 seconds (5-8 minutes) for full refresh
--   Problem: Every daily refresh includes this overhead
--
-- RECOMMENDATIONS FOR IMPROVEMENT:
--
--   Short-term (fix current TODOs):
--     1. Fix Month_Invoiced_On 6× subquery → eliminate wasteful re-execution
--     2. Consolidate Task_Category into single source (move out of 4_Timesheet to KEY02 or vice versa)
--     3. Use b."Task_Type" directly instead of re-querying (already available from base_1)
--
--   Medium-term (architectural):
--     4. Create unified "Final_Timesheet_Enriched" view combining both pipelines
--     5. Split into 2-3 independent refresh jobs (can run in parallel)
--     6. Eliminate KEYS_TIME pass-through, merge logic into KEY02_base
--
--   Long-term (redesign):
--     7. Reduce to 4-5 strategic views instead of 12 (consolidate base_* layers)
--     8. Move computation earlier (don't re-compute Task_Type 3 times)
--     9. Create data lineage tests to catch silent NULL propagation
--    10. Document "column ownership" for each view (which layer owns Task_Name, etc.)
-- 1_Job_Task_Details_Table_base
-- DAX equivalent: 1_Job_Task_Details_Table_base
-- Base view for job task details with staff assignment and client information.
-- Dependencies: TOCHECK_ClientDetails (from 015_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table_base" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table_base" AS
SELECT
    (jt."JobDetailsRemoteID"::text || jt."UUID"::text || jta."UUID"::text) AS "Job_Task_Staff_ID",
    jt."JobDetailsRemoteID"::text AS "Job_ID",
    jt."UUID" AS "Task_UUID",
    jt."Name" AS "Task_Name",
    jt."EstimatedMinutes",
    jt."ActualMinutes",
    jt."Billable" AS "Task_Billable",
    jt."Completed" AS "Task_Completed",
    jta."UUID" AS "Staff_UUID",
    jta."Name" AS "Staff_Name",
    cd."Name" AS "Client_Name",
    jt."StartDate",
    jt."DueDate",
    jta."AllocatedMinutes"::float AS "Task_Allocated_Mins"
FROM
    jobtask jt
    LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."RemoteID"::uuid
    LEFT JOIN jobdetails jd ON jd."RemoteID" = jt."JobDetailsRemoteID"
    LEFT JOIN TOCHECK_ClientDetails cd ON cd."UUID" = jd."ClientUUID"::uuid
WHERE
    jt."IsDeleted" = FALSE
    AND jta."Name" IS NOT NULL
    AND jta."Name" NOT IN (
        'Anna Williams',
        'Conor Cameron',
        'Conor O''Brien',
        'Dinniss',
        'Sahar Sedaghat',
        'The OLD - Dani Millar'
    );


CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table_base" ("Staff_Name");


-- 1_Job_Task_Details_Table
-- DAX equivalent: 1_Job_Task_Details_Table
-- Extended view with task type and adjusted date columns.
-- Dependencies: 1_Job_Task_Details_Table_base, key04_task_name, key06_job_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "1_Job_Task_Details_Table" CASCADE;


CREATE MATERIALIZED VIEW "1_Job_Task_Details_Table" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_UUID",
    b."Task_Name",
    -- Task_Type: LOOKUPVALUE(KEY04_Task_Name[Task_Type], KEY04_Task_Name[Task_Name], Task_Name)
    kt."Task_Type",
    b."EstimatedMinutes",
    b."ActualMinutes",
    b."Task_Billable",
    b."Task_Completed",
    b."Staff_UUID",
    b."Staff_Name",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    -- StartDateAdjusted: IF(ISBLANK(StartDate), LOOKUPVALUE(KEY06_Job_Table[StartDate], ...), StartDate)
    COALESCE(b."StartDate", kj."StartDate") AS "StartDateAdjusted",
    -- DueDateAdjusted: IF(ISBLANK(DueDate), LOOKUPVALUE(KEY06_Job_Table[EarlierDate], ...), DueDate)
    COALESCE(b."DueDate", kj."EarlierDate") AS "DueDateAdjusted"
FROM
    "1_Job_Task_Details_Table_base" b
    LEFT JOIN (
        SELECT DISTINCT ON ("Task_Name")
            "Task_Name",
            "Task_Type"
        FROM key04_task_name
        ORDER BY "Task_Name"
    ) kt ON kt."Task_Name" = b."Task_Name"
    LEFT JOIN (
        SELECT DISTINCT ON ("Job_ID")
            "Job_ID",
            "StartDate",
            "EarlierDate"
        FROM key06_job_table
        ORDER BY "Job_ID"
    ) kj ON kj."Job_ID" = b."Job_ID";


CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Job_ID");
CREATE INDEX ON "1_Job_Task_Details_Table" ("Staff_Name");


-- 4_Timesheet_Table_base
-- DAX equivalent: 4_Timesheet_Table_base
-- Timesheet data joined with staff names, filtered from 2020 onwards.
-- Dependencies: key03_staff_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base" AS
SELECT
    (t."JobID"::text || t."TaskUUID"::text || t."StaffMemberUUID"::text) AS "Job_Task_Staff_ID",
    t."UUID" AS "Timesheet_UUID",
    t."JobID"::text AS "Job_ID",
    t."TaskUUID" AS "Task_ID",
    t."StaffMemberUUID" AS "Staff_ID",
    s."Staff_Name",
    t."Note" AS "Timesheet_Notes",
    t."Billable",
    t."InvoiceUUID" AS "Invoice_ID",
    t."InvoiceTaskUUID" AS "Invoice_Task_ID",
    t."Date",
    t."Minutes" AS "Recorded_Minutes"
FROM
    "time" t
    LEFT JOIN key03_staff_table s ON s."Staff_UUID" = t."StaffMemberUUID"
WHERE
    t."Date" >= '2020-01-01'::timestamp
    AND s."Staff_Name" IS NOT NULL;


CREATE INDEX ON "4_Timesheet_Table_base" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base" ("Date");


-- 4_Timesheet_Table_base_1
-- DAX equivalent: 4_Timesheet_Table (extended version)
-- Extended timesheet view with task name lookup from job details and Excel fallback.
-- Dependencies: 4_Timesheet_Table_base, 1_Job_Task_Details_Table, EXCEL06_Staff_Recorded_vs_Invoiced_Hours (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_1" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_1" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Task_Name"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Task"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Task_Name",
    -- Client_Name: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Client_Name"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Client"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Client_Name",
    -- Task_Type: LOOKUPVALUE from 1_Job_Task_Details_Table, fallback to EXCEL06 if blank
    COALESCE(
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Task_Type"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1),
        (SELECT DISTINCT ON ("Timesheet_UUID")
            "Task"
         FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
         WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
         ORDER BY "Timesheet_UUID"
         LIMIT 1)
    ) AS "Task_Type",
    -- Task_Completed: LOOKUPVALUE from 1_Job_Task_Details_Table
    (SELECT DISTINCT ON ("Job_Task_Staff_ID")
        "Task_Completed"
     FROM "1_Job_Task_Details_Table"
     WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
     ORDER BY "Job_Task_Staff_ID"
     LIMIT 1) AS "Task_Completed",
    -- Is_Client: TRUE if not (Dinniss Admin client OR Admin - Non-billable task)
    NOT (
        COALESCE(
            (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                "Client_Name"
             FROM "1_Job_Task_Details_Table"
             WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
             ORDER BY "Job_Task_Staff_ID"
             LIMIT 1),
            (SELECT DISTINCT ON ("Timesheet_UUID")
                "Client"
             FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
             WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
             ORDER BY "Timesheet_UUID"
             LIMIT 1)
        ) = 'Dinniss Admin'
        OR
        COALESCE(
            (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                "Task_Type"
             FROM "1_Job_Task_Details_Table"
             WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
             ORDER BY "Job_Task_Staff_ID"
             LIMIT 1),
            (SELECT DISTINCT ON ("Timesheet_UUID")
                "Task"
             FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
             WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
             ORDER BY "Timesheet_UUID"
             LIMIT 1)
        ) = 'Admin - Non-billable'
    ) AS "Is_Client"
FROM
    "4_Timesheet_Table_base" b;


CREATE INDEX ON "4_Timesheet_Table_base_1" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_1" ("Date");


-- 4_Timesheet_Table_base_2
-- DAX equivalent: 4_Timesheet_Table (with invoicing, billability, and categorization columns)
-- Extended timesheet view with invoicing status, billability classification, and task categorization.
-- Dependencies: 4_Timesheet_Table_base_1, SUPPORT_Invoice_Task_Table (from 015_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_2" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_2" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    -- Invoiced_Time: Categorizes timesheet based on invoice status and client type
    CASE
        WHEN b."Invoice_Task_ID" IS NULL AND b."Is_Client" = TRUE THEN 'Un-Invoiced'
        WHEN b."Invoice_Task_ID" IS NULL THEN 'Dinniss Time'
        ELSE 'Invoiced'
    END AS "Invoiced_Time",
    -- Is_Billable: FALSE if not client work OR coaching task, else TRUE
    CASE
        WHEN b."Is_Client" = FALSE
            OR COALESCE(
                (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                    "Task_Type"
                 FROM "1_Job_Task_Details_Table"
                 WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                 ORDER BY "Job_Task_Staff_ID"
                 LIMIT 1),
                (SELECT DISTINCT ON ("Timesheet_UUID")
                    "Task"
                 FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
                 WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
                 ORDER BY "Timesheet_UUID"
                 LIMIT 1)
            ) = 'Coaching'
        THEN FALSE
        ELSE TRUE
    END AS "Is_Billable",
    -- Billable_Selector: Simple categorization based on Is_Billable logic
    CASE
        WHEN b."Is_Client" = FALSE
            OR COALESCE(
                (SELECT DISTINCT ON ("Job_Task_Staff_ID")
                    "Task_Type"
                 FROM "1_Job_Task_Details_Table"
                 WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
                 ORDER BY "Job_Task_Staff_ID"
                 LIMIT 1),
                (SELECT DISTINCT ON ("Timesheet_UUID")
                    "Task"
                 FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
                 WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
                 ORDER BY "Timesheet_UUID"
                 LIMIT 1)
            ) = 'Coaching'
        THEN 'Not Billable'
        ELSE 'Billable'
    END AS "Billable_Selector",
    -- Task_Category: Categorizes based on task name content and client type
    CASE
        WHEN (
            b."Task_Name" ILIKE '%Holiday%'
            OR b."Task_Name" ILIKE '%Other leave%'
        ) THEN 'Leave Tasks'
        WHEN b."Task_Name" ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN (
            b."Task_Name" ILIKE '%Admin - Non-billable%'
            OR b."Client_Name" = 'Dinniss Admin'
        ) THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Invoice_Number: Lookup invoice ID from SUPPORT_Invoice_Task_Table
    (SELECT DISTINCT ON ("Invoice_Task_ID")
        "InvoiceID"
     FROM SUPPORT_Invoice_Task_Table
     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
     ORDER BY "Invoice_Task_ID"
     LIMIT 1) AS "Invoice_Number",
    -- Month_Time_Recorded: Month of timesheet date
    DATE_TRUNC('month', b."Date")::DATE AS "Month_Time_Recorded"
FROM
    "4_Timesheet_Table_base_1" b;


CREATE INDEX ON "4_Timesheet_Table_base_2" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_2" ("Date");


-- 4_Timesheet_Table_base_3
-- DAX equivalent: 4_Timesheet_Table (with invoice timing columns)
-- Timesheet view with invoice month tracking and invoiced minutes reconciliation.
-- Dependencies: 4_Timesheet_Table_base_2, SUPPORT_Invoice_Task_Table (from 015_create_materialized_views.sql), EXCEL06_Staff_Recorded_vs_Invoiced_Hours (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table" CASCADE;
DROP MATERIALIZED VIEW IF EXISTS "4_Timesheet_Table_base_3" CASCADE;


CREATE MATERIALIZED VIEW "4_Timesheet_Table_base_3" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    b."Invoiced_Time",
    b."Is_Billable",
    b."Billable_Selector",
    b."Task_Category",
    b."Invoice_Number",
    b."Month_Time_Recorded",
    -- Month_Time_Invoiced: Lookup invoiced month from SUPPORT_Invoice_Task_Table
    (SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
        "Invoiced_Month"
     FROM SUPPORT_Invoice_Task_Table
     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
       AND "InvoiceID" = b."Invoice_Number"
     ORDER BY "Invoice_Task_ID", "InvoiceID"
     LIMIT 1) AS "Month_Time_Invoiced",
    -- Invoiced_Minutes: Lookup invoiced minutes from EXCEL06
    (SELECT DISTINCT ON ("Timesheet_UUID")
        "Invoiced_Mins"
     FROM EXCEL06_Staff_Recorded_vs_Invoiced_Hours
     WHERE "Timesheet_UUID" = b."Timesheet_UUID"::text
     ORDER BY "Timesheet_UUID"
     LIMIT 1) AS "Invoiced_Minutes",
    -- Month_Invoiced_On: Calculate months between recorded and invoiced dates
    CASE
        WHEN b."Invoice_Number" IS NULL THEN NULL
        ELSE
            CASE
                WHEN (EXTRACT(YEAR FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(YEAR FROM b."Month_Time_Recorded")) * 12 +
                (EXTRACT(MONTH FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(MONTH FROM b."Month_Time_Recorded")) = -1
                THEN 1
                ELSE (EXTRACT(YEAR FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(YEAR FROM b."Month_Time_Recorded")) * 12 +
                (EXTRACT(MONTH FROM (
                    SELECT DISTINCT ON ("Invoice_Task_ID", "InvoiceID")
                        "Invoiced_Month"
                     FROM SUPPORT_Invoice_Task_Table
                     WHERE "Invoice_Task_ID" = b."Invoice_Task_ID"
                       AND "InvoiceID" = b."Invoice_Number"
                     ORDER BY "Invoice_Task_ID", "InvoiceID"
                     LIMIT 1
                )) - EXTRACT(MONTH FROM b."Month_Time_Recorded")) + 1
            END
    END AS "Month_Invoiced_On"
FROM
    "4_Timesheet_Table_base_2" b;


CREATE INDEX ON "4_Timesheet_Table_base_3" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table_base_3" ("Date");


-- 4_Timesheet_Table
-- DAX equivalent: 4_Timesheet_Table (with recorded hours invoicing classification and job name)
-- Final timesheet view with invoicing timeline categorization for billable tasks and job name lookup.
-- Dependencies: 4_Timesheet_Table_base_3, key06_job_table (from 01_create_views.sql)
-- NOTE: DAX formula uses related(KEY02_Job_Task_Staff_ID[Job_Name]), but SQL implementation
--       looks up directly from key06_job_table to avoid circular dependency:
--       4_Timesheet_Table → KEYS_TIME → KEY02_Job_Task_Staff_ID_base → KEY02_Job_Task_Staff_ID
--       Since both sources retrieve Job_Name from the same key06_job_table lookup, the result is identical.
CREATE MATERIALIZED VIEW "4_Timesheet_Table" AS
SELECT
    b."Job_Task_Staff_ID",
    b."Timesheet_UUID",
    b."Job_ID",
    b."Task_ID",
    b."Staff_ID",
    b."Staff_Name",
    b."Timesheet_Notes",
    b."Billable",
    b."Invoice_ID",
    b."Invoice_Task_ID",
    b."Date",
    b."Recorded_Minutes",
    b."Task_Name",
    b."Client_Name",
    b."Task_Type",
    b."Task_Completed",
    b."Is_Client",
    b."Invoiced_Time",
    b."Is_Billable",
    b."Billable_Selector",
    b."Task_Category",
    b."Invoice_Number",
    b."Month_Time_Recorded",
    b."Month_Time_Invoiced",
    b."Invoiced_Minutes",
    b."Month_Invoiced_On",
    -- Recorded_Hours_invoiced: Categorizes when billable tasks were invoiced relative to recording
    CASE
        WHEN b."Task_Category" = 'Billable Tasks'
        THEN
            CASE
                WHEN b."Invoiced_Time" = 'Un-Invoiced' THEN '5_Un-Invoiced'
                WHEN b."Month_Invoiced_On" = 1 THEN '1_Same Month'
                WHEN b."Month_Invoiced_On" = 2 THEN '2_Following Month'
                WHEN b."Month_Invoiced_On" = 3 THEN '3_Third Month'
                ELSE '4_Fourth Month +'
            END
        ELSE NULL
    END AS "Recorded_Hours_invoiced",
    -- Job_Name: LOOKUPVALUE from key06_job_table
    (SELECT DISTINCT ON ("Job_ID")
        "Job_Name"
     FROM key06_job_table
     WHERE "Job_ID" = b."Job_ID"
     ORDER BY "Job_ID"
     LIMIT 1) AS "Job_Name"
FROM
    "4_Timesheet_Table_base_3" b;


CREATE INDEX ON "4_Timesheet_Table" ("Job_Task_Staff_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Job_ID");
CREATE INDEX ON "4_Timesheet_Table" ("Staff_Name");
CREATE INDEX ON "4_Timesheet_Table" ("Date");


-- KEYS_TIME
-- DAX equivalent: KEYS_TIME
-- Job-task-staff combinations from timesheet records with task names.
-- Dependencies: 4_Timesheet_Table
DROP MATERIALIZED VIEW IF EXISTS KEYS_TIME CASCADE;


CREATE MATERIALIZED VIEW KEYS_TIME AS
SELECT
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_ID",
    "Task_Name",
    "Staff_ID" AS "Staff_UUID",
    "Staff_Name"
FROM
    "4_Timesheet_Table";


CREATE INDEX ON KEYS_TIME ("Job_Task_Staff_ID");
CREATE INDEX ON KEYS_TIME ("Job_ID");
CREATE INDEX ON KEYS_TIME ("Staff_Name");


-- KEY02_Job_Task_Staff_ID_base
-- DAX equivalent: KEY02_Job_Task_Staff_ID_base
-- Base view with unique job-task-staff combinations from job task details and timesheet records.
-- Combines 1_Job_Task_Details_Table and KEYS_TIME, then deduplicates by Job_Task_Staff_ID.
-- Dependencies: 1_Job_Task_Details_Table, KEYS_TIME
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID CASCADE;
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID_base CASCADE;


CREATE MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base AS
SELECT DISTINCT ON ("Job_Task_Staff_ID")
    "Job_Task_Staff_ID",
    "Job_ID",
    "Task_Type",
    "Staff_Name",
    "StartDateAdjusted",
    "DueDateAdjusted",
    "Task_Name"
FROM (
    -- From 1_Job_Task_Details_Table (excluding timesheet-only records)
    SELECT
        "Job_Task_Staff_ID",
        "Job_ID",
        "Task_Type",
        "Staff_Name",
        "StartDateAdjusted",
        "DueDateAdjusted",
        CAST(NULL AS VARCHAR) AS "Task_Name"
    FROM "1_Job_Task_Details_Table"

    UNION ALL

    -- From KEYS_TIME (timesheet records without job task details)
    SELECT
        "Job_Task_Staff_ID",
        "Job_ID",
        CAST(NULL AS VARCHAR) AS "Task_Type",
        "Staff_Name",
        CAST(NULL AS DATE) AS "StartDateAdjusted",
        CAST(NULL AS DATE) AS "DueDateAdjusted",
        "Task_Name"
    FROM KEYS_TIME
) combined
ORDER BY "Job_Task_Staff_ID";


CREATE INDEX ON KEY02_Job_Task_Staff_ID_base ("Job_Task_Staff_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base ("Job_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base ("Staff_Name");


-- KEY02_Job_Task_Staff_ID_base_1
-- DAX equivalent: KEY02_Job_Task_Staff_ID_base_1
-- Extended view with task name, client name, and job name lookups from base KEY02.
-- Dependencies: KEY02_Job_Task_Staff_ID_base, 1_Job_Task_Details_Table, key06_job_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID CASCADE;
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID_base_1 CASCADE;


CREATE MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base_1 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_Type",
    b."Staff_Name",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    -- Task_Name: LOOKUPVALUE from 1_Job_Task_Details_Table
    COALESCE(
        b."Task_Name",
        (SELECT DISTINCT ON ("Job_Task_Staff_ID")
            "Task_Name"
         FROM "1_Job_Task_Details_Table"
         WHERE "Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         ORDER BY "Job_Task_Staff_ID"
         LIMIT 1)
    ) AS "Task_Name",
    -- Client_Name: LOOKUPVALUE from key06_job_table
    (SELECT DISTINCT ON ("Job_ID")
        "Client_Name"
     FROM key06_job_table
     WHERE "Job_ID" = b."Job_ID"
     ORDER BY "Job_ID"
     LIMIT 1) AS "Client_Name",
    -- Job_Name: LOOKUPVALUE from key06_job_table
    (SELECT DISTINCT ON ("Job_ID")
        "Job_Name"
     FROM key06_job_table
     WHERE "Job_ID" = b."Job_ID"
     ORDER BY "Job_ID"
     LIMIT 1) AS "Job_Name"
FROM
    KEY02_Job_Task_Staff_ID_base b;


CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_1 ("Job_Task_Staff_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_1 ("Job_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_1 ("Staff_Name");


-- KEY02_Job_Task_Staff_ID_base_2
-- DAX equivalent: KEY02_Job_Task_Staff_ID_base_2
-- View with task category and task type enrichment from lookups.
-- Dependencies: KEY02_Job_Task_Staff_ID_base_1, key05_task_type (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID CASCADE;
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID_base_2 CASCADE;


CREATE MATERIALIZED VIEW KEY02_Job_Task_Staff_ID_base_2 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_Type",
    b."Staff_Name",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Task_Name",
    b."Client_Name",
    b."Job_Name",
    -- Task_Category: Categorizes based on task name content and client type
    CASE
        WHEN (
            b."Task_Name" ILIKE '%Holiday%'
            OR b."Task_Name" ILIKE '%Other leave%'
        ) THEN 'Leave Tasks'
        WHEN b."Task_Name" ILIKE '%Sick leave%' THEN 'Leave Tasks'
        WHEN (
            b."Task_Name" ILIKE '%Admin - Non-billable%'
            OR b."Client_Name" = 'Dinniss Admin'
        ) THEN 'Admin Tasks'
        ELSE 'Billable Tasks'
    END AS "Task_Category",
    -- Task_Type1: CONCATENATEX over key05_task_type, collecting type names found (via ILIKE search) in the task name
    COALESCE(
        NULLIF(
            TRIM(
                (
                    SELECT STRING_AGG(kt."Name", ' ')
                    FROM key05_task_type kt
                    WHERE b."Task_Name" ILIKE ('%' || kt."Name" || '%')
                )
            ),
            ''
        ),
        b."Task_Name"
    ) AS "Task_Type1"
FROM
    KEY02_Job_Task_Staff_ID_base_1 b;


CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_2 ("Job_Task_Staff_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_2 ("Job_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID_base_2 ("Staff_Name");


-- KEY02_Job_Task_Staff_ID
-- DAX equivalent: KEY02_Job_Task_Staff_ID
-- Final view with online content task name preference over task type classification.
-- Dependencies: KEY02_Job_Task_Staff_ID_base_2
DROP MATERIALIZED VIEW IF EXISTS KEY02_Job_Task_Staff_ID CASCADE;


CREATE MATERIALIZED VIEW KEY02_Job_Task_Staff_ID AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Staff_Name",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Task_Name",
    b."Client_Name",
    b."Job_Name",
    b."Task_Category",
    b."Task_Type1",
    -- Task_Type: If task name is "Online content creation" or "Online content management", use task name, else use Task_Type1
    CASE
        WHEN b."Task_Name" ILIKE '%Online content creation%' THEN b."Task_Name"
        WHEN b."Task_Name" ILIKE '%Online content management%' THEN b."Task_Name"
        ELSE b."Task_Type1"
    END AS "Task_Type"
FROM
    KEY02_Job_Task_Staff_ID_base_2 b;


CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Job_Task_Staff_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Job_ID");
CREATE INDEX ON KEY02_Job_Task_Staff_ID ("Staff_Name");


-- SUPPORT_Staff_Leave_Allocation_byDay_base
-- DAX equivalent: SUPPORT_Staff_Leave_Allocation_byDay = FILTER(CROSSJOIN(KEY02_Job_Task_Staff_ID, KEY01_CalendarDate), KEY02_Job_Task_Staff_ID[Task_Category]="Leave Tasks")
-- Support view combining every calendar date with every leave task assignment.
-- Filtered to include only rows where Task_Category = "Leave Tasks" for leave allocation tracking.
-- Dependencies: KEY02_Job_Task_Staff_ID, key01_calendar_date (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay_base CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base AS
SELECT
    k."Job_Task_Staff_ID",
    k."Job_ID",
    k."Staff_Name",
    k."StartDateAdjusted",
    k."DueDateAdjusted",
    k."Task_Name",
    k."Client_Name",
    k."Job_Name",
    k."Task_Category",
    k."Task_Type1",
    k."Task_Type",
    c."Date",
    c."PublicHoliday",
    c."Weekday",
    c."WeekEnd",
    c."StartOfMonth",
    c."EndOfMonth",
    c."Is_Range_for_Invoicing"
FROM
    KEY02_Job_Task_Staff_ID k
    CROSS JOIN key01_calendar_date c
WHERE
    k."Task_Category" = 'Leave Tasks';


CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base ("Date");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base ("Staff_Name");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base ("Job_Task_Staff_ID");


-- SUPPORT_Staff_Leave_Allocation_byDay_base_1
-- DAX equivalent: Extended leave allocation view with workability and staff schedule validation
-- Extends SUPPORT_Staff_Leave_Allocation_byDay_base with workability, adjusted dates, and staff workable day-of-week lookup.
-- Dependencies: SUPPORT_Staff_Leave_Allocation_byDay_base, 1_Job_Task_Details_Table, EXCEL01_Staff_Workable_Days (from 021_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay_base_1 CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base_1 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Staff_Name",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Task_Name",
    b."Client_Name",
    b."Job_Name",
    b."Task_Category",
    b."Task_Type1",
    b."Task_Type",
    b."Date",
    b."PublicHoliday",
    b."Weekday",
    b."WeekEnd",
    b."StartOfMonth",
    b."EndOfMonth",
    b."Is_Range_for_Invoicing",
    -- Is_WorkableDay: IF(AND(WeekEnd=FALSE, PublicHoliday=FALSE), TRUE, FALSE)
    CASE
        WHEN b."WeekEnd" = FALSE AND b."PublicHoliday" = FALSE THEN TRUE
        ELSE FALSE
    END AS "Is_WorkableDay",
    -- AdjustedStartDate: LOOKUPVALUE(1_Job_Task_Details_Table[StartDateAdjusted], 1_Job_Task_Details_Table[Job_Task_Staff_ID], Job_Task_Staff_ID)
    COALESCE(
        (SELECT DISTINCT ON (jt."Job_Task_Staff_ID")
            jt."StartDateAdjusted"
         FROM "1_Job_Task_Details_Table" jt
         WHERE jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         LIMIT 1),
        b."StartDateAdjusted"
    ) AS "AdjustedStartDate",
    -- AdjustedDueDate: LOOKUPVALUE(1_Job_Task_Details_Table[DueDateAdjusted], 1_Job_Task_Details_Table[Job_Task_Staff_ID], Job_Task_Staff_ID)
    COALESCE(
        (SELECT DISTINCT ON (jt."Job_Task_Staff_ID")
            jt."DueDateAdjusted"
         FROM "1_Job_Task_Details_Table" jt
         WHERE jt."Job_Task_Staff_ID" = b."Job_Task_Staff_ID"
         LIMIT 1),
        b."DueDateAdjusted"
    ) AS "AdjustedDueDate",
    -- Is_Staff_Workable_DayOfWeek: LOOKUPVALUE(EXCEL01_Staff_Workable_Days[Working Day], EXCEL01_Staff_Workable_Days[Day of Week], Weekday, EXCEL01_Staff_Workable_Days[StaffName], Staff_Name)
    COALESCE(
        (SELECT DISTINCT ON (e."Day of Week", e."StaffName")
            e."Working Day"
         FROM EXCEL01_Staff_Workable_Days e
         WHERE e."Day of Week" = b."Weekday" AND e."StaffName" = b."Staff_Name"
         LIMIT 1),
        FALSE
    ) AS "Is_Staff_Workable_DayOfWeek"
FROM
    SUPPORT_Staff_Leave_Allocation_byDay_base b;


CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_1 ("Date");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_1 ("Staff_Name");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_1 ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_1 ("Is_WorkableDay");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_1 ("Is_Staff_Workable_DayOfWeek");


-- SUPPORT_Staff_Leave_Allocation_byDay_base_2
-- DAX equivalent: Leave allocation view with date range validation
-- Extends SUPPORT_Staff_Leave_Allocation_byDay_base_1 with date range filtering.
-- Dependencies: SUPPORT_Staff_Leave_Allocation_byDay_base_1
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Staff_Leave_Allocation_byDay_base_2 CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Staff_Leave_Allocation_byDay_base_2 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Staff_Name",
    b."StartDateAdjusted",
    b."DueDateAdjusted",
    b."Task_Name",
    b."Client_Name",
    b."Job_Name",
    b."Task_Category",
    b."Task_Type1",
    b."Task_Type",
    b."Date",
    b."PublicHoliday",
    b."Weekday",
    b."WeekEnd",
    b."StartOfMonth",
    b."EndOfMonth",
    b."Is_Range_for_Invoicing",
    b."Is_WorkableDay",
    b."AdjustedStartDate",
    b."AdjustedDueDate",
    b."Is_Staff_Workable_DayOfWeek",
    -- Is_DateBetweenTask: IF(AND(Date >= AdjustedStartDate, Date <= AdjustedDueDate), TRUE, FALSE)
    CASE
        WHEN b."Date" >= b."AdjustedStartDate" AND b."Date" <= b."AdjustedDueDate" THEN TRUE
        ELSE FALSE
    END AS "Is_DateBetweenTask"
FROM
    SUPPORT_Staff_Leave_Allocation_byDay_base_1 b;


CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_2 ("Date");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_2 ("Staff_Name");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_2 ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Staff_Leave_Allocation_byDay_base_2 ("Is_DateBetweenTask");


-- SUPPORT_Job_Leave_Task_Details_Table_base
-- Power Query equivalent: Transforms jobtask and jobtaskassignee records with client details, filtered to leave tasks only
-- Combines job task details with staff assignments, filtered to leave task types (Holiday, Sick leave, Other leave).
-- Only includes records with allocated minutes > 0 and specific staff members.
-- Dependencies: base tables (jobtask, jobtaskassignee, jobdetails, TOCHECK_ClientDetails from 015_create_materialized_views.sql)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table_base CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base AS
SELECT
    (jt."JobDetailsRemoteID"::TEXT || jt."UUID"::TEXT || jta."UUID"::TEXT) AS "Job_Task_Staff_ID",
    jt."JobDetailsRemoteID"::TEXT AS "Job_ID",
    jt."Name" AS "Task_Name",
    jt."UUID"::TEXT AS "Task_UUID",
    jta."Name" AS "Staff_Name",
    jta."UUID"::TEXT AS "Staff_UUID",
    cd."Name" AS "Client_Name",
    jt."StartDate",
    jt."DueDate",
    jta."AllocatedMinutes" AS "Task_Allocated_Mins",
    jta."AllocatedMinutes" AS "Initial_Avg_Mins_perWorkDay"
FROM
    jobtask jt
    LEFT JOIN jobtaskassignee jta ON jta."JobTaskID" = jt."UUID"
    LEFT JOIN jobdetails jd ON jd."ID" = jt."JobDetailsRemoteID"
    LEFT JOIN TOCHECK_ClientDetails cd ON cd."UUID" = jd."ClientUUID"
WHERE
    jta."Name" IN ('Carl Patton', 'Dan Sharpe', 'Dani Millar', 'Dave Simmons', 'Greta Sutcliffe', 'Marty Dinniss', 'Meagan Robertson', 'Ruby Rowe')
    AND (jt."Name" ILIKE '%Holiday%' OR jt."Name" ILIKE '%Sick leave%' OR jt."Name" ILIKE '%Other leave%')
    AND jta."AllocatedMinutes" <> 0
    AND (jt."IsDeleted"::TEXT NOT IN ('true', 'True', '1') OR jt."IsDeleted" IS NULL)
    AND (jta."IsDeleted"::TEXT NOT IN ('true', 'True', '1') OR jta."IsDeleted" IS NULL);


CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base ("Staff_Name");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base ("Job_ID");


-- SUPPORT_Job_Leave_Task_Details_Table_base_1
-- DAX equivalent: Leave task details with adjusted start and due dates
-- Extends SUPPORT_Job_Leave_Task_Details_Table_base with adjusted date lookups from job details.
-- Dependencies: SUPPORT_Job_Leave_Task_Details_Table_base, key06_job_table (from 01_create_views.sql)
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table CASCADE;
DROP MATERIALIZED VIEW IF EXISTS SUPPORT_Job_Leave_Task_Details_Table_base_1 CASCADE;


CREATE MATERIALIZED VIEW SUPPORT_Job_Leave_Task_Details_Table_base_1 AS
SELECT
    b."Job_Task_Staff_ID",
    b."Job_ID",
    b."Task_Name",
    b."Task_UUID",
    b."Staff_Name",
    b."Staff_UUID",
    b."Client_Name",
    b."StartDate",
    b."DueDate",
    b."Task_Allocated_Mins",
    b."Initial_Avg_Mins_perWorkDay",
    -- StartDateAdjusted: IF(ISBLANK(StartDate), LOOKUPVALUE(KEY06_Job_Table[StartDate], KEY06_Job_Table[Job_ID], Job_ID), StartDate)
    COALESCE(
        b."StartDate",
        (SELECT DISTINCT ON (k6."Job_ID")
            k6."StartDate"
         FROM key06_job_table k6
         WHERE k6."Job_ID" = b."Job_ID"
         LIMIT 1)
    ) AS "StartDateAdjusted",
    -- DueDateAdjusted: IF(ISBLANK(DueDate), LOOKUPVALUE(KEY06_Job_Table[EarlierDate], KEY06_Job_Table[Job_ID], Job_ID), DueDate)
    COALESCE(
        b."DueDate",
        (SELECT DISTINCT ON (k6."Job_ID")
            k6."EarlierDate"
         FROM key06_job_table k6
         WHERE k6."Job_ID" = b."Job_ID"
         LIMIT 1)
    ) AS "DueDateAdjusted"
FROM
    SUPPORT_Job_Leave_Task_Details_Table_base b;


CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_1 ("Job_Task_Staff_ID");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_1 ("Staff_Name");
CREATE INDEX ON SUPPORT_Job_Leave_Task_Details_Table_base_1 ("Job_ID");


-- Note: SUPPORT_Staff_Leave_Allocation_byDay is created in 026_create_materialized_views.sql
