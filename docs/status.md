# Implementation Status

This document tracks the current status and upcoming work. Completed user stories are archived in `docs/complete.md`.

---

## Current Status Summary

**All planned Phase 1 user stories are complete.**

- ✅ 231 tests passing
- ✅ 84% overall code coverage
- ✅ Comprehensive E2E test coverage
- ✅ Full CLI/API/UI feature parity

---

## In Progress

No active implementation tasks at this time. All planned user stories have been completed.

---

## Backlog

### Future Enhancements

#### US 6.3: Performance optimization for 10M+ row datasets
**Priority:** Medium
**Estimated Effort:** 1-2 weeks

Optimize the data generation and loading pipeline to efficiently handle very large datasets (10+ million rows).

**Proposed Improvements:**
- Parallel Parquet batch generation using multiprocessing
- Streaming data loading to reduce memory footprint
- Batch size tuning based on available system resources
- Progress indicators for long-running operations
- Incremental loading with checkpoint/resume support

**Acceptance Criteria:**
- Generate and load 10M rows in under 10 minutes on standard hardware
- Memory usage remains under 2GB during generation
- Users receive real-time progress updates every 5 seconds
- Failed jobs can resume from last checkpoint

---

#### US 6.4: Data lineage tracking and visualization
**Priority:** Low
**Estimated Effort:** 2-3 weeks

Track and visualize the lineage of generated data, including FK relationships, generation runs, and data transformations.

**Proposed Features:**
- Lineage metadata stored in SQLite alongside experiment schemas
- Graph visualization of table relationships and FK chains
- Run history showing which data came from which generation run
- Export lineage as GraphViz DOT files

**Acceptance Criteria:**
- Users can view a visual graph of FK relationships in the Web UI
- Each data row can be traced back to its generation run
- Lineage metadata persists across experiment resets
- Export functionality generates valid DOT files

---

#### US 6.5: Export experiments as Docker images for reproducibility
**Priority:** Low
**Estimated Effort:** 1 week

Package experiments (schema + generated data + warehouse state) into portable Docker images for sharing and reproducibility.

**Proposed Features:**
- Export experiment as self-contained Docker image
- Image includes schema, Parquet files, and SQLite/PostgreSQL dump
- Import experiment from Docker image on another machine
- Version tagging for experiment snapshots

**Acceptance Criteria:**
- Users can export an experiment to a Docker image via CLI/API
- Exported images can be imported on a different machine
- Imported experiments maintain all data and relationships
- Image size is optimized (compressed Parquet, deduplication)

---

## Documentation Maintenance

Completed user stories are moved to `docs/complete.md` to keep this file focused on current and upcoming work. See `docs/ai.md` for the workflow automation rule.
