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

### US 6.3: Performance optimization for 10M+ row datasets

**Priority:** Medium
**Started:** 2025-11-16
**Goal:** Optimize the data generation and loading pipeline to efficiently handle very large datasets (10+ million rows).

**Implementation Plan:**

**Step 1/7: Multiprocessing infrastructure (✅ COMPLETE)**
- ✅ Added `multiprocessing` support to `ExperimentGenerator` for parallel batch generation
- ✅ Implemented process pool for concurrent Parquet file writes using `multiprocessing.Pool`
- ✅ Added `max_workers` configuration parameter (defaulting to `cpu_count() - 1`)
- ✅ Ensured deterministic seeding across worker processes via batch-specific seeds
- ✅ Created module-level worker functions (`_generate_batch_worker`, `_generate_value_worker`)
- ✅ Pre-calculated unique column offsets for each batch to prevent conflicts
- ✅ Added 6 comprehensive unit tests:
  - `test_multiprocessing_parallel_generation_produces_correct_results`
  - `test_multiprocessing_deterministic_seeding`
  - `test_multiprocessing_single_worker_vs_multi_worker_equivalence`
  - `test_multiprocessing_unique_values_across_batches`
  - `test_multiprocessing_with_foreign_keys`
  - `test_multiprocessing_max_workers_configuration`
- Test Results: All 28 generator tests passing (22 existing + 6 new multiprocessing tests)

**Step 2/7: Streaming data loading (⏳ IN PROGRESS)**
- Refactor `load_parquet_files_to_table()` to use streaming/chunked reads
- Replace full-file loading with batched INSERT statements
- Implement memory-efficient Parquet reader using PyArrow's streaming API
- Add configurable chunk size (default: 10k rows per batch)
- Target: Memory usage under 2GB regardless of dataset size
- Tests: Memory profiling tests with 10M+ rows

**Step 3/7: Adaptive batch size tuning (PENDING)**
- Add `psutil` dependency for system resource monitoring
- Implement dynamic batch size calculation based on available memory
- Add safety limits (min: 1k rows, max: 100k rows per batch)
- Expose tuning parameters via environment variables and CLI flags
- Tests: Batch size calculation under various memory constraints

**Step 4/7: Real-time progress indicators (PENDING)**
- Extend `generation_runs` table with `progress_pct` and `last_updated` columns
- Implement progress callback mechanism in generator
- Update progress every 5 seconds (or configurable interval)
- Surface progress via API (`GET /api/experiments/{name}/runs/{run_id}`)
- Enhance Web UI to poll and display live progress bars
- Tests: Progress update frequency and accuracy tests

**Step 5/7: Checkpoint/resume functionality (PENDING)**
- Add `generation_checkpoints` table tracking completed batches per table
- Implement checkpoint save after each successful batch write
- Add resume logic to skip already-generated batches
- Expose resume capability via CLI flag (`--resume`) and API parameter
- Clean up checkpoints on successful completion
- Tests: Resume scenarios (partial failure, mid-generation abort)

**Step 6/7: Performance testing and benchmarking (PENDING)**
- Create dedicated performance test suite (`tests/test_performance.py`)
- Add 10M row benchmark test with timing assertions (< 10 minutes)
- Add memory profiling integration (using `memory_profiler` or `tracemalloc`)
- Validate all acceptance criteria with real workloads
- Document baseline performance metrics
- Tests: Automated performance regression tests

**Step 7/7: Documentation and user guidance (PENDING)**
- Update README.md with performance optimization section
- Add troubleshooting guide for large datasets
- Document tuning parameters and recommended settings
- Add example schemas and commands for 10M+ row scenarios
- Update tech-spec.md with architecture changes

**Acceptance Criteria:**
- [ ] AC1: Generate and load 10M rows in under 10 minutes on standard hardware
- [ ] AC2: Memory usage remains under 2GB during generation
- [ ] AC3: Users receive real-time progress updates every 5 seconds
- [ ] AC4: Failed jobs can resume from last checkpoint

---

## Backlog

### Future Enhancements

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
