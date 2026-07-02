# Parallel SQL parsing — progress ledger
Task 1: complete (cpu_detection.py, commits e8fc959..a9c4247, review clean, 18/18 tests)
Task 2: complete (read-only ConnectionWrapper/FileBackedDict + SchemaResolver.snapshot_to/load_readonly, commits 48c566b..b20a13b, review clean incl H1 fix, 58/58 tests)
Task 3: complete (ParallelSqlParser spawn pool + BackpressureAwareExecutor generalization, commit fcb1543c, review Spec✅/Approved, 45 tests)
  MINOR for final review:
  - M1: SqlParsingResult exception tracebacks dropped across pickle (parallel debug_info has no traceback vs serial); lineage unaffected. Consider a comment in _worker_parse.
  - M2: DatahubClientConfig.token is plain str; ensure no future edit logs graph_config/initargs (no leak today).
