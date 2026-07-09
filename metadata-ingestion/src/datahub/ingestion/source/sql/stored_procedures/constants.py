# Container name / trailing DataFlow segment for stored procedures. Lives here
# (not base.py) because base.py imports lineage.py, so a shared constant in
# base.py would be circular.
STORED_PROCEDURES_CONTAINER = "stored_procedures"
