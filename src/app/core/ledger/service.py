"""Run ledger service.

Provides idempotent run tracking and duplicate prevention.
"""
# - Record workflow runs
# - Check for existing runs (idempotency)
# - Support retry logic
# - Query run history

