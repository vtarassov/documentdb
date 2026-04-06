\i ../regress/sql/documentdb_test_helpers.sql

-- Turn off cron jobs to avoid flakiness in tests.
UPDATE cron.job set active = false;
