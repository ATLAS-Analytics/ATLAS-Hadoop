Reshuffle.pig
-- this code reads from /atlas/analytics/panda/jobs_status, reshuffles records so that each pandaid has only one row, stores result for further processing.

Intervals.pig
-- this code uses reshuffled data and extracts all the intervals
