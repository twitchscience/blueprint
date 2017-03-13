
## Schema Suggestor

The Schema Suggestor parses log files of untracked events from the processor and recommends schemas
for them based on inferred types and lengths of fields in events.

### Update Mechanism

The processors each flush their untracked events every 10 minutes to s3 and write an event to SQS.
The schema suggestor polls for SQS messages every 2 minutes and adds them to an internal buffer.
Every 5 minutes, that internal buffer is flushed, sending suggestions to blueprint and replacing
previous suggestions.

This behavior has a few implications:
- Every event that comes in will eventually show up in the schema suggestor
for exactly five minutes, starting between 0 and ~12 minutes after reaching the processor.
- Any given five-minute window may be empty if the processors sync up when they flush.
