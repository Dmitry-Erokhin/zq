# ZQ 
ZQ – is a multi-producer-single-consumer durable queue based on the PostgreSQL.
All operations are performed by calling stored procedures, so it makes it a 
reasonable choice if you already have a PostgreSQL in your zoo and you need a 
simple queue solution.

Inspired by pgq.

## Way of work
In nutshell work with queue (after it was created) is as simple as:
- Enqueue data
- Consume data
- Confirm consumed data
    - data becomes inaccessible anymore (and eventually will be removed from the storage)
    - next consumption will start from the item followed by last consumed and confirmed

## PostgresSQL API
### Queues management
- Create queue: `zq.create_queue(queue_name TEXT) : BOOLEAN`
  - Tries to create a queue "queue_name"
  - Returns `true` if queue was successfully created
  - Returns `false` if queue is already exists

- Delete queue: `zq.drop_queue(queue_name TEXT) : BOOLEAN`
  - Tries to delete queue "queue_name"
  - Returns `true` if queue was successfully deleted
  - Returns `false` if queue is not exists

### Data management
- Enqueue data: `zq.enqueue(queue_name TEXT, data TEXT[]) : VOID`
  - Inserts items from data array as events in the queue 'queue_name' preserving order
  - Throws an exception if given queue is not exists

- Consume data: `zq.dequeue(queue_name TEXT, max_events INT) : ROWSET`
  - Returns at most `max_events` oldest events from the queue "queue_name" as a list
  of rows with following columns:
    - `"timestamp" TIMESTAMP`
    - `"data" TEXT`
  - Throws an exception if given queue is not exists
  
- Confirm events: `zq.confirm(queue_name TEXT) : INT`
  - Confirms successfully processing of previously consumed events
  - Throws an exception if given queue is not exists
  - Throws an exception if queue has no requests for the events consumption
