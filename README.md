# ZQ 
ZQ – is a multi-producer-single-consumer durable queue based on the PostgreSQL.
All operations are performed by calling stored procedures, so it makes it a 
reasonable choice if you already have a PostgreSQL in your zoo and need a 
simple queue solution.

Inspired by pgq.

## Way of work
In nutshell work with queue (after it was created) is as simple as:
- Enqueue data
- Open batch of desired size
- Consume data
- Close or cancel batch
  - If batch was closed (for instance in case of successful data processing):
    - data will be removed from queue
    - next batch will be started from item followed by last item of closed batch
  - If batch was canceled (for instance in case of temporary impediments of data processing):
    - data stays in the queue
    - next batch will be started from same item 

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

- Consume data: `zq.dequeue(queue_name TEXT) : ROWSET`
  - Returns content of the current batch for the queue "queue_name" as the list
  of rows with following columns:
    - `"timestamp" TIMESTAMP`
    - `"data" TEXT`
  - Throws an exception if given queue is not exists
  - Throws an exception if there is batch was not created for given queue

### Batch management
  
- Create new batch: `zq.open_batch(queue_name TEXT, max_batch_size INT) : INT`
  - Creates new batch of maximum size "max_batch_size" for the queue  "queue_name"
  - Returns actual size of the newly created batch (<="max_batch_size")
  - Throws an exception if given queue is not exists
  - Throws an exception if non-committed batch for given queue is already exists 

  
- Commit current batch: `zq.close_batch(queue_name TEXT)`
  - Commits previously created batch for the queue "queue_name"
  - Throws an exception if given queue is not exists
  - Throws an exception if there is no batch was created for given queue
  
- Cancel current batch: `zq.cancel_batch(queue_name TEXT)`
  - Cancel previously created batch for the queue "queue_name"
  - Throws an exception if given queue is not exists
  - Throws an exception if there is no batch was created for given queue