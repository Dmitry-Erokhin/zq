#ZQ API

##Queues management
- Create queue: `zq.create_queue(queue_name TEXT)`
  - Creates queue "queue_name"
  - Throws an exception if queue with this name is already exists

- Delete queue: `zq.drop_queue(queue_name TEXT)`
  - Deletes queue "queue_name" with all existing data and consumers
  - Throws an exception if given queue is not exists

##Consumers management
- Create consumer: `zq.create_consumer(queue_name TEXT, consumer_name TEXT)`
  - Creates consumer "consumer_name" for queue "queue_name"
  - Throws an exception if given queue is not exists
  - Throws an exception if consumer with this name is already exists for given queue

- Delete consumer: `zq.drop_consumer(queue_name TEXT, consumer_name TEXT)`
  - Deletes consumer "consumer_name" of queue "queue_name"
  - Deletes data required only by deleted consumer (all data of the queue if it was last consumer)
  - Throws an exception if given queue is not exists
  - Throws an exception if consumer with this name is not exists for given queue

##Data management
- Enqueue data: `zq.enqueue_data(queue_name TEXT, data TEXT[])`
  - Inserts items from data array as events in the queue 'queue_name'
  - Throws an exception if given queue is not exists
  
- Create new batch: `zq.create_batch(queue_name TEXT, consumer_name TEXT, max_batch_size INT) : INT`
  - Creates new batch of maximum size "max_batch_size" for queue and consumer "queue_name" and "consumer_name" 
  respectively
  - Returns actual size of the newly created batch (<="max_batch_size")
  - Throws an exception if given queue is not exists
  - Throws an exception if consumer with this name is not exists for given queue
  - Throws an exception if batch for given consumer and queue was already created 

- Consume events: `zq.dequeue(queue_name TEXT, consumer_name TEXT) : ROWSET`
  - Returns content of the current batch for queue and consumer "queue_name" and "consumer_name" respectively 
  as the list of rows with following columns:
    - `"timestamp" TIMESTAMP`
    - `"data" TEXT`
  - Throws an exception if given queue is not exists
  - Throws an exception if consumer with this name is not exists for given queue
  - Throws an exception if there is no batch was created for given queue and consumer
  
- Commit current batch: `zq.commit_batch(queue_name TEXT, consumer_name TEXT)`
  - Commits previously created batch for queue and consumer "queue_name" and "consumer_name" respectively
  - If there is data committed by all consumers of given queue - deletes it
  - Throws an exception if given queue is not exists
  - Throws an exception if consumer with this name is not exists for given queue
  - Throws an exception if there is no batch was created for given queue and consumer