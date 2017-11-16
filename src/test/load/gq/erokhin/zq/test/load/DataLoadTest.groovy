package gq.erokhin.zq.test.load

import gq.erokhin.zq.test.ZQSpecification
import groovy.sql.Sql
import groovyx.gpars.GParsPool

import java.sql.SQLException
import java.util.concurrent.CountDownLatch

import static gq.erokhin.zq.test.ApiWrappers.*
import static gq.erokhin.zq.test.Helpers.*

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class DataLoadTest extends ZQSpecification {

    def static TEST_EVENTS_COUNT = 10000
    def static PARALLEL_WORKERS_COUNT = 42

    def static TEST_EVENTS = generateTestEvents(TEST_EVENTS_COUNT)

    def "Queue should work correct under parallel enqueuing"() {
        given: "A queue"
        createQueue(dataSource, TEST_QUEUE_NAME)

        and: "Chunks for processing"
        def chunks = TEST_EVENTS.collate(TEST_EVENTS_COUNT / PARALLEL_WORKERS_COUNT as int)

        when: "Many treads enqueue data concurrently"
        GParsPool.withPool(PARALLEL_WORKERS_COUNT) {
            chunks.eachParallel { List<String> chunk ->
                def subChunkSize = RANDOM.nextInt(chunk.size()) + 1
                chunk.collate(subChunkSize).each { subChunk ->
                    enqueue(dataSource, TEST_QUEUE_NAME, subChunk)
                }
            }
        }

        then: "Events are the same that were enqueued"
        openBatch(dataSource, TEST_QUEUE_NAME, TEST_EVENTS_COUNT) == TEST_EVENTS_COUNT
        def events = dequeue(dataSource, TEST_QUEUE_NAME)
        events.sort() == TEST_EVENTS.sort()
    }

    def "Parallel dequeuing should not break order or quantity of events"() {
        given: "A queue with events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        enqueue(dataSource, TEST_QUEUE_NAME, TEST_EVENTS)

        and: "List for collecting events"
        List<String> result = []

        when: "Many treads dequeuing data concurrently by pieces of 1..100 events into this list"
        GParsPool.withPool(PARALLEL_WORKERS_COUNT) {
            (1..PARALLEL_WORKERS_COUNT).eachParallel {
                def queueIsExhausted = false
                while (!queueIsExhausted) {
                    int maxBatchSize = RANDOM.nextInt(100) + 1
                    boolean batchIsOpen = false

                    try {
                        def batchSize = openBatch(dataSource, TEST_QUEUE_NAME, maxBatchSize)
                        batchIsOpen = batchSize > 0
                        queueIsExhausted = batchSize == 0
                    } catch (SQLException ignored) {
                    }

                    if (batchIsOpen) {
                        result += dequeue(dataSource, TEST_QUEUE_NAME)
                        closeBatch(dataSource, TEST_QUEUE_NAME)
                    }

                    Thread.sleep(RANDOM.nextInt(10)) //To add some fun in this race
                }
            }
        }

        then: "List of dequeued events should match list of enqueued"
        result == TEST_EVENTS

        and: "Data table is empty"
        new Sql(dataSource).firstRow("SELECT count(*) AS rowCount FROM zq.queue_1").rowCount == 0
    }


    def "Dequeuing with cursor from queue dropped in the middle of process"() {
        given: "A queue with events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        enqueue(dataSource, TEST_QUEUE_NAME, TEST_EVENTS)

        and: "List for collecting events"
        List<String> result = []

        and: "Fetch size to configure statement for cursor usage"
        int fetchSize = TEST_EVENTS_COUNT / 10 as Integer

        and: "Amount of consumed events after which queue will be dropped"
        int deleteThreshold = TEST_EVENTS_COUNT / 2 as Integer

        and: "Process that will drop a queue"
        CountDownLatch latch = new CountDownLatch(1)
        Thread.start {
            latch.await()
            dropQueue(dataSource, TEST_QUEUE_NAME)
        }

        when: "Dequeuing events with limited fetch size while dropping a queue"
        openBatch(dataSource, TEST_QUEUE_NAME, TEST_EVENTS_COUNT)
        new Sql(dataSource).eachRowLazy("SELECT \"data\" FROM zq.dequeue('$TEST_QUEUE_NAME')", fetchSize) {
            result += it.data
            if (result.size() == deleteThreshold) {
                latch.countDown()
                Thread.sleep(50) //time to proceed with queue deletion
            }
        }

        then: "List of dequeued events should match list of enqueued"
        result == TEST_EVENTS

        and: "Queue is deleted (deletion returns false)"
        !dropQueue(dataSource, TEST_QUEUE_NAME)

        and: "Data table is not exists"
        !isTableExists(dataSource, "queue_1")
    }

    private static List<String> generateTestEvents(int count) {
        def result = (1..count).collect { "event $it" }.toList()
        Collections.shuffle(result)
        result
    }

}
