package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.helpers.ZQSpecification
import spock.lang.Unroll

import java.sql.SQLException

import static gq.erokhin.zq.test.helpers.ApiWrappers.*
import static gq.erokhin.zq.test.helpers.TestHelpers.TEST_QUEUE_NAME

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class BatchesTest extends ZQSpecification {

    @Unroll
    def "Open batch of #maxBatchSize on queue with #numberOfEvents events returns #result "() {
        given: "A queue with events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        numberOfEvents.times { enqueue(dataSource, TEST_QUEUE_NAME, "event ${it + 1}") }

        expect: "Open batch returns proper result"
        result == openBatch(dataSource, TEST_QUEUE_NAME, maxBatchSize)

        where:
        numberOfEvents | maxBatchSize || result
        0              | 0            || 0
        0              | 10           || 0
        42             | 12           || 12
        42             | 42           || 42
        42             | 64           || 42
    }

    def "Open batch for non existing queue throws an exception"() {
        when: "Open a new batch for non existing queue"
        openBatch(dataSource, TEST_QUEUE_NAME, 42)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" is not exists.")
    }

    def "Open batch twice throws an exception"() {
        given: "A queue with open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, 42)

        when: "Open a new batch"
        openBatch(dataSource, TEST_QUEUE_NAME, 42)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" has already open batch.")
    }

    def "Open batch with negative max batch size param throws an exception"() {
        when: "Open a new batch with negative size"
        createQueue(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, -1)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Param \"max_batch_size\" should not be negative.")
    }


    def "Close opened batch successfully"() {
        given: "A queue with open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, 42)

        when: "Close the batch"
        closeBatch(dataSource, TEST_QUEUE_NAME)

        then: "No exception thrown"
    }

    def "Close batch for non existing queue throws an exception"() {
        when: "Close a batch for non existing queue"
        closeBatch(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" is not exists.")
    }

    def "Close batch for a queue without open batch throws an exception"() {
        given: "A queue without open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Close a batch"
        closeBatch(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" does not have open batch.")
    }

    def "New batch starts from unconsumed event"() {
        given: "A queue with 10 events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        10.times { enqueue(dataSource, TEST_QUEUE_NAME, "event ${it + 1}") }

        when: "Open and close a batch of 5, then open a new batch"
        openBatch(dataSource, TEST_QUEUE_NAME, 5)
        closeBatch(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, 42)
        
        then: "First consumed event is 6th enqueued"
        dequeue(dataSource, TEST_QUEUE_NAME)[0] == "event 6"
    }

    
    
    def "Cancel opened batch successfully"() {
        given: "A queue with open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, 42)

        when: "Cancel the batch"
        cancelBatch(dataSource, TEST_QUEUE_NAME)

        then: "No Exception thrown"
    }

    def "Cancel batch for non existing queue throws an exception"() {
        when: "Cancel a batch for non existing queue"
        cancelBatch(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" is not exists.")
    }

    def "Cancel batch for a queue without open batch throws an exception"() {
        given: "A queue without open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Cancel a batch"
        cancelBatch(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" does not have open batch.")
    }

    def "New batch after cancel starts from same event"() {
        given: "A queue with 10 events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        10.times { enqueue(dataSource, TEST_QUEUE_NAME, "event ${it + 1}") }

        when: "Open and cancel a batch of 5, then open new batch"
        openBatch(dataSource, TEST_QUEUE_NAME, 5)
        cancelBatch(dataSource, TEST_QUEUE_NAME)
        openBatch(dataSource, TEST_QUEUE_NAME, 42)


        then: "First consumed event is first enqueued"
        dequeue(dataSource, TEST_QUEUE_NAME)[0] == "event 1"
    }
}