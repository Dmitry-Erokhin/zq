package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.helpers.ZQSpecification
import spock.lang.Unroll

import java.sql.SQLException

import static gq.erokhin.zq.test.helpers.ApiWrappers.*
import static gq.erokhin.zq.test.helpers.TestHelpers.TEST_QUEUE_NAME

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 28.10.17
 */
class DataTest extends ZQSpecification {

    def "Enqueue successfully"() {
        given: "A queue"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Enqueue event"
        enqueue(dataSource, "event")

        then: "No exception thrown"
    }

    def "Enqueue to NON existing queue throws exception"() {
        given: "No queue exists"

        when: "Enqueue event"
        enqueue(dataSource, "event")

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" is not exists.")
    }


    @Unroll("[#iterationCount] #featureName")
    def "Dequeue returns events of batch size in proper order"() {
        given: "A queue with events and open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)
        inEvents.each { enqueue(dataSource, TEST_QUEUE_NAME, it) }
        openBatch(dataSource, TEST_QUEUE_NAME, maxBatchSize)

        expect: "Received expected amount of events in proper order"
        expectedEvents == dequeue(dataSource, TEST_QUEUE_NAME)

        where:
        inEvents   | maxBatchSize || expectedEvents
        []         | 10           || []
        ['a', 'b'] | 10           || ['a', 'b']
        ['a', 'b'] | 0            || []
        ['a', 'b'] | 1            || ['a']
    }

    def "Dequeue from NON existing queue throws exception"() {
        given: "No queue exists"

        when: "Dequeue events"
        dequeue(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" is not exists.")
    }

    def "Dequeue from queue without open batch throws exception"() {
        given: "Queue without open batch"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Dequeue events"
        dequeue(dataSource, TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$TEST_QUEUE_NAME\" does not have open batch.")
    }
}
