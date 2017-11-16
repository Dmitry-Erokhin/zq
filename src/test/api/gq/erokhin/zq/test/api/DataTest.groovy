package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.ApiWrappers
import gq.erokhin.zq.test.Helpers
import gq.erokhin.zq.test.ZQSpecification
import spock.lang.Unroll

import java.sql.SQLException

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 28.10.17
 */
class DataTest extends ZQSpecification {

    def "Enqueue successfully"() {
        given: "A queue"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        when: "Enqueue event"
        ApiWrappers.enqueue(dataSource, Helpers.TEST_QUEUE_NAME, ["event"])

        then: "No exception thrown"
    }

    def "Enqueue to NON existing queue throws exception"() {
        given: "No queue exists"

        when: "Enqueue event"
        ApiWrappers.enqueue(dataSource, Helpers.TEST_QUEUE_NAME, ["event"])

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$Helpers.TEST_QUEUE_NAME\" is not exists.")
    }


    @Unroll("[#iterationCount] #featureName")
    def "Dequeue returns events of batch size in proper order"() {
        given: "A queue with events and open batch"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)
        ApiWrappers.enqueue(dataSource, Helpers.TEST_QUEUE_NAME, inEvents)
        ApiWrappers.openBatch(dataSource, Helpers.TEST_QUEUE_NAME, maxBatchSize)

        expect: "Received expected amount of events in proper order"
        expectedEvents == ApiWrappers.dequeue(dataSource, Helpers.TEST_QUEUE_NAME)

        where:
        inEvents   | maxBatchSize || expectedEvents
        ['a', 'b'] | 10           || ['a', 'b']
        ['a', 'b'] | 1            || ['a']
        ['a']      | 10           || ['a']
    }

    def "Dequeue from NON existing queue throws exception"() {
        given: "No queue exists"

        when: "Dequeue events"
        ApiWrappers.dequeue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$Helpers.TEST_QUEUE_NAME\" is not exists.")
    }

    def "Dequeue from queue without open batch throws exception"() {
        given: "Queue without open batch"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        when: "Dequeue events"
        ApiWrappers.dequeue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"$Helpers.TEST_QUEUE_NAME\" does not have open batch.")
    }
}
