package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.helpers.ZQSpecification

import static gq.erokhin.zq.test.helpers.ApiWrappers.createQueue
import static gq.erokhin.zq.test.helpers.ApiWrappers.dropQueue
import static gq.erokhin.zq.test.helpers.TestHelpers.TEST_QUEUE_NAME

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class QueueManagementTest extends ZQSpecification {

    def "Queue created and returns true"() {
        when: "Creating a queue"
        def result = createQueue(dataSource, TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }

    def "Duplicate queue creation returns false"() {
        when: "Creating two queues of same name"
        createQueue(dataSource, TEST_QUEUE_NAME)
        def result = createQueue(dataSource, TEST_QUEUE_NAME)

        then: "False is returned for second attempt"
        !result
    }

    def "Delete of existing queue returns true"() {
        given: "A queue"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Delete the queue"
        def result = dropQueue(dataSource, TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }

    def "Delete of NON existing queue returns false"() {
        given: "No queue exists"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Delete this queue"
        def result = dropQueue(dataSource, TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }
}