package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.ApiWrappers
import gq.erokhin.zq.test.Helpers
import gq.erokhin.zq.test.ZQSpecification

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class QueuesTest extends ZQSpecification {

    def "Queue created and returns true"() {
        when: "Creating a queue"
        def result = ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }

    def "Duplicate queue creation returns false"() {
        when: "Creating two queues of same name"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)
        def result = ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "False is returned for second attempt"
        !result
    }

    def "Delete of existing queue returns true"() {
        given: "A queue"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        when: "Delete the queue"
        def result = ApiWrappers.dropQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }

    def "Delete of NON existing queue returns false"() {
        given: "No queue exists"
        ApiWrappers.createQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        when: "Delete this queue"
        def result = ApiWrappers.dropQueue(dataSource, Helpers.TEST_QUEUE_NAME)

        then: "True is returned"
        result
    }
}
