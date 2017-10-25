package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.helpers.ZQSpecification

import java.sql.SQLException

import static gq.erokhin.zq.test.helpers.ApiWrappers.*
import static gq.erokhin.zq.test.helpers.TestHelpers.createSchema
import static gq.erokhin.zq.test.helpers.TestHelpers.dropSchema

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class BatchManagementTest extends ZQSpecification {

    void setup() {
        createSchema(dataSource)
    }

    def cleanup() {
        dropSchema(dataSource)
    }

    def "Open batch on empty queue returns 0"() {
        given: "A queue"
        createQueue(dataSource, "test_queue")

        when: "Open a batch"
        def result = openBatch(dataSource, "test_queue")

        then: "True is returned"
        result == 0
    }

    def "Open batch for non existing queue throws an exception"() {
        when: "Open a new batch for non existing queue"
        openBatch(dataSource, "non_existing_test_queue")

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"non_existing_test_queue\" is not exists.")
    }

    def "Open batch twice throws an exception"() {
        given: "A queue with open batch"
        createQueue(dataSource, "test_queue")
        openBatch(dataSource, "test_queue")

        when: "Open a new batch"
        openBatch(dataSource, "test_queue")

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"test_queue\" has already open batch.")
    }

    def "Close open batch successfully"() {
        given: "A queue with open batch"
        createQueue(dataSource, "test_queue")
        openBatch(dataSource, "test_queue")

        when: "Close the batch"
        closeBatch(dataSource, "test_queue")

        then: "No exception thrown"
    }

    def "Close batch for non existing queue throws an exception"() {
        when: "Close a batch for non existing queue"
        openBatch(dataSource, "non_existing_test_queue")

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"non_existing_test_queue\" is not exists.")
    }

    def "Close batch for a queue without open batch throws an exception"() {
        given: "A queue without open batch"
        createQueue(dataSource, "test_queue")

        when: "Close a batch"
        openBatch(dataSource, "test_queue")

        then: "Exception is thrown"
        def ex = thrown(SQLException)
        ex.getMessage().contains("Queue \"test_queue\" does not have open batch.")
    }
}