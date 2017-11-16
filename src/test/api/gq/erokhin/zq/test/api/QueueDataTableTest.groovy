package gq.erokhin.zq.test.api

import gq.erokhin.zq.test.ZQSpecification
import groovy.sql.Sql

import static gq.erokhin.zq.test.ApiWrappers.closeBatch
import static gq.erokhin.zq.test.ApiWrappers.createQueue
import static gq.erokhin.zq.test.ApiWrappers.dropQueue
import static gq.erokhin.zq.test.ApiWrappers.enqueue
import static gq.erokhin.zq.test.ApiWrappers.openBatch
import static gq.erokhin.zq.test.Helpers.TEST_QUEUE_NAME
import static gq.erokhin.zq.test.Helpers.isTableExists

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class QueueDataTableTest extends ZQSpecification {

    def "Queue creation creates corresponding data table"() {
        when: "Creating a queue"
        createQueue(dataSource, TEST_QUEUE_NAME)

        then: "Data table is also created"
        isTableExists(dataSource,"queue_1")
    }

    def "Queue dropping also drops corresponding data table"() {
        given: "A queue"
        createQueue(dataSource, TEST_QUEUE_NAME)

        when: "Dropping a queue"
        dropQueue(dataSource, TEST_QUEUE_NAME)

        then: "Data table is also dropped"
        !isTableExists(dataSource,"queue_1")
    }


    def "Close of batch deletes consumed events"() {
        given: "A queue with 11 events"
        createQueue(dataSource, TEST_QUEUE_NAME)
        enqueue(dataSource, TEST_QUEUE_NAME, (1..11).collect {"event $it"})

        when: "Open batch of 7"
        openBatch(dataSource, TEST_QUEUE_NAME, 7)

        and: "Close it"
        closeBatch(dataSource, TEST_QUEUE_NAME)

        then: "Size of queue table should be 4"
        new Sql(dataSource).firstRow("SELECT count(*) as rowCount FROM zq.queue_1").rowCount == 4
    }

}
