package gq.erokhin.zq.test.impl

import gq.erokhin.zq.test.helpers.ZQSpecification
import groovy.sql.Sql

import static gq.erokhin.zq.test.helpers.ApiWrappers.createQueue
import static gq.erokhin.zq.test.helpers.ApiWrappers.dropQueue
import static gq.erokhin.zq.test.helpers.TestHelpers.TEST_QUEUE_NAME
import static gq.erokhin.zq.test.helpers.TestHelpers.isTableExists

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

}
