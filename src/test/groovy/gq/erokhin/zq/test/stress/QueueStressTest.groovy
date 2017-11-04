package gq.erokhin.zq.test.stress

import gq.erokhin.zq.test.helpers.ZQSpecification
import groovy.sql.Sql
import groovyx.gpars.GParsPool

import java.sql.SQLException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import static gq.erokhin.zq.test.helpers.ApiWrappers.*
import static gq.erokhin.zq.test.helpers.TestHelpers.RANDOM
import static gq.erokhin.zq.test.helpers.TestHelpers.TEST_QUEUE_NAME

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class QueueStressTest extends ZQSpecification {

    def static TEST_QUEUES_COUNT = 30
    def static PARALLEL_WORKERS_COUNT = 42

    def static TEST_QUEUE_NAMES = (1..TEST_QUEUES_COUNT).collect { "queue_$it" }

    def "Queue creation in parallel works correctly"() {
        given: "List of queues to create"

        when: "Many threads creates all queues in parallel with random delays"
        AtomicInteger count = new AtomicInteger()
        GParsPool.withPool(PARALLEL_WORKERS_COUNT) {
            (1..PARALLEL_WORKERS_COUNT).eachParallel {
                def queueCreationList = TEST_QUEUE_NAMES.collect()
                Collections.shuffle(queueCreationList)
                for (queue in queueCreationList) {
                    if (createQueue(dataSource, queue)) {
                        count.incrementAndGet()
                    }
                    Thread.sleep(RANDOM.nextInt(5))
                }
            }
        }

        then: "Amount of successful creations matches size of queues list size"
        count.get() == TEST_QUEUE_NAMES.size()

        and: "Queues table content matches queues list"
        def queuesNamesFromDb = new Sql(dataSource)
                .rows("SELECT que_name AS name FROM zq.queues")
                .collect({ it.name})
        queuesNamesFromDb.size() == TEST_QUEUE_NAMES.size()
        queuesNamesFromDb.toSet() == TEST_QUEUE_NAMES.toSet()

        and: "Corresponding data tables were created"
        new Sql(dataSource).rows("""
                 SELECT table_name
                 FROM information_schema.tables
                 WHERE 1=1 
                  AND table_schema = 'zq' 
                  AND table_name LIKE 'queue\\_%'""").size() == TEST_QUEUE_NAMES.size()
    }

    def "Queue dropping in parallel works correctly"() {
        given: "Queues"
        TEST_QUEUE_NAMES.each { createQueue(dataSource, it) }

        when: "Many threads drops all of these queues in parallel with random delays"
        AtomicInteger count = new AtomicInteger()
        GParsPool.withPool(PARALLEL_WORKERS_COUNT) {
            (1..PARALLEL_WORKERS_COUNT).eachParallel {
                def queueDeletionList = TEST_QUEUE_NAMES.collect()
                Collections.shuffle(queueDeletionList)
                for (queue in queueDeletionList) {
                    if (dropQueue(dataSource, queue)) {
                        count.incrementAndGet()
                    }
                    Thread.sleep(RANDOM.nextInt(5))
                }
            }
        }

        then: "Amount of successful drop operations matches size of queues list size"
        count.get() == TEST_QUEUE_NAMES.size()

        and: "Queues table is empty"
        new Sql(dataSource).firstRow("SELECT count(*) AS queues FROM zq.queues").queues == 0

        and: "Corresponding data tables were dropped as well"
        new Sql(dataSource).rows("""
                 SELECT table_name
                 FROM information_schema.tables
                 WHERE 1=1 
                  AND table_schema = 'zq' 
                  AND table_name LIKE 'queue\\_%'""").size() == 0
    }

}
