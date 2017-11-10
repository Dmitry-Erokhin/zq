package gq.erokhin.zq.test.performance

import com.codahale.metrics.MetricRegistry
import gq.erokhin.zq.test.helpers.TestHelpers

import static gq.erokhin.zq.test.helpers.TestHelpers.RANDOM

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 09.11.17
 */
class PerformanceTest {
    QueuingSolution queuingSolution
    MetricRegistry metrics
    def enqueueTimer = metrics.timer("Enqueue")
    def dequeueTimer = metrics.timer("Dequeue")


    def testEnqueue(int eventSize, int chunkSize) {
        def data = generateData(eventSize, chunkSize)
        while (true) {
            def ctx = enqueueTimer.time()
            queuingSolution.enqueue(TestHelpers.TEST_QUEUE_NAME, data)
            ctx.close()
        }
    }

    def testDequeue(eventSize, maxBatchSize) {

    }

    def testEnqueueDequeue(eventSize, chunkSize, maxBatchSize) {

    }

    static List<String> generateData(int eventSize, int chunkSize) {
        [(1..eventSize).collect { (('A'..'Z') + ('a'..'z'))[RANDOM.nextInt(52)] }.join()] * chunkSize
    }
}
