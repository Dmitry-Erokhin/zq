package gq.erokhin.zq.test.performance

import com.codahale.metrics.Meter
import com.codahale.metrics.MetricRegistry
import gq.erokhin.zq.test.Helpers

import java.time.Duration

import static Helpers.RANDOM
import static java.time.LocalDateTime.now

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 09.11.17
 */
class PerformanceTest {
    QueuingSolution solution
    Duration maxRunTime
    long maxEvents
    MetricRegistry metrics
    Meter enqueueMeter
    Meter dequeueMeter

    PerformanceTest(
            QueuingSolution solution,
            MetricRegistry metrics,
            Duration maxRunTime = Duration.ofDays(365),
            long maxEvents = Long.MAX_VALUE) {
        this.solution = solution
        this.maxRunTime = maxRunTime
        this.maxEvents = maxEvents
        this.metrics = metrics
        this.enqueueMeter = metrics.meter("Enqueue")
        this.dequeueMeter = metrics.meter("Dequeue")
    }

    def testEnqueue(int eventSize, int chunkSize) {
        def data = generateData(eventSize, chunkSize)
        def endTime = now() + maxRunTime
        long enqueuedEvents = 0

        while (Thread.interrupted() || now() < endTime || enqueuedEvents > maxEvents) {
            solution.enqueue(Helpers.TEST_QUEUE_NAME, data)
            enqueueMeter.mark(data.size())
            enqueuedEvents += data.size()
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
