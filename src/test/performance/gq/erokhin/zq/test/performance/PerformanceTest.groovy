package gq.erokhin.zq.test.performance

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
    Duration runTime
    Duration warmUpTime
    MetricRegistry metrics = new MetricRegistry()

    def enqueueRate
    def dequeueRate

    PerformanceTest(
            QueuingSolution solution,
            Duration runTime, Duration warmUpTime) {
        this.solution = solution
        this.runTime = runTime
        this.warmUpTime = warmUpTime
    }

    def testEnqueue(int chunkSize, int eventSize) {
        def data = generateData(chunkSize, eventSize)
        def endTime = now() + runTime
        def warmUpFinishTime = now() + warmUpTime
        def meter = null

        while (Thread.interrupted() || now() < endTime) {
            solution.enqueue(Helpers.TEST_QUEUE_NAME, data)
            if (now() > warmUpFinishTime) {
                if (!meter) {
                    meter = metrics.meter("Enqueue")
                }
                meter.mark(data.size())
            }
        }
        enqueueRate = meter.meanRate.round(3)
    }

    def testDequeue(eventSize, maxBatchSize) {
        //TODO: implement
    }

    def testEnqueueDequeue(eventSize, chunkSize, maxBatchSize) {
        //TODO: implement
    }


    static List<String> generateData(int chunkSize, int eventSize) {
        [(1..eventSize).collect { (('A'..'Z') + ('a'..'z'))[RANDOM.nextInt(52)] }.join()] * chunkSize
    }
}
