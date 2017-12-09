package gq.erokhin.zq.test.performance

import com.codahale.metrics.MetricRegistry

import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import static gq.erokhin.zq.test.Helpers.TEST_QUEUE_NAME
import static gq.erokhin.zq.test.Helpers.generateRandomData
import static java.time.LocalDateTime.now

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 04.12.17
 */
class ParallelTest extends AbstractPerformanceTest {
    static FILE_NAME = 'perftest_parallel_results.csv'

    Duration testRuntime
    Duration dequeueDelay
    Duration warmUpTime

    @Override
    TestResults test(QueuingSolution solution, int chunkSize, int eventSize) {
        MetricRegistry metrics = new MetricRegistry()
        def result = new TestResults()
        def data = generateRandomData(chunkSize, eventSize)
        def testEndTime = now() + testRuntime
        def executor = Executors.newCachedThreadPool()

        def enqueueWorker = {
            def meter = null
            def startMeasureTime = now() + dequeueDelay + warmUpTime //start measuring after dequeuing will be started
            while (now() < testEndTime && !Thread.currentThread().isInterrupted()) {
                solution.enqueue(TEST_QUEUE_NAME, data)
                if (now() > startMeasureTime) {
                    if (!meter) {
                        meter = metrics.meter("Enqueue")
                    }
                    meter.mark(data.size())
                }
            }
            return meter.meanRate.round(3)
        }

        def dequeueWorker = {
            def meter = null
            def startMeasureTime = now() + warmUpTime
            while (now() < testEndTime && !Thread.currentThread().isInterrupted()) {
                def dequeuedCount = solution.dequeue(TEST_QUEUE_NAME, chunkSize)

                if (dequeuedCount == 0) {
                    println "Dequeue was interrupted due to absence of data in the queue."
                    return 0
                }

                if (now() > startMeasureTime) {
                    if (!meter) {
                        meter = metrics.meter("Dequeue")
                    }
                    meter.mark(dequeuedCount)
                }
            }
            return meter.meanRate.round(3)
        }

        Future<Double> enqueueRate = executor.submit(enqueueWorker as Callable<Double>)
        TimeUnit.SECONDS.sleep(dequeueDelay.seconds)
        Future<Double> dequeueRate = executor.submit(dequeueWorker as Callable<Double>)

        result.enqueueRate = enqueueRate.get()
        result.dequeueRate = dequeueRate.get()

        return result
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "${ParallelTest.class.simpleName}.groovy <options> [output dir]")

        cli.c(longOpt: 'chunk-sizes', args: 1, required: true, 'Coma separated amount of events in chunk')
        cli.e(longOpt: 'event-sizes', args: 1, required: true, 'Coma separated sizes of the event')
        cli.t(longOpt: 'test-runtime', args: 1, required: true, 'Duration of test in seconds')
        cli.t(longOpt: 'dequeue-delay', args: 1, required: true, 'Delay before start dequeuing in seconds')
        cli.w(longOpt: 'warmup-time', args: 1, required: false, 'Warm up duration in seconds (default – no warm up)')
        cli.h(longOpt: 'pg-host', args: 1, required: false, 'PostgreSQL host (default – localhost)')
        cli.p(longOpt: 'pg-port', args: 1, required: false, 'PostgreSQL port (default – 5432)')
        cli.o(longOpt: 'output-dir', args: 1, required: false, 'Directory path where result files will be stored (default – current dir)')

        def options = cli.parse(args)

        new ParallelTest(
                name: 'Parallel enq/deq',
                resultsFile: Paths.get(options.'output-dir' ?: './', FILE_NAME).toFile(),
                chunkSizes: options.'chunk-sizes'.trim().split(',').collect({ it as int }).toList(),
                eventSizes: options.'event-sizes'.trim().split(',').collect({ it as int }).toList(),
                testRuntime: Duration.ofSeconds(options.'test-runtime' as long),
                dequeueDelay: Duration.ofSeconds(options.'dequeue-delay' as long),
                warmUpTime: Duration.ofSeconds((options.'warmup-time' ?: 0) as long),
                host: (options.'pg-host' ?: 'localhost'),
                port: (options.'pg-port' ?: 5432) as int
        ).run()
    }

}
