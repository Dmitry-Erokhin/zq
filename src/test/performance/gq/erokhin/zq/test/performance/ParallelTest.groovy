package gq.erokhin.zq.test.performance

import com.codahale.metrics.MetricRegistry

import javax.sql.DataSource
import java.nio.file.Paths
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit

import static gq.erokhin.zq.test.Helpers.*
import static java.time.LocalDateTime.now

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 04.12.17
 */
class ParallelTest {
    static FILE_NAME = 'perftest_parallel_results.csv'
    static CSV_HEADER = 'Chunk size,Event size (symbols),Enqueue rate (events/sec),Dequeue rate (events/sec)'

    File resultsFile
    Duration testRuntime
    Duration dequeueDelay
    Duration warmUpTime
    int[] chunkSizes
    int[] eventSizes
    int postgresPort

    ParallelTest(options) {
        def outDir = options.'output-dir' ?: './'
        resultsFile = Paths.get(outDir, FILE_NAME).toFile()
        resultsFile.delete()
        resultsFile << CSV_HEADER << '\n'

        chunkSizes = options.'chunk-sizes'.trim().split(',').collect({ it as int }).toList()
        eventSizes = options.'event-sizes'.trim().split(',').collect({ it as int }).toList()

        testRuntime = Duration.ofSeconds(options.'test-runtime' as long)
        dequeueDelay = Duration.ofSeconds(options.'dequeue-delay' as long)
        warmUpTime = Duration.ofSeconds((options.'warmup-time' ?: 0) as long)

        postgresPort = (options.'pg-port' ?: 5432) as int
    }

    def run() {
        DataSource dataSource = createDatasource('localhost', postgresPort)
        createSchema(dataSource)

        QueuingSolution zq = new ZQSolution(dataSource)
        zq.createQueue(TEST_QUEUE_NAME)

        println "Start parallel tests: chunk sizes = $chunkSizes; event sizes = $eventSizes; " +
                "enqueue for ${testRuntime.seconds} seconds."
        for (chunkSize in chunkSizes) {
            for (eventSize in eventSizes) {
                def results = test(zq, chunkSize, eventSize)
                resultsFile << "$chunkSize,$eventSize,${results.enqueueRate},${results.dequeueRate}" << '\n'
                println "Test for chunk of $chunkSize and event size of $eventSize finished. Results: " +
                        "enqueue rate = ${results.enqueueRate} events/sec, " +
                        "dequeue rate = ${results.dequeueRate} events/sec. "
            }
        }

        println "All sequential tests finished, results stored in $resultsFile"
    }


    def test(QueuingSolution solution, int chunkSize, int eventSize) {
        MetricRegistry metrics = new MetricRegistry()
        def result = new Expando()
        def data = generateRandomData(chunkSize, eventSize)
        def testEndTime = now() + testRuntime
        def executor = Executors.newCachedThreadPool()

        def enqueueWorker = {
            def meter = null
            def warmUpFinishTime = now() + warmUpTime
            while (now() < testEndTime && !Thread.currentThread().isInterrupted()) {
                solution.enqueue(TEST_QUEUE_NAME, data)
                if (now() > warmUpFinishTime) {
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
            def warmUpFinishTime = now() + warmUpTime
            while (now() < testEndTime && !Thread.currentThread().isInterrupted()) {
                def dequeuedCount = solution.dequeue(TEST_QUEUE_NAME, chunkSize)

                if (dequeuedCount == 0) {
                    println "Dequeue was interrupted due to absence of data. Reconsider test params."
                    return 0
                }

                if (now() > warmUpFinishTime) {
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
        cli.p(longOpt: 'pg-port', args: 1, required: false, 'PostgreSQL port (default – 5432)')
        cli.o(longOpt: 'output-dir', args: 1, required: false, 'Directory path where result files will be stored (default – current dir)')

        new ParallelTest(cli.parse(args)).run()
    }

}
