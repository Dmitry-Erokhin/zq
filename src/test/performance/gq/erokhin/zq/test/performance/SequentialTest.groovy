package gq.erokhin.zq.test.performance

import com.codahale.metrics.MetricRegistry

import javax.sql.DataSource
import java.nio.file.Paths
import java.time.Duration

import static gq.erokhin.zq.test.ApiWrappers.createQueue
import static gq.erokhin.zq.test.Helpers.*
import static java.time.LocalDateTime.now

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 10.11.17
 */
class SequentialTest {
    static FILE_NAME = 'perftest_sequential_results.csv'
    static CSV_HEADER = 'Chunk size,Event size (symbols),Enqueue rate (events/sec),Dequeue rate (events/sec)'

    File resultsFile
    Duration enqueueRuntime
    Duration warmUpTime
    int[] chunkSizes
    int[] eventSizes
    int postgresPort

    SequentialTest(options) {
        def outDir = options.'output-dir' ?: './'
        resultsFile = Paths.get(outDir, FILE_NAME).toFile()
        resultsFile.delete()
        resultsFile << CSV_HEADER << '\n'

        chunkSizes = options.'chunk-sizes'.trim().split(',').collect({ it as int }).toList()
        eventSizes = options.'event-sizes'.trim().split(',').collect({ it as int }).toList()

        enqueueRuntime = Duration.ofSeconds(options.'enqueue-runtime' as long)
        warmUpTime = Duration.ofSeconds((options.'warm-up-time' ?: 0) as long)

        postgresPort = (options.'pg-port' ?: 5432) as int
    }

    def run() {
        DataSource dataSource = createDatasource('localhost', postgresPort)
        createSchema(dataSource)
        createQueue(dataSource, TEST_QUEUE_NAME)

        QueuingSolution zq = new ZQSolution(dataSource)

        println "Start sequential tests: chunk sizes = $chunkSizes; event sizes = $eventSizes; " +
                "enqueue for ${enqueueRuntime.seconds} seconds."
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
        def data = generateData(chunkSize, eventSize)
        def warmUpFinishTime = now() + warmUpTime
        def enqueueFinishTime = now() + enqueueRuntime
        def meter = null

        while (now() < enqueueFinishTime && !Thread.currentThread().isInterrupted()) {
            solution.enqueue(TEST_QUEUE_NAME, data)
            if (now() > warmUpFinishTime) {
                if (!meter) {
                    meter = metrics.meter("Enqueue")
                }
                meter.mark(data.size())
            }
        }

        result.enqueueRate = meter.meanRate.round(3)

        warmUpFinishTime = now() + warmUpTime
        meter = null

        while (!Thread.currentThread().isInterrupted()) {
            def dequeuedCount = solution.dequeue(TEST_QUEUE_NAME, chunkSize)

            if (dequeuedCount == 0) {
                break
            }

            if (now() > warmUpFinishTime) {
                if (!meter) {
                    meter = metrics.meter("Dequeue")
                }
                meter.mark(dequeuedCount)
            }
        }

        result.dequeueRate = meter.meanRate.round(3)

        return result
    }

    static List<String> generateData(int chunkSize, int eventSize) {
        [(1..eventSize).collect { (('A'..'Z') + ('a'..'z'))[RANDOM.nextInt(52)] }.join()] * chunkSize
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "${SequentialTest.class.simpleName}.groovy <options> [output dir]")

        cli.c(longOpt: 'chunk-sizes', args: 1, required: true, 'Coma separated amount of events in chunk')
        cli.e(longOpt: 'event-sizes', args: 1, required: true, 'Coma separated sizes of event')
        cli.t(longOpt: 'enqueue-runtime', args: 1, required: true, 'Maximum time of single test execution in seconds')
        cli.w(longOpt: 'warm-up-time', args: 1, required: false, 'Warm up period in seconds (default – no warm up)')
        cli.p(longOpt: 'pg-port', args: 1, required: false, 'PostgreSQL port (default – 5432)')
        cli.o(longOpt: 'output-dir', args: 1, required: false, 'Directory path where result files will be stored (default – current dir)')

        new SequentialTest(cli.parse(args)).run()
    }

}
