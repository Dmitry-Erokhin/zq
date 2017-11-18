package gq.erokhin.zq.test.performance

import javax.sql.DataSource
import java.nio.file.Paths
import java.time.Duration

import static gq.erokhin.zq.test.ApiWrappers.createQueue
import static gq.erokhin.zq.test.Helpers.*

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 10.11.17
 */
class Runner {
    static ENQUEUE_FILE_NAME = 'pt_enqueue.csv'
    static DEQUEUE_FILE_NAME = 'pt_dequeue.csv'
    static MIXED_FILE_NAME = 'pt_mixed.csv'
    static CSV_HEADER_ENQUEUE = 'Chunk size,Event size (symbols),Enqueue rate (events/sec)'
    static CSV_HEADER_DEQUEUE = 'Chunk size,Event size (symbols),Dequeue rate (events/sec)'
    static CSV_HEADER_MIXED = 'Chunk size,Event size (symbols),Enqueue rate (events/sec),Dequeue rate (events/sec)'

    File enqueueReportFile
    File dequeueReportFile
    File mixedReportFile
    Duration singleTestRunTime
    Duration warmUpTestRunTime
    int[] chunkSizes
    int[] eventSizes
    int postgresPort

    Runner(options) {
        def outDir = options.'output-dir' ?: './'
        enqueueReportFile = Paths.get(outDir, ENQUEUE_FILE_NAME).toFile()
        dequeueReportFile = Paths.get(outDir, DEQUEUE_FILE_NAME).toFile()
        mixedReportFile = Paths.get(outDir, MIXED_FILE_NAME).toFile()

        enqueueReportFile.delete()
        dequeueReportFile.delete()
        mixedReportFile.delete()

        enqueueReportFile << CSV_HEADER_ENQUEUE << '\n'
        dequeueReportFile << CSV_HEADER_DEQUEUE << '\n'
        mixedReportFile << CSV_HEADER_MIXED << '\n'

        singleTestRunTime = Duration.ofSeconds(options.'run-time' as long)
        warmUpTestRunTime = Duration.ofSeconds((options.'warm-up-time' ?: 0) as long)

        chunkSizes = options.'chunk-sizes'.trim().split(',').collect({ it as int }).toList()
        eventSizes = options.'event-sizes'.trim().split(',').collect({ it as int }).toList()

        postgresPort = (options.'pg-port' ?: 5432) as int
    }

    def run() {
        DataSource dataSource = createDatasource('localhost', postgresPort)
        createSchema(dataSource)
        createQueue(dataSource, TEST_QUEUE_NAME)

        QueuingSolution zq = new ZQSolution(dataSource)

        for (chunkSize in chunkSizes) {
            for (eventSize in eventSizes) {
                println "Start enqueuing test. Chunk size = $chunkSize. Event size = $eventSize chars."
                PerformanceTest test = new PerformanceTest(zq, singleTestRunTime, warmUpTestRunTime)
                test.testEnqueue(chunkSize as int, eventSize as int)
                enqueueReportFile << "$chunkSize,$eventSize,${test.enqueueRate}" << '\n'
                println "Enqueuing test finished, rate = ${test.enqueueRate} events/sec \n"
            }
        }

        println "All tests are finished."
    }

    static void main(String[] args) {
        def cli = new CliBuilder(usage: "${Runner.class.simpleName}.groovy <options> [output dir]")

        cli.t(longOpt: 'run-time', args: 1, required: true, 'Maximum time of single test execution in seconds')
        cli.w(longOpt: 'warm-up-time', args: 1, required: false, 'Warm up period in seconds (default – no warm up)')
        cli.e(longOpt: 'event-sizes', args: 1, required: true, 'Coma separated sizes of event')
        cli.c(longOpt: 'chunk-sizes', args: 1, required: true, 'Coma separated amount of events in chunk')
        cli.o(longOpt: 'output-dir', args: 1, required: false, 'Directory path where result files will be stored (default – current dir)')
        cli.p(longOpt: 'pg-port', args: 1, required: false, 'PostgreSQL port (default – 5432)')

        new Runner(cli.parse(args)).run()
    }

}
