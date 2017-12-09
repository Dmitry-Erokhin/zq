package gq.erokhin.zq.test.performance

import javax.sql.DataSource

import static gq.erokhin.zq.test.Helpers.*

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 08.12.17
 */

abstract class AbstractPerformanceTest {
    static CSV_HEADER = 'Chunk size,Event size (symbols),Enqueue rate (events/sec),Dequeue rate (events/sec)'

    String name
    File resultsFile
    int[] chunkSizes
    int[] eventSizes
    String host
    int port


    def run() {
        resultsFile.delete()
        resultsFile << CSV_HEADER << '\n'

        DataSource dataSource = createDatasource(host, port)
        createSchema(dataSource)

        QueuingSolution zq = new ZQSolution(dataSource)
        zq.createQueue(TEST_QUEUE_NAME)

        println "Starting ${name.toLowerCase()}: chunk sizes = $chunkSizes, event sizes = $eventSizes."
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

    abstract TestResults test(QueuingSolution solution, int chunkSize, int eventSize)

    static class TestResults {
        double enqueueRate
        double dequeueRate
    }
}
