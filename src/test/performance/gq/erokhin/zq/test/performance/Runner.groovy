package gq.erokhin.zq.test.performance

import com.codahale.metrics.ConsoleReporter
import com.codahale.metrics.MetricRegistry

import javax.sql.DataSource
import java.time.Duration
import java.util.concurrent.TimeUnit

import static gq.erokhin.zq.test.ApiWrappers.createQueue
import static gq.erokhin.zq.test.Helpers.TEST_QUEUE_NAME
import static gq.erokhin.zq.test.Helpers.createDatasource
import static gq.erokhin.zq.test.Helpers.createSchema

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 10.11.17
 */

class Runner {

    static void main(String[] args) { // TODO: parametrise
        DataSource dataSource = createDatasource('localhost', 54321)
        QueuingSolution solution = new ZQSolution(dataSource)
        MetricRegistry metrics = new MetricRegistry()
        createSchema(dataSource) //TODO: move to build-script or later?
        createQueue(dataSource, TEST_QUEUE_NAME)
        PerformanceTest test = new PerformanceTest(solution, metrics, Duration.ofSeconds(20))
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build()
        reporter.start(1, TimeUnit.SECONDS)
        test.testEnqueue(16, 10000)
    }

}
