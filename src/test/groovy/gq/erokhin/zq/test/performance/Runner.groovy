package gq.erokhin.zq.test.performance

import com.codahale.metrics.MetricRegistry

import javax.sql.DataSource

import static gq.erokhin.zq.test.helpers.TestHelpers.createDatasource

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 10.11.17
 */

class Runner {

    def static main(... agrs) {
        DataSource dataSource = createDatasource('localhost', 54321)
        QueuingSolution solution = new ZQSolution(dataSource)
        MetricRegistry metrics = new MetricRegistry()
        PerformanceTest test = new PerformanceTest(
                metrics: metrics,
                queuingSolution: solution
                // max runtime
                // max amount of events
        )

        test.testEnqueue(16, 100)
    }

}
