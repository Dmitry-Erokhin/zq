package gq.erokhin.zq.test.performance

import javax.sql.DataSource

import static gq.erokhin.zq.test.ApiWrappers.*

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 10.11.17
 */

class ZQSolution implements QueuingSolution {

    private DataSource dataSource

    ZQSolution(DataSource dataSource) {
        this.dataSource = dataSource
    }

    @Override
    void createQueue(String queueName) {
        createQueue(dataSource, queueName)
    }

    @Override
    void enqueue(String queueName, List<String> data) {
        enqueue(dataSource, queueName, data)
    }

    @Override
    int dequeue(String queueName, int maxBatchSize) {
        if (openBatch(dataSource, queueName, maxBatchSize) == 0) {
            return 0
        }

        def events = dequeue(dataSource, queueName)
        closeBatch(dataSource, queueName)

        return events.size()
    }
}
