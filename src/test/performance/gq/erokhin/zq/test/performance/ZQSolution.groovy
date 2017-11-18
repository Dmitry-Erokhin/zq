package gq.erokhin.zq.test.performance

import gq.erokhin.zq.test.ApiWrappers

import javax.sql.DataSource

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
        ApiWrappers.createQueue(dataSource, queueName)
    }

    @Override
    void enqueue(String queueName, List<String> data) {
        ApiWrappers.enqueue(dataSource, queueName, data)
    }

    @Override
    void dequeue(String queueName, int maxBatchSize) {
        if (ApiWrappers.openBatch(dataSource, queueName, maxBatchSize) > 0) {
            ApiWrappers.dequeue(dataSource, queueName)
            ApiWrappers.closeBatch(dataSource, queueName)
        }
    }
}
