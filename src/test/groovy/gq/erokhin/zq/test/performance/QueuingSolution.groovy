package gq.erokhin.zq.test.performance

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 09.11.17
 */

interface QueuingSolution {
    void createQueue(String queueName)
    void enqueue(String queueName, List<String> data)
    void dequeue(String queueName, int maxBatchSize)
}