package gq.erokhin.zq.test.performance

/**
 * The reason of extracting this interface is competitive tests support (#22)
 *
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 09.11.17
 */
interface QueuingSolution {
    void createQueue(String queueName)

    void enqueue(String queueName, List<String> data)

    int dequeue(String queueName, int maxBatchSize)
}