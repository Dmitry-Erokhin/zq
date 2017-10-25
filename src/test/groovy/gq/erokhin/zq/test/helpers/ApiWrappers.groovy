package gq.erokhin.zq.test.helpers

import groovy.sql.Sql

import javax.sql.DataSource
import java.sql.Types

import static gq.erokhin.zq.test.helpers.TestHelpers.executeCall

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 24.10.17
 */

class ApiWrappers {

    static boolean createQueue(DataSource ds, String queueName) {
        executeCall(ds, "{ ? = call zq.create_queue(?) }", Types.BOOLEAN, queueName) as boolean
    }

    static boolean dropQueue(DataSource ds, String queueName) {
        executeCall(ds, "{ ? = call zq.drop_queue(?) }", Types.BOOLEAN, queueName) as boolean
    }

    static int openBatch(DataSource ds, String queueName) {
        executeCall(ds, "{ ? = call zq.open_batch(?) }", Types.INTEGER, queueName) as int
    }

    static int closeBatch(DataSource ds, String queueName) {
        executeCall(ds, "{ ? = call zq.close_batch(?) }", Types.INTEGER, queueName) as int
    }

    static int enqueue(DataSource ds, String queueName, String[] data) {
        executeCall(ds, "{ ? = call zq.enqueue(?, ?) }", Types.INTEGER, queueName, data) as int
    }

    def static dequeue(DataSource ds, String queueName) {
        new Sql(ds).rows("select * from zq.dequeue(?)", queueName)
                .collect { it[2] }
                .toList()
    }

    def static dequeueWithTS(DataSource ds, String queueName) {
        new Sql(ds).rows("select * from zq.dequeue(?)", queueName)
                .collect { [it[1], it[2]] }
                .toList()
    }
}
