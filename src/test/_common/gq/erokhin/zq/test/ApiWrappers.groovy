package gq.erokhin.zq.test

import groovy.sql.Sql

import javax.sql.DataSource
import java.sql.Connection
import java.sql.Types

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 24.10.17
 */

class ApiWrappers {

    static boolean createQueue(DataSource ds, String queueName) {
        Helpers.executeCall(ds, "{ ? = call zq.create_queue(?) }", Types.BOOLEAN, queueName) as boolean
    }

    static boolean dropQueue(DataSource ds, String queueName) {
        Helpers.executeCall(ds, "{ ? = call zq.drop_queue(?) }", Types.BOOLEAN, queueName) as boolean
    }

    static int openBatch(DataSource ds, String queueName, int maxBatchSize) {
        Helpers.executeCall(ds, "{ ? = call zq.open_batch(?, ?) }", Types.INTEGER, queueName, maxBatchSize) as int
    }

    static void closeBatch(DataSource ds, String queueName) {
        new Sql(ds).execute("SELECT FROM zq.close_batch($queueName)")
    }

    static void cancelBatch(DataSource ds, String queueName) {
        new Sql(ds).execute("SELECT FROM zq.cancel_batch($queueName)")
    }

    static void enqueue(DataSource ds, String queueName, Collection<String> data) {
        def sql = new Sql(ds)
        sql.cacheConnection {Connection conn ->
            sql.execute("SELECT FROM zq.enqueue(?, ?)", [queueName, conn.createArrayOf("TEXT", data.toArray())])
        }
        sql.close()
    }

    def static dequeue(DataSource ds, String queueName) {
        new Sql(ds).rows("SELECT * FROM zq.dequeue(?)", queueName)
                .collect { it[1] }
                .toList()
    }

    def static dequeueWithTS(DataSource ds, String queueName) {
        new Sql(ds).rows("SELECT * FROM zq.dequeue(?)", queueName)
                .collect { [it[0], it[1]] }
                .toList()
    }
}
