package gq.erokhin.zq.test.helpers

import groovy.sql.Sql

import javax.sql.DataSource
import java.sql.CallableStatement
import java.sql.Connection
import java.sql.SQLException

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 24.10.17
 */
class TestHelpers {
    public static final String TEST_QUEUE_NAME = "test_queue"

    //Due to the problems with groovy sql, fallback to original java
    def static executeCall(DataSource dataSource, String call, int resultType, ... params) {
        def noResult = "NO RESULT MARKER"
        def result = noResult
        Connection conn = dataSource.getConnection()
        try {
            CallableStatement statement = conn.prepareCall(call)
            statement.registerOutParameter(1, resultType)
            params.eachWithIndex { param, idx ->
                statement.setObject(idx + 2, param)
            }
            statement.execute()
            result = statement.getObject(1)
        } finally {
            conn.close()
        }

        if (result == noResult) {
            throw new SQLException("Can not perform db call")
        }

        result
    }

    static void createSchema(DataSource dataSource) {

        new Sql(dataSource).cacheConnection { Connection conn ->
            new File("./src/sql")
                    .listFiles()
                    .findAll({ it.name.endsWith('.sql') })
                    .sort({ it.name })
                    .forEach({ conn.createStatement().execute(it.text) })
        }
    }

    static void dropSchema(DataSource dataSource) {
        new Sql(dataSource).execute("DROP SCHEMA IF EXISTS zq CASCADE;")
    }

}
