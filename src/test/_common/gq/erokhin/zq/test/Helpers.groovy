package gq.erokhin.zq.test

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import groovy.sql.Sql

import javax.sql.DataSource
import java.sql.*

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 24.10.17
 */
class Helpers {
    public static final String TEST_QUEUE_NAME = "test_queue"
    def static RANDOM = new Random()

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

    static boolean isTableExists(DataSource dataSource, String tableName) {
        new Sql(dataSource).rows("""
                 SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'zq' AND table_name = ?""", tableName).size() == 1
    }

    static DataSource createDatasource(host, port,
                                       databaseName = 'postgres',
                                       userName = 'postgres',
                                       password = 'postgres') {
        HikariConfig hikariConfig = new HikariConfig()
        hikariConfig.setJdbcUrl("jdbc:postgresql://$host:$port/$databaseName")
        hikariConfig.setUsername(userName)
        hikariConfig.setPassword(password)
        new HikariDataSource(hikariConfig)
    }

    static {
        Sql.metaClass.eachRowLazy = { String query, int fetchSize, Closure consumer ->
            def oldResultSetType = delegate.resultSetType
            def oldResultSetConcurrency = delegate.resultSetConcurrency
            delegate.cacheConnection { Connection conn ->
                boolean autoCommitOriginalState = conn.autoCommit
                try {
                    conn.autoCommit = false
                    delegate.resultSetType = ResultSet.TYPE_FORWARD_ONLY
                    delegate.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
                    delegate.withStatement { final Statement stmt ->
                        stmt.fetchSize = fetchSize
                    }
                    delegate.eachRow(query, consumer)
                } finally {
                    if (!conn.closed) {
                        conn.autoCommit = autoCommitOriginalState
                        delegate.resultSetType = oldResultSetType
                        delegate.resultSetConcurrency = oldResultSetConcurrency
                    }
                }
            }
        }
    }
}
