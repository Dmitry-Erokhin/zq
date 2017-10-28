package gq.erokhin.zq.test.helpers

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class ZQSpecification extends Specification {

    @Shared
    DataSource dataSource

    def setupSpec() {
        HikariConfig hikariConfig = new HikariConfig()
        hikariConfig.setJdbcUrl("jdbc:postgresql://localhost:54321/postgres")
        hikariConfig.setUsername("postgres")
        hikariConfig.setPassword("postgres")
        dataSource = new HikariDataSource(hikariConfig)
    }

    def cleanupSpec() {
        (dataSource as HikariDataSource).close()
    }

    def setup() {
        TestHelpers.createSchema(dataSource)
    }

    def cleanup() {
        TestHelpers.dropSchema(dataSource)
    }
}
