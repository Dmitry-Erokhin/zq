package gq.erokhin.zq.test

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
        dataSource = Helpers.createDatasource('localhost', 54321)
    }

    def cleanupSpec() {
        (dataSource as HikariDataSource).close()
    }

    def setup() {
        Helpers.createSchema(dataSource)
    }

    def cleanup() {
        Helpers.dropSchema(dataSource)
    }
}
