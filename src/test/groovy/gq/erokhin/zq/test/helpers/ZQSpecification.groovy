package gq.erokhin.zq.test.helpers

import com.zaxxer.hikari.HikariDataSource
import spock.lang.Shared
import spock.lang.Specification

import javax.sql.DataSource

import static gq.erokhin.zq.test.helpers.TestHelpers.createDatasource

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class ZQSpecification extends Specification {

    @Shared
    DataSource dataSource

    def setupSpec() {
        dataSource = createDatasource('localhost', 54321)
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
