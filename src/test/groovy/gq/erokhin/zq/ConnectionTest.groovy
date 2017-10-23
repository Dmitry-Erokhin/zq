package gq.erokhin.zq

import java.sql.ResultSet
import java.sql.Statement

/**
 * Created by Dmitry Erokhin (dmitry.erokhin@gmail.com)
 * 23.10.17
 */
class ConnectionTest extends ZQSpecification {

    def "Database is accessible"() {
        when: "Querying the database"
        Statement statement = dataSource.getConnection().createStatement()
        statement.execute("SELECT 1")
        ResultSet resultSet = statement.getResultSet()
        resultSet.next()

        then: "Result is returned"
        resultSet.getInt(1) == 1
    }
}