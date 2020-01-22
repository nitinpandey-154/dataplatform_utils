package com.goibibo.dp.utils

import java.sql.Connection


import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest._
import org.scalatest.time.{Second, Seconds, Span}

import scala.concurrent._
import scala.concurrent.duration._

trait DockerMysqlService extends DockerKit {

    def mysqlPort = 3306

    val mysqlContainer: DockerContainer = DockerContainer("mysql:5.7.14")
        .withPorts(mysqlPort -> Some(mysqlPort))
        .withEnv("MYSQL_ROOT_PASSWORD=secret", "MYSQL_DATABASE=goibibo")
        .withReadyChecker(
            DockerReadyChecker
                .LogLineContains("MySQL init process done. Ready for start up.")
        )

    abstract override def dockerContainers: List[DockerContainer] = mysqlContainer :: super.dockerContainers
}

class MysqlUtilsTest extends FlatSpec
    with Matchers
    with DockerTestKit
    with DockerKitSpotify
    with DockerMysqlService with BeforeAndAfter with BeforeAndAfterAll {


    implicit val pc: PatienceConfig = PatienceConfig(Span(1, Seconds), Span(1, Second))

    val defaultConnectionSpecs = ConnectionSpecs(
        "com.mysql.jdbc.Driver",
        "jdbc",
        "mysql",
        "localhost",
        3306,
        "goibibo",
        "root",
        "secret"
    )

    def withConnection(impl: Connection => Any) {
        println("Container not ready..")
        val ports = getContainerState(mysqlContainer).getPorts()
        Await.ready(ports, Duration(60, SECONDS))
        println("Container ready now..")
        implicit val connection: Connection = MysqlUtils.createConnection(defaultConnectionSpecs).get
        try {
            impl(connection)
        }
        finally connection.close()
    }

    "mysql node" should "be ready with log line checker" in {
        isContainerReady(mysqlContainer).futureValue shouldBe true
        println(mysqlContainer.getIpAddresses().futureValue)
    }

    "Helper function " should "convert int to bool correctly" in {
        MysqlUtils.intToBool(0) shouldBe false
        MysqlUtils.intToBool(1) shouldBe true
        MysqlUtils.intToBool(-1) shouldBe true
    }

    "MysqlUtils " should " create a connection successfully " in withConnection { connection =>
        connection.isInstanceOf[java.sql.Connection]
    }


    it should " create a table successfully " in withConnection { connection =>
        isContainerReady(mysqlContainer).futureValue shouldBe true
        val ddl =
            """
              |CREATE TABLE IF NOT EXISTS `hotels_rateplan` (
              |  `id` int(11) NOT NULL AUTO_INCREMENT,
              |  `hotel_name` varchar(200),
              |  `plan_name` varchar(200),
              |  `price` integer,
              |  PRIMARY KEY (`id`)
              |  )
            """.stripMargin
        MysqlUtils.createTable(ddl)(connection)
    }


    it should " create index table successfully " in withConnection { connection =>
        val ddl = "create index idx1 on hotels_rateplan(id);"
        MysqlUtils.createTable(ddl)(connection)
    }


    it should " check if table exist correctly" in withConnection { connection =>
        assert(MysqlUtils.isTablePresent("goibibo", "hotels_rateplan")(connection))
        assert(!MysqlUtils.isTablePresent("goibibo", "invalid-table")(connection))
    }


    it should " check if table exist correctly without passing connection" in withConnection { connection =>
        assert(MysqlUtils.isTablePresent(defaultConnectionSpecs, "hotels_rateplan"))
        assert(!MysqlUtils.isTablePresent(defaultConnectionSpecs, "invalid-table"))
    }

    it should " list all the primary keys correctly" in withConnection { connection =>
        MysqlUtils.getPrimaryKeys("goibibo", "hotels_rateplan")(connection).head shouldBe "id"
        MysqlUtils.getPrimaryKeys(defaultConnectionSpecs, "hotels_rateplan").head shouldBe "id"

        intercept[Exception](
            MysqlUtils.getPrimaryKeys("goibibo", "invalid-table")(connection)

        )
        intercept[Exception](
            MysqlUtils.getPrimaryKeys(defaultConnectionSpecs, "invalid-table")
        )

    }

    it should " list all the indexes keys correctly" in withConnection { connection =>
        MysqlUtils.getIndexes("goibibo", "hotels_rateplan")(connection).head shouldBe "id"
        MysqlUtils.getIndexes(defaultConnectionSpecs, "hotels_rateplan").head shouldBe "id"

        intercept[Exception](
            MysqlUtils.getIndexes("goibibo", "invalid-table")(connection)
        )
        intercept[Exception](
            MysqlUtils.getIndexes(defaultConnectionSpecs, "invalid-table")
        )

    }


    it should "fetch record count correctly" in withConnection { connection =>

        val insertQuery = "INSERT INTO goibibo.hotels_rateplan(hotel_name,plan_name,price) values('hotel taj goa agonda','single',4342)"
        val statement = connection.createStatement()
        statement.executeUpdate(insertQuery)
        assert(MysqlUtils.getRecordCount("SELECT count(1) from goibibo.hotels_rateplan")(connection) == 1)
        assert(MysqlUtils.getRecordCount(defaultConnectionSpecs, "SELECT count(1) from goibibo.hotels_rateplan") == 1)
        intercept[Exception](
            assert(MysqlUtils.getRecordCount("SELECT count(1) from goibibo.invalid-table")(connection) == 1)
        )

        intercept[Exception](
            assert(MysqlUtils.getRecordCount(defaultConnectionSpecs, "SELECT count(1) from goibibo.invalid-table") == 1)
        )
    }

    it should "list all the columns" in withConnection { connection =>
        assert(MysqlUtils.getColumnList("hotels_rateplan")(connection).length == 4)
        assert(MysqlUtils.getColumnList(defaultConnectionSpecs, "hotels_rateplan").length == 4)
        intercept[Exception](
            MysqlUtils.getColumnList("invalid-table")(connection)
        )
    }

    it should "drop mysql table successfully" in withConnection { connection =>

        MysqlUtils.dropTable("goibibo", "hotels_rateplan")(connection)
        intercept[Exception](
            MysqlUtils.dropTable("goibibo", "hotels_rateplan")(connection)
        )
    }

}
