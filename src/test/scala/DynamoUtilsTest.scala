package com.goibibo.dp.utils


import scala.util.Try
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.{Item, Table}
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.whisk.docker.{DockerContainer, DockerKit, DockerReadyChecker}
import com.whisk.docker.impl.spotify.DockerKitSpotify
import com.whisk.docker.scalatest.DockerTestKit
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.Matchers
import org.scalatest.time.{Second, Seconds, Span}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}

trait DockerDynamoService extends DockerKit {

    val dynamoPort: Int = 8000

    val dynamoContainer: DockerContainer = DockerContainer("amazon/dynamodb-local")
        .withPorts(dynamoPort -> Some(dynamoPort))
        .withReadyChecker(DockerReadyChecker.LogLineContains("Initializing DynamoDB Local with the following configuration"))


    abstract override def dockerContainers: List[DockerContainer] = dynamoContainer :: super.dockerContainers
}

class DynamoUtilsTest extends FlatSpec
    with Matchers
    with DockerTestKit
    with DockerKitSpotify
    with DockerDynamoService with BeforeAndAfter {

    implicit val pc: PatienceConfig = PatienceConfig(Span(20, Seconds), Span(1, Second))

    def withDynamoClient(localhost: String, port: Int, impl: AmazonDynamoDB => Any) {
        println("Container not ready..")
        val ports = getContainerState(dynamoContainer).getPorts()
        Await.ready(ports, Duration(60, SECONDS))
        println("Container ready now..")
        implicit val dynamoClient: AmazonDynamoDB = DynamoUtils.createDynamoClient(
            "ap-south-1", s"http://$localhost:$port")
        try {
            impl(dynamoClient)
        }
        finally dynamoClient.shutdown()
    }


    "DynamoDB client" should "be created successful" in withDynamoClient("localhost", 8000, dynamoClient =>
        assert(dynamoClient.isInstanceOf[AmazonDynamoDB])
    )

    it should "throw Exception if invalid dynamo endpoint is specified when creating a client" in withDynamoClient(
        "invalid-localhost", 8000, dynamoClient =>

            intercept[Exception] {
                dynamoClient.listTables()
            }
    )

    it should "should be tested successfully using the test connection method " in withDynamoClient(
        "localhost", 8000, dynamoClient =>

            DynamoUtils.testDynamoClient(dynamoClient) shouldBe true


    )

    it should "should be throw exception using the test connection method for invalid connection " in withDynamoClient(
        "invalid-localhost", 8000, dynamoClient =>

            DynamoUtils.testDynamoClient(dynamoClient) shouldBe false
    )


    it should "list all the tables" in withDynamoClient(
        "localhost", 8000, dynamoClient => {

            DynamoUtils.createTable(DynamoDBTableSpecs("my-test-table-5", "key", "string"))(dynamoClient)
            assert(DynamoUtils.listTables()(dynamoClient).contains("my-test-table-5"))
        }
    )

    it should "throw an exception when table does not exist and we call getDynamoTable" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "some-table-which-doesn't exist"
            intercept[Exception](DynamoUtils.getDynamoTable(tableName)(dynamoClient))

        })

    "DynamoDB datatypes" should "be returned correctly as per input dataypes" in {
        DynamoUtils.getDynamoDBType("STRING") shouldBe ScalarAttributeType.S
        DynamoUtils.getDynamoDBType("string") shouldBe ScalarAttributeType.S

        DynamoUtils.getDynamoDBType("int") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("INT") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("integer") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("INTEGER") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("NUMBER") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("number") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("BIGINT") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("bigint") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("LONG") shouldBe ScalarAttributeType.N
        DynamoUtils.getDynamoDBType("long") shouldBe ScalarAttributeType.N

        DynamoUtils.getDynamoDBType("binary") shouldBe ScalarAttributeType.B
        DynamoUtils.getDynamoDBType("BINARY") shouldBe ScalarAttributeType.B


    }

    "DynamoDB Table" should "be created successfully with just partition key specs" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "my-test-table-1"

            DynamoUtils.createTable(DynamoDBTableSpecs(tableName, "key", "string"))(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true
        }
    )

    it should "be created successfully with provisioned specs" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "my-test-table-2"

            val provisionedThroughput = DynamoDBProvisionedThroughput(10L, 10L)
            DynamoUtils.createTable(DynamoDBTableSpecs(
                tableName, "key", "string", None, Some(provisionedThroughput)))(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true

        })

    it should "be created successfully with sort key specs" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "my-test-table-3"

            val sortKeySpecs = DynamoDBSortKeySpecs("id", "number")
            DynamoUtils.createTable(DynamoDBTableSpecs(tableName, "key", "string", Some(sortKeySpecs)))(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true
        }
    )

    it should "be created successfully with sort key specs and provisioned throughput" in withDynamoClient(
        "localhost", 8000, dynamoClient => {

            val tableName = "my-test-table-4"

            val sortKeySpecs = DynamoDBSortKeySpecs("id", "number")
            val provisionedThroughput = DynamoDBProvisionedThroughput(10L, 10L)
            DynamoUtils.createTable(DynamoDBTableSpecs(
                tableName, "key", "string", Some(sortKeySpecs), Some(provisionedThroughput)))(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true
        }
    )

    "DynamoDB Item" should "be generated successfully" in {
        assert(DynamoUtils.generateItem("key",
            "string", "year", "1930").isInstanceOf[Item])
        assert(DynamoUtils.generateItem("key",
            "string", "year", 1930).isInstanceOf[Item])
        assert(DynamoUtils.generateItem("key", "string").isInstanceOf[Item])
    }

    it should "be enriched successfully" in {
        val item: Item = DynamoUtils.generateItem("key",
            "string", "year", "1930")
        val enrichedItem = DynamoUtils.enrichItem(
            item,
            DynamoColumn("artist", "Ma¬roon 5", "string"),
            DynamoColumn("song", "Sugar", "string"),
            DynamoColumn("duration", "310", "integer"),
            DynamoColumn("views", 24323245, "long"),
            DynamoColumn("likes", "343232", "long"),
            DynamoColumn("single", true, "boolean")
        )
        assert(enrichedItem.getBoolean("single"))
        assert(enrichedItem.getInt("duration") == 310)
        assert(enrichedItem.getString("song") == "Sugar")
        assert(enrichedItem.getInt("views") == 24323245)
        assert(enrichedItem.getInt("likes") == 343232)
        assert(enrichedItem.getString("likes") == "343232")
        assert(enrichedItem.getBoolean("single"))

    }

    it should "get written to a dynamo table with sort key" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "artists"

            DynamoUtils.createTable(
                DynamoDBTableSpecs(
                    tableName,
                    "artist-name",
                    "string",
                    Some(
                        DynamoDBSortKeySpecs(
                            "year",
                            "number"
                        )
                    )
                )
            )(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true
            val beautifulEminem = DynamoUtils.generateItem(
                "artist-name",
                "Eminem",
                "year",
                2009,
                DynamoColumn("uncensored", true, "boolean"),
                DynamoColumn("length", "3:40", "string")
            )
            val enriqueHero = DynamoUtils.generateItem(
                "artist-name",
                "Enrique",
                "year",
                "2001",
                DynamoColumn("uncensored", false, "boolean"),
                DynamoColumn("length", "4:40", "string")
            )

            val table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)

            DynamoUtils.write(beautifulEminem, table)
            DynamoUtils.write(enriqueHero, table)

        })

    "DynamoDB Table" should "allow items to be read - all attributes" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "artist-name",
                "Eminem",
                "year",
                2009)

            val tableName: String = "artists"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val item: Try[Item] = DynamoUtils.read(getItemSpec, table)
            if (item.isSuccess) {
                println(item.get.attributes())
            }

        })

    "DynamoDB Item" should "get written to a dynamo table without sort key" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val tableName = "job-status"

            DynamoUtils.createTable(
                DynamoDBTableSpecs(
                    tableName,
                    "job-id",
                    "string"
                )
            )(dynamoClient)
            DynamoUtils.checkTableExists(tableName)(dynamoClient) shouldBe true
            val failedJob = DynamoUtils.generateItem(
                "job-id",
                "123",
                DynamoColumn("timestamp", "2019-11-10T21:04:22", "string")
            )
            val successJob = DynamoUtils.generateItem(
                "job-id",
                "466",
                DynamoColumn("timestamp", "2019-11-04T11:24:12", "string"),
                DynamoColumn("active", true, "boolean"),
                DynamoColumn("recordCount", 434323242, "long"),
                DynamoColumn("year", 2019, "int")

            )

            val table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)

            DynamoUtils.write(successJob, table)
            DynamoUtils.write(failedJob, table)

        }
    )

    "DynamoDB Table" should "allow items to be read attributes by name" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "123"
            )

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val item = DynamoUtils.read(getItemSpec, table)
            if (item.isSuccess) {
                assert(item.get.getString("timestamp") != "")
            }
            else {
                println("Could not find an item...")
                intercept[Exception](item.get.getBoolean("uncensored"))
            }


        }
    )
    it should "allow items to be read as json" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "466")

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val item = DynamoUtils.readAsJson(getItemSpec, table)
            if (item.isSuccess) {
                println(item.get)
                assert(item.get != "")
            }
            else {
                println(item.failed.get.printStackTrace())
                intercept[Exception](item.get)
            }
        }
    )
    "DynamoDB Table" should "allow items to read a column as string" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "466")

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val ts = DynamoUtils.getString(getItemSpec, "timestamp", table).get
            assert(ts == "2019-11-04T11:24:12")

        }
    )

    it should "allow items to read a column as long" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "466")

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val rc = DynamoUtils.getLong(getItemSpec, "recordCount", table).get
            assert(rc == 434323242)

        })

    it should "allow items to read a column as int" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "466")

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val year = DynamoUtils.getInt(getItemSpec, "year", table).get
            assert(year == 2019)

        })

    it should "allow items to read a column as boolean" in withDynamoClient(
        "localhost", 8000, dynamoClient => {
            val getItemSpec = DynamoUtils.createGetItemSpec(
                "job-id",
                "466")

            val tableName: String = "job-status"
            val table: Table = DynamoUtils.getDynamoTable(tableName)(dynamoClient)
            val active = DynamoUtils.getBoolean(getItemSpec, "active", table).get
            assert(active)

        }
    )
}
