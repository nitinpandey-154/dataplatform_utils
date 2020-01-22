package com.goibibo.dp.utils

import java.util

import scala.util._
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClientBuilder}
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, Table}
import com.amazonaws.services.dynamodbv2.model._

object DynamoUtils {

    /**
      * Create AmazonDynamoDB client which is used to perform operations over AWS DynamoDb
      *
      * @param region Parameter of type String containing region name
      * @return AmazonDynamoDB client instance
      */
    def createDynamoClient(region: String): AmazonDynamoDB = {
        val dynamoEndpoint = s"https://dynamodb.$region.amazonaws.com"
        AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(dynamoEndpoint, region)).build()
    }

    /**
      * Create AmazonDynamoDB client which is used to perform operations over AWS DynamoDb
      *
      * @param region         Parameter of type String containing region name
      * @param dynamoEndpoint DynamoDB end point. Example - https://dynamodb.ap-south-1.amazonaws.com
      * @return AmazonDynamoDB client instance
      */
    def createDynamoClient(region: String, dynamoEndpoint: String): AmazonDynamoDB = {
        AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
            new AwsClientBuilder.EndpointConfiguration(dynamoEndpoint, region)).build()
    }

    /**
      * Tests if dynamoDB connection is valid..
      *
      * @param client AmazonDynamoDB client instance
      * @return True or False
      */
    def testDynamoClient(client: AmazonDynamoDB): Boolean = {
        Try {
            client.listTables()
        }.isSuccess
    }

    /**
      * Returns a dynamodb compatible datatype (ScalarAttributeType) for scala/java primitive datatypes
      *
      * @param dataType Datatype - Long, Int, Bigint, Number, String, Binary
      * @return ScalarAttributeType
      */
    def getDynamoDBType(dataType: String): ScalarAttributeType = {
        if (dataType.toLowerCase.trim == "binary")
            ScalarAttributeType.B
        else if (Seq("integer", "int", "long", "bigint", "number").contains(dataType.toLowerCase.trim))
            ScalarAttributeType.N
        else
            ScalarAttributeType.S

    }

    /**
      * Creates a dynamoDB table based on the table specifications provided..
      *
      * @param tableName                  The name of the dynamoDB table.
      * @param partitionKeyName           Name of the dynamoDB partitionKey
      * @param partitionKeyType           DataType of dynamoDB partitionKey
      * @param sortKeyName                Name of the dynamoDB sortkey
      * @param sortKeyType                DataType of dynamoDB sortKey
      * @param readProvisionedThroughput  Read Capacity in long
      * @param writeProvisionedThroughput Write Capacity in long
      * @param client                     Implicit client of type AmazonDynamoDB
      * @return None
      */
    def createTable(tableName: String, partitionKeyName: String, partitionKeyType: String,
                    sortKeyName: String, sortKeyType: String,
                    readProvisionedThroughput: Long, writeProvisionedThroughput: Long)(
                       implicit client: AmazonDynamoDB): Unit = {

        val dynamoPartitionKeyType = getDynamoDBType(partitionKeyType)
        val dynamoSortKeyType = getDynamoDBType(sortKeyType)

        System.out.println("Attempting to create table; please wait...")
        val table: Table = new DynamoDB(client).createTable(
            tableName,
            java.util.Arrays.asList(
                new KeySchemaElement(
                    partitionKeyName,
                    KeyType.HASH
                ), // Partition Key
                new KeySchemaElement(
                    sortKeyName,
                    KeyType.RANGE) // Sort key
            ),
            util.Arrays.asList(
                new AttributeDefinition(
                    partitionKeyName,
                    dynamoPartitionKeyType
                ),
                new AttributeDefinition(sortKeyName,
                    dynamoSortKeyType
                )),
            new ProvisionedThroughput(
                readProvisionedThroughput,
                writeProvisionedThroughput)
        )
        table.waitForActive
        System.out.println("Success.  Table status: " + table.getDescription.getTableStatus)


    }


    /**
      * Creates a dynamoDB table based on the table specifications provided..
      *
      * @param tableName                  The name of the dynamoDB table.
      * @param partitionKeyName           Name of the dynamoDB partitionKey
      * @param partitionKeyType           DataType of dynamoDB partitionKey
      * @param readProvisionedThroughput  Read Capacity in long
      * @param writeProvisionedThroughput Write Capacity in long
      * @param client                     Implicit client of type AmazonDynamoDB
      * @return None
      */
    def createTable(tableName: String, partitionKeyName: String, partitionKeyType: String,
                    readProvisionedThroughput: Long, writeProvisionedThroughput: Long)(
                       implicit client: AmazonDynamoDB): Unit = {

        val dynamoPartitionKeyType = getDynamoDBType(partitionKeyType)

        System.out.println("Attempting to create table; please wait...")
        val table: Table = new DynamoDB(client).createTable(
            tableName,
            java.util.Arrays.asList(
                new KeySchemaElement(
                    partitionKeyName,
                    KeyType.HASH
                ) // Partition Key

            ),
            util.Arrays.asList(
                new AttributeDefinition(
                    partitionKeyName,
                    dynamoPartitionKeyType
                )
            ),
            new ProvisionedThroughput(
                readProvisionedThroughput,
                writeProvisionedThroughput)
        )
        table.waitForActive
        System.out.println("Success.  Table status: " + table.getDescription.getTableStatus)

    }

    /**
      * Creates a dynamoDB table based on the table specifications provided..
      *
      * @param dynamoTableSpecs DynamoDB Table Specs of type DynamoDBTableSpecs
      * @param client           Implicit client of type AmazonDynamoDB
      * @return None
      */
    def createTable(dynamoTableSpecs: DynamoDBTableSpecs)(implicit client: AmazonDynamoDB): Unit = {


        val tableName = dynamoTableSpecs.tableName
        val partitionKeyName = dynamoTableSpecs.partitionKeyName
        val partitionKeyType = dynamoTableSpecs.partitionKeyType
        val sortKeySpecs = dynamoTableSpecs.sortKeySpecs
        val provisionedThroughput = dynamoTableSpecs.provisionedThroughput

        if (sortKeySpecs.isDefined && provisionedThroughput.isDefined) {
            // SORT KEY AND PROVISIONED THROUGHPUT SPECIFIED
            createTable(
                tableName,
                partitionKeyName,
                partitionKeyType,
                sortKeySpecs.get.sortKeyName,
                sortKeySpecs.get.sortKeyType,
                provisionedThroughput.get.readCapacity,
                provisionedThroughput.get.writeCapacity
            )
        } else if (sortKeySpecs.isDefined) {
            // SORT KEY SPECIFIED
            createTable(
                tableName,
                partitionKeyName,
                partitionKeyType,
                sortKeySpecs.get.sortKeyName,
                sortKeySpecs.get.sortKeyType,
                10L,
                10L
            )

        }
        else if (provisionedThroughput.isDefined) {
            // Provisione throughput specified
            createTable(
                tableName,
                partitionKeyName,
                partitionKeyType,
                provisionedThroughput.get.readCapacity,
                provisionedThroughput.get.writeCapacity
            )

        }
        else {
            createTable(
                tableName,
                partitionKeyName,
                partitionKeyType,
                10L,
                10L
            )

        }

    }

    /**
      * List all the dynamoDB tables
      *
      * @param client AmazonDynamoDB client instance of type implicit
      * @return List of tables
      */
    def listTables()(implicit client: AmazonDynamoDB): util.List[String] = {
        client.listTables().getTableNames
    }

    /**
      * Checks for the presence of a dynamodb table
      *
      * @param tableName Dynamodb table name
      * @param client    AmazonDynamoDB client instance of type implicit
      * @return Boolean flag indicating whether the table is present or not
      */
    def checkTableExists(tableName: String)(implicit client: AmazonDynamoDB): Boolean = {
        client.listTables().getTableNames.contains(tableName)
    }

    /**
      * Get dynamodb table instance
      *
      * @param tableName Dynamodb table name
      * @param client    AmazonDynamoDB client instance of type implicit
      * @return Dynamodb table instance
      */
    def getDynamoTable(tableName: String)(implicit client: AmazonDynamoDB): Table = {
        if (checkTableExists(tableName)) {
            new DynamoDB(client).getTable(tableName)
        }
        else throw new TableNotFoundException(s"The dynamoDB $tableName table does not exist..")

    }

    /**
      * Generates an element of type DynamoDB Item
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @return Dynamodb Item instance with the given specs
      */
    def generateItem[A](partitionKeyName: String, partitionKeyValue: String): Item = {
        new Item().withPrimaryKey(partitionKeyName, partitionKeyValue)
    }

    /**
      * Generates an element of type DynamoDB Item
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @return Dynamodb Item instance with the given specs
      */
    def generateItem[A](partitionKeyName: String, partitionKeyValue: String, columns: DynamoColumn*): Item = {
        enrichItem(new Item().withPrimaryKey(partitionKeyName, partitionKeyValue), columns: _*)
    }

    /**
      * Generates an element of type DynamoDB Item
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @param sortKeyName       Name of the sort key column in dynamodb table
      * @param sortKeyValue      SortKey Column Value
      * @return Dynamodb Item instance with the given specs
      */
    def generateItem[A](partitionKeyName: String, partitionKeyValue: String, sortKeyName: String, sortKeyValue: A): Item = {
        new Item().withPrimaryKey(partitionKeyName, partitionKeyValue, sortKeyName, sortKeyValue)
    }

    /**
      * Generates an element of type DynamoDB Item
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @param sortKeyName       Name of the sort key column in dynamodb table
      * @param sortKeyValue      SortKey Column Value
      * @param columns           Sequence of DynamoColumns
      * @return Dynamodb Item instance with the given specs
      */
    def generateItem[A](partitionKeyName: String, partitionKeyValue: String, sortKeyName: String, sortKeyValue: A,
                        columns: DynamoColumn*): Item = {
        enrichItem(new Item().withPrimaryKey(partitionKeyName, partitionKeyValue, sortKeyName, sortKeyValue), columns: _*)
    }

    /**
      * Enrich dynamodb item instance with additional column details
      *
      * @param partialItem DynamoDB Item instance having partitionKey (and sortKey) details
      * @param columns     Sequence of type DynamoRecordField[A](fieldName: String, fieldDataType: Any, fieldValue: A)
      * @return DynamoDB Item enriched with additional columns
      */
    def enrichItem(partialItem: Item, columns: DynamoColumn*): Item = {
        var item = partialItem
        columns.foreach(column => {
            item = column.dataType.trim.toLowerCase() match {
                case "boolean" | "bool" => item.withBoolean(column.name, column.value.toString.toBoolean)
                case "int" | "integer" | "number" => item.withInt(column.name, column.value.toString.toInt)
                case "long" => item.withLong(column.name, column.value.toString.toLong)
                case "string" => item.withString(column.name, column.value.toString)
                case _ => throw new Exception(s"Invalid Field Type for !!")
            }
        })
        item

    }

    /**
      * Writes a given
      *
      * @param item  DynamoDB item instance
      * @param table Implicit parameter - Dynamodb Table instance
      * @return Success(_) if write succeeds else Failure(t:Throwable) in case of a failure
      */

    def write(item: Item, table: Table): Try[PutItemResult] = {
        Try {
            table.putItem(item).getPutItemResult

        }
    }

    /**
      * Creates a dynamoDB GetItemSpec with partitionKey, sortKey and columnNames to fetch
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @param sortKeyName       Name of the sort key column in dynamodb table
      * @param sortKeyValue      SortKey Column Value
      * @param attributes        Sequence of columns which are to be fetched
      * @return GetItemSpec AmazonDynamoDB Spec
      */
    def createGetItemSpec(partitionKeyName: String, partitionKeyValue: String, sortKeyName: String,
                          sortKeyValue: Int, attributes: String*): GetItemSpec = {
        new GetItemSpec().withPrimaryKey(partitionKeyName, partitionKeyValue,
            sortKeyName, sortKeyValue).withAttributesToGet(attributes: _*)
    }

    /**
      * Creates a dynamoDB GetItemSpec with partitionKey, sortKey and columnNames to fetch
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @param attributes        Sequence of columns which are to be fetched
      * @return GetItemSpec AmazonDynamoDB Spec
      */
    def createGetItemSpec(partitionKeyName: String, partitionKeyValue: String, attributes: String*): GetItemSpec = {
        new GetItemSpec().withPrimaryKey(partitionKeyName, partitionKeyValue).withAttributesToGet(attributes: _*)
    }

    /**
      * Creates a dynamoDB GetItemSpec with partitionKey, sortKey and columnNames to fetch
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @param sortKeyName       Name of the sort key column in dynamodb table
      * @param sortKeyValue      SortKey Column Value
      * @return GetItemSpec AmazonDynamoDB Spec
      */
    def createGetItemSpec(partitionKeyName: String, partitionKeyValue: String, sortKeyName: String,
                          sortKeyValue: Int): GetItemSpec = {
        new GetItemSpec().withPrimaryKey(partitionKeyName, partitionKeyValue,
            sortKeyName, sortKeyValue)
    }

    /**
      * Creates a dynamoDB GetItemSpec with partitionKey, sortKey and columnNames to fetch
      *
      * @param partitionKeyName  Partition Key Name in dynamodb table
      * @param partitionKeyValue Partition Value for the record
      * @return GetItemSpec AmazonDynamoDB Spec
      */
    def createGetItemSpec(partitionKeyName: String, partitionKeyValue: String): GetItemSpec = {
        new GetItemSpec().withPrimaryKey(partitionKeyName, partitionKeyValue)
    }


    /**
      * Reads a dynamodDB item based on the specs provided
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param table       Dynamodb Table instance
      * @return Success(Item) if succeeds else Failure(throwable)
      */
    def read(getItemSpec: GetItemSpec, table: Table): Try[Item] = {
        Try {
            table.getItem(getItemSpec)
        }
    }

    /**
      * Reads a dynamodDB record as JSON based on the specs provided
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param table       Dynamodb Table instance
      * @return Success(JSON) if succeeds else Failure(throwable)
      */
    def readAsJson(getItemSpec: GetItemSpec, table: Table): Try[String] = {
        Try {
            table.getItem(getItemSpec).toJSON
        }
    }

    /**
      * Reads a dynamodDB item field as String
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param columnName  Name of the column
      * @param table       DynamoDB Table instance
      * @return Success(value:String) if succeeds else Failure(throwable)
      */
    def getString(getItemSpec: GetItemSpec, columnName: String, table: Table): Try[String] = {
        Try {
            val item: Item = table.getItem(getItemSpec)
            item.getString(columnName)
        }

    }

    /**
      * Reads a dynamodDB item field as Long
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param columnName  Name of the column
      * @param table       DynamoDB Table instance
      * @return Success(value:Long) if succeeds else Failure(throwable)
      */
    def getLong(getItemSpec: GetItemSpec, columnName: String, table: Table): Try[Long] = {
        Try {
            val item: Item = table.getItem(getItemSpec)
            item.getLong(columnName)
        }
    }

    /**
      * Reads a dynamodDB item field as Int
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param columnName  Name of the column
      * @param table       DynamoDB Table instance
      * @return Success(value:Int) if succeeds else Failure(throwable)
      */
    def getInt(getItemSpec: GetItemSpec, columnName: String, table: Table): Try[Int] = {
        Try {
            val item: Item = table.getItem(getItemSpec)
            item.getInt(columnName)
        }
    }

    /**
      * Reads a dynamodDB item field as Boolean
      *
      * @param getItemSpec DynamoDB GetItemSpec having details about the record
      * @param columnName  Name of the column
      * @param table       Implicit parameter - DynamoDB Table instance
      * @return Success(value:Boolean) if succeeds else Failure(throwable)
      */
    def getBoolean(getItemSpec: GetItemSpec, columnName: String, table: Table): Try[Boolean] = {
        Try {
            val item: Item = table.getItem(getItemSpec)
            item.getBoolean(columnName)
        }
    }

}


