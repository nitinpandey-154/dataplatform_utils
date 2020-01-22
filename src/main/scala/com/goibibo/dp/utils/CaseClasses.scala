package com.goibibo.dp.utils

case class DynamoDBSortKeySpecs(
                                   sortKeyName: String,
                                   sortKeyType: String
                               )

case class DynamoDBProvisionedThroughput(
                                            readCapacity: Long,
                                            writeCapacity: Long
                                        )


case class DynamoDBTableSpecs(
                                 tableName: String,
                                 partitionKeyName: String,
                                 partitionKeyType: String,
                                 sortKeySpecs: Option[DynamoDBSortKeySpecs] = None,
                                 provisionedThroughput: Option[DynamoDBProvisionedThroughput] = None
                             )

case class DynamoColumn(name: String, value: Any, dataType: String)

case class Field(fieldName: String, fieldDataType: String, precision: Int, scale: Int,
                 isNullable: Option[Boolean] = None, autoIncrement: Option[Boolean] = None)

case class ConnectionSpecs(
                              drivername: String,
                              standard: String, //JDBC OR ODBC
                              dialect: String, // mysql, postgresql
                              hostname: String,
                              port: Int,
                              databasename: String,
                              username: String,
                              password: String
                          ) {
    def getConnectionUrl: String = {
        standard + ":" + dialect + "://" + hostname + ":" + port + "/" + databasename + "?user=" + username + "&password=" + password
    }

}