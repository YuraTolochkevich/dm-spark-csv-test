# Mapper Test

## Building

You'll need JDK 8, sbt 0.13.x

Compile :

        sbt compile

## Running 
 You can run your application locally with `sbt run`:
 Input arguments:

```
  -p, --path <value>       Input CSV file path
  -c, --column-update    A comma separated list columns to transfrom:  existingColName|name|newColName|newDataType,existingColName|name|newColName|newDataType|dateExpression


```

##Example

    sbt " run -p  /tmp/Sample.csv -c  name|firstName|string,age|new_age|integer,birthday|data_of_b|date|dd-MM-yyyy"
