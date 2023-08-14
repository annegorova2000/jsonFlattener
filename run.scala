val inputFileName : String = "/home/ann-egorova2000/sber/ПреобразованиеJson/input/huge-file.json";
val outputFileName : String = "/home/ann-egorova2000/sber/ПреобразованиеJson/output/huge-file.parquet";

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ValueNode

import scala.collection.JavaConverters._;	
import scala.collection.JavaConversions._;
import scala.Tuple3;

import org.apache.spark.sql.functions._;
import org.apache.spark.sql.expressions.UserDefinedFunction;


object JsonFlattener {
  //output = Struct{path, key, value}
  def flattenJson(inputJson: String): java.util.List[Tuple3[String, String, String]] = {
    val outArray = new java.util.ArrayList[Tuple3[String, String, String]]
    if (inputJson == null || inputJson.isEmpty || inputJson.equalsIgnoreCase("null") || inputJson.equalsIgnoreCase("none") || inputJson.equalsIgnoreCase("nan")) {
      outArray.add(new Tuple3[String, String, String]("/", "/", "null"))
      return outArray
    }
    val objectMapper = new ObjectMapper
    try {
      val jsonNode = objectMapper.readTree(inputJson)
      addKeys(outArray, jsonNode, "", "")
    } catch {
      case e: Exception =>
        outArray.add(new Tuple3[String, String, String]("", "parsing_error_invalid_json", e.getMessage))
        return outArray
    }
    outArray
  }

  private def addKeys(outArray: java.util.List[Tuple3[String, String, String]], jsonNode: JsonNode, path: String, key: String): Unit = {
    if (jsonNode.isArray) parseArray(outArray, jsonNode.asInstanceOf[ArrayNode], path, key)
    else if (jsonNode.isObject) parseObject(outArray, jsonNode.asInstanceOf[ObjectNode], path, key)
    else parseValue(outArray, jsonNode.asInstanceOf[ValueNode], path, key)
  }

  private def buildPath(path: String, key: String): String = {
    var newPath: String = null
    if (path.isEmpty) return key
    if (!key.isEmpty && key.charAt(0) == '[') newPath = path + key
    else newPath = path + "/" + key
    newPath
  }

  private def parseObject(outArray: java.util.List[Tuple3[String, String, String]], objectNode: ObjectNode, path: String, key: String): Unit = {
    val newPath = buildPath(path, key)
    if (objectNode.size == 0) outArray.add(new Tuple3[String, String, String](newPath, key, "{}"))
    val iterator = objectNode.fields
    while (iterator.hasNext) {
      val entry = iterator.next
      val entryKey = entry.getKey
      addKeys(outArray, entry.getValue, newPath, entryKey)
    }
  }

  private def parseArray(outArray: java.util.List[Tuple3[String, String, String]], arrayNode: ArrayNode, path: String, key: String): Unit = {
    val newPath = buildPath(path, key)
    if (arrayNode.size == 0) {
      outArray.add(new Tuple3[String, String, String](newPath, key, "[]"))
      return
    }
    for (i <- 0 until arrayNode.size) {
      addKeys(outArray, arrayNode.get(i), newPath, "[" + i + "]")
    }
  }

  private def parseValue(outArray: java.util.List[Tuple3[String, String, String]], valueNode: ValueNode, path: String, key: String): Unit = {
    val newPath = buildPath(path, key)
    if (valueNode.isFloatingPointNumber) outArray.add(new Tuple3[String, String, String](newPath, key, String.valueOf(valueNode.asDouble)))
    else if (valueNode.isBoolean) outArray.add(new Tuple3[String, String, String](newPath, key, String.valueOf(valueNode.asBoolean)))
    else if (valueNode.isIntegralNumber) outArray.add(new Tuple3[String, String, String](newPath, key, String.valueOf(valueNode.asLong)))
    else {
      val text = valueNode.asText
      outArray.add(new Tuple3[String, String, String](newPath, key, if (text == null || text.trim.isEmpty) "null"
      else text.trim))
    }
  }
}

val jsonToKeyValueUdf : UserDefinedFunction = udf((inputJson: String) => {
  val javaList : java.util.List[Tuple3[String, String, String]] = JsonFlattener.flattenJson(inputJson);
  val scalaList : scala.List[Tuple3[String, String, String]] = javaList.asScala.toList;
  scalaList
});


val jsonDf = spark.read.option("wholetext", true).text(inputFileName).selectExpr("value as json");
//val jsonText : String = jsonDf.first().getString(0);

val jsonParsed = jsonDf.withColumn("resultStruct", explode(jsonToKeyValueUdf(col("json")))).drop("json").selectExpr("resultStruct._1 as path", "resultStruct._2 as key", "resultStruct._3 as value");
jsonParsed.cache();

jsonParsed.write.mode("overwrite").parquet(outputFileName);

jsonParsed.show(10,false);