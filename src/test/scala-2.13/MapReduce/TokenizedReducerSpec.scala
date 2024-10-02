package MapReduce

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.scalatest.funsuite.AnyFunSuite
import scala.jdk.CollectionConverters._

class TokenizedReducerSpec extends AnyFunSuite {

  test("TokenizedReducer should correctly reduce token IDs and count occurrences") {
    val reducer = new TokenizedReducer()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Reducer[Text, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    // Simulating input: key = "test" and values = ["[1,2] 3", "[2] 1"]
    val key = new Text("test")
    val values = List(new Text("[1,2] 3"), new Text("[2] 1")).asJava

    reducer.reduce(key, values, context)

    // Verify the output
    assert(output.contains((key, new Text("[1, 2] 4")))) // Total count for token IDs 1 and 2 should be 4
  }

  test("TokenizedReducer should handle empty values gracefully") {
    val reducer = new TokenizedReducer()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Reducer[Text, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    // Simulating input: key = "empty" and no values
    val key = new Text("empty")
    val values = List.empty[Text].asJava

    reducer.reduce(key, values, context)

    // Check that the output is empty as no values were provided
    assert(output.isEmpty)
  }

  test("TokenizedReducer should skip invalid token IDs") {
    val reducer = new TokenizedReducer()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Reducer[Text, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    // Simulating input with an invalid token ID: key = "invalid" and values = ["[invalid,2] 3"]
    val key = new Text("invalid")
    val values = List(new Text("[invalid,2] 3")).asJava

    reducer.reduce(key, values, context)

    // Check that no output is produced due to invalid token ID
    assert(output.isEmpty)
  }
}
