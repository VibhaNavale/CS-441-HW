package MapReduce

import org.apache.hadoop.io.{Text, LongWritable}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.mapreduce.Mapper

class TokenizedMapperSpec extends AnyFunSuite {

  test("TokenizedMapper should process a simple line correctly") {
    val mapper = new TokenizedMapper()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Mapper[LongWritable, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    mapper.map(new LongWritable(1), new Text("Hello world"), context)

    assert(output.contains((new Text("Hello"), new Text("[0] 1"))))  // Adjust token IDs based on your logic
    assert(output.contains((new Text("world"), new Text("[1] 1"))))  // Adjust token IDs based on your logic
  }

  test("TokenizedMapper should handle multiple spaces correctly") {
    val mapper = new TokenizedMapper()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Mapper[LongWritable, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    mapper.map(new LongWritable(1), new Text("Hello     world"), context)

    assert(output.contains((new Text("Hello"), new Text("[0] 1"))))  // Adjust token IDs based on your logic
    assert(output.contains((new Text("world"), new Text("[1] 1"))))  // Adjust token IDs based on your logic
  }

  test("TokenizedMapper should remove unwanted characters") {
    val mapper = new TokenizedMapper()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Mapper[LongWritable, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    mapper.map(new LongWritable(1), new Text("Hello 😊 world"), context)

    assert(output.contains((new Text("Hello"), new Text("[0] 1"))))  // Adjust token IDs based on your logic
    assert(output.contains((new Text("world"), new Text("[1] 1"))))  // Adjust token IDs based on your logic
  }

  test("TokenizedMapper should trim leading and trailing whitespace") {
    val mapper = new TokenizedMapper()
    val output = scala.collection.mutable.ListBuffer[(Text, Text)]()

    val context = new Mapper[LongWritable, Text, Text, Text]#Context {
      override def write(key: Text, value: Text): Unit = {
        output += ((key, value))
      }
    }

    mapper.map(new LongWritable(1), new Text("   Hello world   "), context)

    assert(output.contains((new Text("Hello"), new Text("[0] 1"))))  // Adjust token IDs based on your logic
    assert(output.contains((new Text("world"), new Text("[1] 1"))))  // Adjust token IDs based on your logic
  }
}
