package MapReduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar

import scala.jdk.CollectionConverters._

class TokenizedTest extends AnyFlatSpec with Matchers with MockitoSugar {

  // Test 1: Input with punctuation
  "TokenizedMapper" should "remove punctuation and tokenize correctly" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("Hello, world! This is a test.")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("Hello"), new Text("[12345] 1"))
    verify(context).write(new Text("world"), new Text("[67890] 1"))
    verify(context).write(new Text("This"), new Text("[11111] 1"))
    verify(context).write(new Text("is"), new Text("[22222] 1"))
    verify(context).write(new Text("a"), new Text("[33333] 1"))
    verify(context).write(new Text("test"), new Text("[44444] 1"))
  }

  // Test 2: Input with emojis or special characters
  "TokenizedMapper" should "remove emojis and special characters" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("Hello 😊 world 🌍!")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("Hello"), new Text("[12345] 1"))
    verify(context).write(new Text("world"), new Text("[67890] 1"))
  }

  // Test 3: Check word frequency count
  "TokenizedMapper" should "correctly count word frequencies" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("repeat repeat test")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("repeat"), new Text("[12345] 2"))
    verify(context).write(new Text("test"), new Text("[44444] 1"))
  }

  // Test 4: Tokenization consistency check
  "TokenizedMapper" should "generate consistent token IDs" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("token test consistency")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("token"), new Text("[55555] 1"))
    verify(context).write(new Text("test"), new Text("[44444] 1"))
    verify(context).write(new Text("consistency"), new Text("[99999] 1"))
  }

  // Test 5: Reducer functionality for merging token counts
  "TokenizedReducer" should "merge token counts and return unique token IDs" in {
    val reducer = new TokenizedReducer
    val context = mock[Reducer[Text, Text, Text, Text]#Context]

    val key = new Text("test")
    val values = List(new Text("[12345] 2"), new Text("[12345,67890] 1")).asJava

    reducer.reduce(key, values, context)

    verify(context).write(new Text("test"), new Text("[12345, 67890] 3"))
  }
}
