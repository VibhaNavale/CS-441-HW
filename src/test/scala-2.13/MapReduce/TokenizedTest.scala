package MapReduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.mockito.ArgumentMatchers.any
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
    verify(context).write(new Text("Hello"), new Text("[9906] 1"))
    verify(context).write(new Text("world"), new Text("[1917] 1"))
    verify(context).write(new Text("This"), new Text("[1115] 1"))
    verify(context).write(new Text("is"), new Text("[374] 1"))
    verify(context).write(new Text("a"), new Text("[264] 1"))
    verify(context).write(new Text("test"), new Text("[1296] 1"))
  }

  // Test 2: Input with emojis or special characters
  "TokenizedMapper" should "remove emojis and special characters" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("Hello 😊 world 🌍!")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("Hello"), new Text("[9906] 1"))
    verify(context).write(new Text("world"), new Text("[1917] 1"))
  }

  // Test 3: Handling multiple spaces
  "TokenizedMapper" should "correctly handle multiple spaces between words" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("Hello   world     This is   a test.")

    mapper.map(new LongWritable(1), inputText, context)

    // Specify expected token IDs based on your tokenization logic
    verify(context).write(new Text("Hello"), new Text("[9906] 1"))
    verify(context).write(new Text("world"), new Text("[1917] 1"))
    verify(context).write(new Text("This"), new Text("[1115] 1"))
    verify(context).write(new Text("is"), new Text("[374] 1"))
    verify(context).write(new Text("a"), new Text("[264] 1"))
    verify(context).write(new Text("test"), new Text("[1296] 1"))
  }

  // Test 4: Handling empty input
  "TokenizedMapper" should "handle empty input gracefully" in {
    val mapper = new TokenizedMapper
    val context = mock[Mapper[LongWritable, Text, Text, Text]#Context]
    val inputText = new Text("") // Empty input

    mapper.map(new LongWritable(1), inputText, context)

    // No output should be produced for empty input
    // Since there's no call to context.write, we can verify it wasn't called
    verify(context, never).write(any[Text], any[Text])
  }

  // Test 5: Reducer functionality for merging token counts
  "TokenizedReducer" should "merge token counts and return unique token IDs" in {
    val reducer = new TokenizedReducer
    val context = mock[Reducer[Text, Text, Text, Text]#Context]

    val key = new Text("test")
    val values = List(new Text("[1296] 1"), new Text("[13454] 2")).asJava // Adjusted values for merge

    reducer.reduce(key, values, context)

    verify(context).write(new Text("test"), new Text("[1296, 13454] 3")) // Adjusted total count to reflect merge
  }
}
