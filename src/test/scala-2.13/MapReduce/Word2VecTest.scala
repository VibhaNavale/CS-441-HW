package MapReduce

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.mockito.{MockedConstruction, Mockito}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import scala.jdk.CollectionConverters._

class Word2VecTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Variables for Mapper and Reducer context and the instances
  var mockMapperContext: Mapper[LongWritable, Text, Text, Text]#Context = _
  var mockReducerContext: Reducer[Text, Text, Text, Text]#Context = _
  var word2VecMapper: Word2VecMapper = _
  var word2VecReducer: Word2VecReducer = _

  override def beforeEach(): Unit = {
    // Create new instances for each test
    // Mock the Mapper and Reducer context using Mockito
    val mockMapperConstruction: MockedConstruction[Mapper[LongWritable, Text, Text, Text]] =
      Mockito.mockConstructionWithAnswer(classOf[Mapper[LongWritable, Text, Text, Text]],
        invocation => {
          // This allows you to define how the mocked context behaves
          mockMapperContext = invocation.getArgument(0).asInstanceOf[Mapper[LongWritable, Text, Text, Text]#Context]
          mockMapperContext
        })

    val mockReducerConstruction: MockedConstruction[Reducer[Text, Text, Text, Text]] =
      Mockito.mockConstructionWithAnswer(classOf[Reducer[Text, Text, Text, Text]],
        invocation => {
          // This allows you to define how the mocked context behaves
          mockReducerContext = invocation.getArgument(0).asInstanceOf[Reducer[Text, Text, Text, Text]#Context]
          mockReducerContext
        })

    word2VecMapper = new Word2VecMapper()
    word2VecReducer = new Word2VecReducer()

    // Set up model loading in the mapper
    when(mockMapperContext.getConfiguration).thenReturn(new org.apache.hadoop.conf.Configuration())
    word2VecMapper.setup(mockMapperContext)
  }

  "Word2VecMapper" should "map input tokens to vectors correctly" in {
    val inputKey = new LongWritable(0)
    val inputValue = new Text("deposited\t[311, 584, 775, 779, 902, 1855, 1944, 23011, 60794] 10")

    // Call the map function
    word2VecMapper.map(inputKey, inputValue, mockMapperContext)

    // Verify that the context wrote the expected output
    verify(mockMapperContext, atLeastOnce()).write(new Text("deposited 10"), any[Text])
  }

  it should "handle lines with insufficient parts gracefully" in {
    val inputKey = new LongWritable(0)
    val inputValue = new Text("insufficient") // This should be ignored
    word2VecMapper.map(inputKey, inputValue, mockMapperContext)

    // Verify that no output was written
    verify(mockMapperContext, never()).write(any[Text], any[Text])
  }

  "Word2VecReducer" should "calculate mean vectors correctly" in {
    val inputKey = new Text("deposited 10")
    val inputValues = List(
      new Text("0.1,0.2,0.3,0.4,0.5"),
      new Text("0.2,0.3,0.4,0.5,0.6")
    ).asJava

    word2VecReducer.reduce(inputKey, inputValues, mockReducerContext)

    // Verify that the context wrote the mean vector output
    verify(mockReducerContext, atLeastOnce()).write(new Text("deposited 10"), new Text("0.15,0.25,0.35,0.45,0.55"))
  }

  it should "handle empty vector lists gracefully" in {
    val inputKey = new Text("deppwho 1")
    val inputValues = List.empty[Text].asJava

    word2VecReducer.reduce(inputKey, inputValues, mockReducerContext)

    // Verify that a zero vector output was written
    verify(mockReducerContext, atLeastOnce()).write(new Text("deppwho 1"), new Text("0.0,0.0,0.0,0.0,0.0"))
  }
}
