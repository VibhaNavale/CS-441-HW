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

class CosineSimilarityTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  // Variables for Mapper and Reducer context and the instances
  var mockMapperContext: Mapper[LongWritable, Text, Text, Text]#Context = _
  var mockReducerContext: Reducer[Text, Text, Text, Text]#Context = _
  var cosineSimilarityMapper: CosineSimilarityMapper = _
  var cosineSimilarityReducer: CosineSimilarityReducer = _

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

    cosineSimilarityMapper = new CosineSimilarityMapper()
    cosineSimilarityReducer = new CosineSimilarityReducer()

    // Set up any additional configurations if needed
    when(mockMapperContext.getConfiguration).thenReturn(new org.apache.hadoop.conf.Configuration())
  }

  "CosineSimilarityMapper" should "emit token and its embedding correctly" in {
    val inputKey = new LongWritable(0)
    val inputValue = new Text("trick\t-0.16966873407363892,-0.08998167514801025,0.07056315243244171,0.15282049775123596,0.10216228663921356")

    // Call the map function
    cosineSimilarityMapper.map(inputKey, inputValue, mockMapperContext)

    // Verify that the context wrote the expected output
    verify(mockMapperContext, atLeastOnce()).write(new Text("trick"), new Text("-0.16966873407363892,-0.08998167514801025,0.07056315243244171,0.15282049775123596,0.10216228663921356"))
  }

  it should "skip malformed input" in {
    val inputKey = new LongWritable(0)
    val inputValue = new Text("malformedInput") // This should be ignored
    cosineSimilarityMapper.map(inputKey, inputValue, mockMapperContext)

    // Verify that no output was written
    verify(mockMapperContext, never()).write(any[Text], any[Text])
  }

  "CosineSimilarityReducer" should "compute cosine similarities and emit results" in {
    val inputKey = new Text("trick")
    val inputValues = List(
      new Text("-0.16966873407363892,-0.08998167514801025,0.07056315243244171,0.15282049775123596,0.10216228663921356"),
      new Text("-0.07420065253973007,-0.05793951451778412,0.13693535327911377,0.07574641704559326,0.1255442500114441")
    ).asJava

    cosineSimilarityReducer.reduce(inputKey, inputValues, mockReducerContext)

    // Verify that the context wrote the cosine similarity result
    verify(mockReducerContext, atLeastOnce()).write(new Text("trick trick"), new Text("1.1153 Very Similar"))
    verify(mockReducerContext, atLeastOnce()).write(new Text("trick tricked"), new Text("1.1153 Very Similar"))
  }

  it should "handle embeddings gracefully and emit classification" in {
    val inputKey1 = new Text("trick")
    val inputValues1 = List(new Text("-0.16966873407363892,-0.08998167514801025,0.07056315243244171")).asJava
    cosineSimilarityReducer.reduce(inputKey1, inputValues1, mockReducerContext)

    val inputKey2 = new Text("tricked")
    val inputValues2 = List(new Text("-0.07420065253973007,-0.05793951451778412,0.13693535327911377")).asJava
    cosineSimilarityReducer.reduce(inputKey2, inputValues2, mockReducerContext)

    // Emit results
    verify(mockReducerContext, atLeastOnce()).write(new Text("trick trick"), any[Text])
    verify(mockReducerContext, atLeastOnce()).write(new Text("trick tricked"), any[Text])
  }

  it should "not emit results if only one embedding is present" in {
    val inputKey = new Text("singleWord")
    val inputValues = List(new Text("1.0,2.0,3.0")).asJava

    cosineSimilarityReducer.reduce(inputKey, inputValues, mockReducerContext)

    // Verify that no output was written as we need at least two embeddings to compute similarity
    verify(mockReducerContext, never()).write(any[Text], any[Text])
  }

  override def afterEach(): Unit = {
    // Clean up if necessary
  }
}
