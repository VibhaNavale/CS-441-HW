# CS441 Fall2024 - HW1
## Vibha Navale
#### UIN: 676301415
#### NetID: vnava22@uic.edu

Repo for the MapReduce Homework-1 for CS441 Fall2024

Project walkthrough:
https://uic.zoom.us/rec/share/A_Ju21cAOss27KLXYy-IIhE1G6nZ2X-e2LpetvAgevxUFftBa5ZkzhLDZgBKwlMw.cl0F9ftwpF9gAUi0

## Environment:
**OS** : macOS (M3 Chip)

---

## Prerequisites:
- SBT (1.10.2) and SBT Assembly (2.2.0)
- Hadoop Version 3.3.6
- Java 11
- Scala 2.13.15
- Download the IMDB dataset of 50K movie reviews from Kaggle (https://www.kaggle.com/datasets/lakshmi25npathi/imdb-dataset-of-50k-movie-reviews)
- Download the English Model File (.bin): Locate the section for the English language in FastText Crawl Vectors website (https://fasttext.cc/docs/en/crawl-vectors.html) and download the appropriate model file (typically named something like cc.en.300.vec or cc.en.300.bin). 
  - This file contains pre-trained word vectors that can enhance the performance of the FastText model. This can be stored in the _src/main/resources_ folder, or uploaded on S3.
---

## Running the project
1) Download the repo from git
2) The root file is found in _src/main/scala/app/MainApp.scala_
3) Run `sbt clean update` and `sbt clean compile` from the terminal.
4) Run `sbt "run app.MainApp"` without any parameters or on hadoop `hadoop jar target/scala-2.13/CS-441-HW-assembly-0.1.jar`
5) Run `sbt test` to test
6) To create the jar file, run the command `sbt clean assembly`
7) The resulting jar file can be found at _target/scala-2.13/CS-441-HW-assembly-0.1.jar_

- Make sure that your local input/output folder has the requisite permissions to allow the program to read and write to it
- Make sure Hadoop is running on your machine before you run the program. Start Hadoop using `start-dfs.sh`

---

### Parameters
1. Sample input path - ```input_data/datasets/IMDB.csv```
2. Sample output path - ```output_data/tokenizer_output```

---

## Requirements:

In this homework, the focus is on utilizing a Map/Reduce (M/R) framework in a cloud environment to create and train a Large Language Model (LLM).
1) Select a manageable open-source text corpus, such as OpenWebText2, WikiText, Project Gutenberg, etc.
2) Split the text corpus into shards for parallel processing and tokenize the text using Byte Pair Encoding (BPE) with JTokkit.
3) Compute token embeddings using JFastText (or DL4J).
4) Calculate the cosine similarities of pair-wise embeddings.

Other Requirements:

1) Logging used for all programs 
2) Configurable input and output paths for the program 
3) Compilable through sbt 
4) Deployed on AWS EMR

---

## Technical Design

We will take a look at the detailed description of how each of these pieces of code work below. Line by line comments explaining every step are also added to the source code in this git repo:

1) ### [MainApp](src/main/scala/app/MainApp.scala) [App]
    This is the main method that we call to run our jobs. It takes our input and output path. Our input is the .csv dataset of IMDB reviews (downloaded from Kaggle), and output path of the first job would be tokenized output files.
- The main method runs all jobs sequentially: Tokenizer, Word2Vec, and Cosine Similarity.
- Takes input path from the config file (inputPath) and specifies output paths for each stage (tokenized output, Word2Vec, Cosine Similarity).
- Job Flow:
   - Tokenize text using TokenizedDriver.
   - Run Word2Vec on tokenized output using Word2VecDriver.
   - Calculate cosine similarities using CosineSimilarityDriver.
- Logs each stage and handles errors if any output is missing or jobs fail.

2) ### [TokenizedDriver](src/main/scala/MapReduce/TokenizedDriver.scala) [MapReduce]
    Handles the setup and execution of the tokenization job, specifying the mapper and reducer classes, input/output paths, and logging the results.
- Sets up a Hadoop job for tokenization.
- Loads config, sets mapper/reducer classes, and handles input/output paths.
- Executes the job, logs results, and handles errors (missing paths or job failure).

3) ### [TokenizedMapper](src/main/scala/MapReduce/TokenizedMapper.scala) [MapReduce]
   Processes input text by cleaning and tokenizing it, then emits tokens with their associated IDs and occurrence counts.
- Cleans input by removing unwanted characters, handles punctuation, and normalizes spaces.
- Uses encode() to tokenize the input text.
- Outputs word tokens with their corresponding token IDs and counts.
4) ### [TokenizedReducer](src/main/scala/MapReduce/TokenizedReducer.scala) [MapReduce]
    Aggregates token IDs from mapper outputs, calculates frequencies, and emits the final token list with total counts.
- Processes token counts from the mapper output.
- Aggregates unique token IDs and sums their frequencies.
- Outputs the final result in the format `word [token IDs] total-frequency.`
- Example Output:

`  deploy	[279, 323, 814, 2085, 5334, 19307] 6
  deployment	[279, 439, 574, 1202, 1618, 2683, 5097, 6342, 21095, 34102, 59862] 12
  depolarize	[4131] 1
  depopulated	[31691] 1
  deportation	[374, 539, 649, 893, 994, 12278, 22890, 25337] 8
  deported	[18, 220, 279, 505, 3277, 5030, 5429, 24785, 68210] 9`

5) ### [Word2VecDriver](src/main/scala/MapReduce/Word2VecDriver.scala) [MapReduce]
    Manages the Hadoop job for Word2Vec processing, sets up the mapper and reducer, handles input/output paths, and ensures the job runs successfully.
- Loads configuration paths for input (tokenizer output) and output (Word2Vec result).
- Initializes Hadoop job and sets up the mapper and reducer.
- Deletes existing output directory if it exists.
- Executes the job and logs success or failure.

6) ### [Word2VecMapper](src/main/scala/MapReduce/Word2VecMapper.scala) [MapReduce]
    Processes each token from the tokenized output and retrieves its vector representation using FastText, truncating to 5 dimensions.
- Initializes and loads the FastText model.
- Cleans tokens and retrieves vectors for valid tokens.
- Uses zero vectors for tokens without a FastText vector.
- Outputs token and vector pairs (without brackets around the token ID).
7) ### [Word2VecReducer](src/main/scala/MapReduce/Word2VecReducer.scala) [MapReduce]
    Aggregates vectors from the mapper and computes the mean vector for each token, handling vector parsing errors gracefully.
- Collects and parses vectors for each token.
- Calculates the mean vector across all vectors for each token.
- Uses zero vectors as fallback for errors or missing vectors.
- Writes the final token and mean vector pair (without brackets around token ID).

- Example Output:

    `trick 719	-0.16966873407363892,-0.08998167514801025,0.07056315243244171,0.15282049775123596,0.10216228663921356
    tricked 12	-0.07420065253973007,-0.05793951451778412,0.13693535327911377,0.07574641704559326,0.1255442500114441
    trickedout 315	-0.043632615357637405,0.08213238418102264,-0.02888466604053974,-0.03074292093515396,0.03337191790342331`

8) ### [CosineSimilarityDriver](src/main/scala/MapReduce/CosineSimilarityDriver.scala) [MapReduce]
    Coordinates the execution of the MapReduce job to compute cosine similarity between word embeddings, including setting up paths, configurations, and launching the job.
- Initializes the job configuration.
- Sets input/output paths and checks for existing output.
- Defines mapper and reducer classes.
- Executes the job and checks for success.

9) ### [CosineSimilarityMapper](src/main/scala/MapReduce/CosineSimilarityMapper.scala) [MapReduce]
    Processes input to extract tokens and their embeddings, emitting them as key-value pairs to be processed by the reducer.
- Extracts tokens and embeddings from input.
- Emits token and embedding as key-value pairs.

10) ### [CosineSimilarityReducer](src/main/scala/MapReduce/CosineSimilarityReducer.scala) [MapReduce]
    Gathers word embeddings, computes cosine similarity between each pair, and classifies their similarity levels.
- Collects embeddings for tokens.
- Computes cosine similarity between token pairs.
- Classifies token pairs as "Very Similar," "Similar," or "Dissimilar."
- Example:
`  trick trick 1.1153 Very Similar
  trick tricked 1.1153 Very Similar
  trickedout trickery 0.5936 Dissimilar
  trickier trickiest 0.5363 Dissimilar
  trick trick 1.1153 Very Similar
  trickled trickles 0.5936 Dissimilar`

---

## Test Cases
These are run through the command `sbt test`

---

