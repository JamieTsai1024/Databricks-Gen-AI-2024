# Databricks notebook source
# MAGIC %pip install databricks-genai-inference
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks_genai_inference import ChatSession

chat = ChatSession(
    model="databricks-dbrx-instruct", system_message="You are a helpful assistant."
)


# COMMAND ----------

chat.reply("What is the relationship between Delta Lake and Parquet?")

# COMMAND ----------

print(chat.last)

# COMMAND ----------

chat.history

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vector Search 

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-vectorsearch databricks-genai-inference llama-index llama-index-readers-web
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient 
vsc = VectorSearchClient()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, FloatType 

spark.sql("CREATE SCHEMA IF NOT EXISTS databricks_generative_ai_world_cup.netflix") # shared.hackathon_demo")
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS databricks_generative_ai_world_cup.netflix.source_table (
        id STRING,
        text STRING 
    )
    USING delta 
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
)

# COMMAND ----------

index=vsc.create_delta_sync_index(
    endpoint_name="netflix_rag",
    index_name="databricks_generative_ai_world_cup.netflix.vs_index",
    source_table_name="databricks_generative_ai_world_cup.netflix.source_table",
    pipeline_type="CONTINUOUS",
    primary_key="id",
    embedding_source_column="text",
    embedding_model_endpoint_name="databricks-bge-large-en"
)

# COMMAND ----------

index = vsc.get_index(
    endpoint_name="netflix_rag",
    index_name="databricks_generative_ai_world_cup.netflix.vs_index"
)

# COMMAND ----------

# # Remove all data from source table 
# %sql 
# truncate table databricks_generative_ai_world_cup.netflix.source_table;

# COMMAND ----------

urls=[
  "https://help.netflix.com/en/node/412",
  "https://help.netflix.com/en/node/22",
  "https://help.netflix.com/en/node/407",
  "https://help.netflix.com/en/node/116380",
  "https://help.netflix.com/en/node/244",
  "https://help.netflix.com/en/node/123277",
  "https://help.netflix.com/en/node/13243",
  "https://help.netflix.com/en/node/41049",
  "https://help.netflix.com/en/node/32950",
  "https://help.netflix.com/en/node/113408",
  "https://help.netflix.com/en/node/54896",
  "https://help.netflix.com/en/node/59095",
  "https://help.netflix.com/en/node/29",
  "https://help.netflix.com/en/node/34",
  "https://help.netflix.com/en/node/2065",
  "https://help.netflix.com/en/node/1019",
  "https://help.netflix.com/en/node/124410",
  "https://help.netflix.com/en/node/14424",
  "https://help.netflix.com/en/node/12232",
  "https://help.netflix.com/en/node/59985",
  "https://help.netflix.com/en/node/45117",
  "https://help.netflix.com/en/node/95",
  "https://help.netflix.com/en/node/11634",
  "https://help.netflix.com/en/node/100242",
  "https://help.netflix.com/en/node/100400",
  "https://help.netflix.com/en/node/470",
  "https://help.netflix.com/en/node/14384",
  "https://help.netflix.com/en/node/127429",
  "https://help.netflix.com/en/node/57688",
  "https://help.netflix.com/en/node/2090",
  "https://help.netflix.com/en/node/11634",
  "https://help.netflix.com/en/node/10421",
  "https://help.netflix.com/en/node/13245",
  "https://help.netflix.com/en/node/122698",
  "https://help.netflix.com/en/node/24853",
  "https://help.netflix.com/en/node/13444",
  "https://help.netflix.com/en/node/113300",
  "https://help.netflix.com/en/node/10523",
  "https://help.netflix.com/en/node/116022",
  "https://help.netflix.com/en/contactus",
  "https://help.netflix.com/en/node/54816",
  "https://help.netflix.com/en/node/47765",
  "https://help.netflix.com/en/node/22205",
  "https://help.netflix.com/en/node/115312",
  "https://help.netflix.com/en/node/264",
  "https://help.netflix.com/en/node/114277",
  "https://help.netflix.com/en/node/114276",
  "https://help.netflix.com/en/node/114275",
  "https://help.netflix.com/en/node/121442",
  "https://help.netflix.com/en/node/121924",
  "https://help.netflix.com/en/node/412",
  "https://help.netflix.com/en/node/102377",
  "https://help.netflix.com/en/node/24926",
  "https://help.netflix.com/en/node/123279",
  "https://help.netflix.com/en/node/30081",
  "https://help.netflix.com/en/node/101653",
  "https://help.netflix.com/en/node/23939",
  "https://help.netflix.com/en/node/33222",
  "https://help.netflix.com/en/node/113160",
  "https://help.netflix.com/en/titlerequest?ui_action=title-suggestion-quicklinks",
  "https://help.netflix.com/en/node/14422/CA",
  "https://help.netflix.com/en/node/2064",
  "https://help.netflix.com/en/node/60541",
  "https://help.netflix.com/en/node/47765",
  "https://help.netflix.com/en/node/115312",
  "https://help.netflix.com/en/node/24853",
  "https://help.netflix.com/en/node/129273",
  "https://help.netflix.com/en/node/128339",
  "https://help.netflix.com/en/node/54816",
  "https://help.netflix.com/en/node/124418",
  "https://help.netflix.com/en/node/62990",
  "https://help.netflix.com/en/node/68489",
  "https://help.netflix.com/en/node/68771",
  "https://help.netflix.com/en/node/110084",
  "https://help.netflix.com/en/node/365",
  "https://help.netflix.com/en/node/25970",
  "https://www.netflix.com/ca/",
  "https://help.netflix.com/en/node/14361",
  "https://help.netflix.com/en/node/23934",
  "https://help.netflix.com/en/node/23887",
  "https://help.netflix.com/en/node/23924",
  "https://help.netflix.com/en/node/23904",
  "https://help.netflix.com/en/node/23876",
  "https://help.netflix.com/en/node/23878",
  "https://help.netflix.com/en/node/23932",
  "https://help.netflix.com/en/node/30081",
  "https://help.netflix.com/en/node/23931",
  "https://help.netflix.com/en/node/23888"]

# COMMAND ----------

from llama_index.readers.web import SimpleWebPageReader
from llama_index.core.node_parser import SentenceSplitter

# llamaindex tools for getting and splitting text
reader = SimpleWebPageReader(html_to_text=True)
parser = SentenceSplitter.from_defaults() 

# text chunks for splitting 
chunks = parser.get_nodes_from_documents(reader.load_data(urls))

# the schema of our source delta table 
schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
    ]
)

# initialize an empty DataFrame with the defined schema 
df = spark.createDataFrame([], schema)

# iterate through the list of chunks 
for chunk in chunks: 
    chunk = chunk.dict()
    chunk_id = chunk["id_"]
    chunk_text = chunk["text"]

    new_row = spark.createDataFrame([(chunk_id, chunk_text)], schema)
    df = df.union(new_row)

df.write.format("delta").mode("append").saveAsTable("databricks_generative_ai_world_cup.netflix.source_table")

# COMMAND ----------

# Query
x = index.similarity_search(
    columns=["text"],
    query_text="Represent this sentence for searching relevant passages: What are the different methods for creating a vector search endpoint?",
    num_results=3
)

# COMMAND ----------

x["result"]
# x["result"]["data_array"][0][0] # uncomment once we have the data scraped  

# COMMAND ----------

# MAGIC %md
# MAGIC # Vector Search and Foundation Model 
# MAGIC

# COMMAND ----------

from databricks_genai_inference import ChatSession, Embedding
from databricks.vector_search.client import VectorSearchClient 

class RAG:
  def __init__(
    self, 
    model="databricks-dbrx-instruct",
    system_message="You are a helpful assistant. Answer the user's question. If context is provided, you must answer based on this context.",
    max_tokens=2048,
    index_name="databricks_generative_ai_world_cup.netflix.vs_index",
    endpoint="netflix_rag",
  ):
    self.chat_session = ChatSession(
      model=model,
      system_message=system_message,
      max_tokens=max_tokens,
    )
    self.vsc = VectorSearchClient(disable_notice=True)
    self.endpoint = endpoint
    self.index_name = index_name 
  
  def query_index(self, query, num_results=3):
    """
    Queries the vector search index to retrieve relevant passages based on the given query.
    Returns the concatenated text of the retrieved passages.
    """
    index = self.vsc.get_index(
      endpoint_name=self.endpoint,
      index_name=self.index_name,
    )

    query_response = index.similarity_search(
      query_text="Represent this sentence for searching relevant passages: " + query,
      columns=["text"],
      num_results=num_results,
    )

    results = query_response["result"]["data_array"]
    concatenated_text = ""

    for i, result in enumerate(results):
      concatenated_text += result[0]
      if i < len(results) - 1: 
        concatenated_text += "\n---\n"
    
    return concatenated_text

  def query_rag(self, user_input):
    """
    Performs Retrieval-Augmented Generation (RAG) using the provided user input 
    Retrieves relevant context from the index and generates a response using the chat model.
    """
    ctx_chunks = self.query_index(user_input)
    ctx = ("Answer the question baed on the provided context. Context: \n\n" + ctx_chunks + "\n\n")
    self.chat_session.reply(ctx + "\n\n" + user_input)
    bot_response = self.chat_session.last.strip() 
    return bot_response

  def clear_chat(self): 
    """
    Clears the chat session history 
    """
    self.chat_session.history.clear() 

# COMMAND ----------

rag = RAG() 

# COMMAND ----------

print(rag.query_rag("What are the different ways I can create a vector search endpoint?"))

# COMMAND ----------

print(rag.query_rag("What is the difference between a delta sync index and a direct vector access index?"))

# COMMAND ----------

rag.clear_chat() 

# COMMAND ----------

print(rag.query_rag("Give me Python code to create a new continous sync vector index called MY_INDEX on a delta table MY_TABLE and endpoint MY_ENDPOINT using the Python sdk. Use the databricks-bge-large-en embeddings model."))
