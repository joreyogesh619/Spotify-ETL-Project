# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import os
import sys

project_path=os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

from utils.transformations import reusable


# COMMAND ----------

# MAGIC %md
# MAGIC **DimUser**

# COMMAND ----------

df=spark.read.format("parquet").load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **AutoLoading**

# COMMAND ----------

# DBTITLE 1,DimUser reading stream
df_user=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimUser/checkpoint")\
                .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
              .load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimUser")
display(df_user)

# COMMAND ----------

df_user=df_user.withColumn("user_name",upper(col("user_name")))
display(df_user)

# COMMAND ----------

df_user_obj=reusable()

df_user=df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user=df_user.dropDuplicates(['user_id'])
display(df_user)


# COMMAND ----------

# DBTITLE 1,writing data
df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimUser/checkpoint")\
        .trigger(once=True)\
            .option("path","<container_name>@<storageAccountName>.dfs.core.windows.net/DimUser/data")\
                .toTable("spotify_catalog.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC **DimArtist**

# COMMAND ----------

# DBTITLE 1,Reading DimArtist stream
df_artist=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimArtist/checkpoint")\
                .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
              .load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimArtist")
display(df_artist)

# COMMAND ----------

trans_obj=reusable()
df_artist=trans_obj.dropColumns(df_artist,['_rescued_data'])
df_artist=df_artist.dropDuplicates(['artist_id'])
display(df_artist)

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimArtist/checkpoint")\
        .trigger(once=True)\
            .option("path","<container_name>@<storageAccountName>.dfs.core.windows.net/DimArtist/data")\
                .toTable("spotify_catalog.silver.DimArtist")

# COMMAND ----------

# MAGIC %md
# MAGIC **DimTrack**

# COMMAND ----------

# DBTITLE 1,DimTrack reading
df_track=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimTrack/checkpoint")\
                .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
              .load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimTrack")
display(df_track)

# COMMAND ----------

trans_obj=reusable()
df_track=trans_obj.dropColumns(df_track,['_rescued_data'])
df_track=df_track.dropDuplicates(['track_id'])
display(df_track)

# COMMAND ----------

df_track=df_track.withColumn("durationFlag",when(col("duration_sec")<150,"low")\
                                            .when((col("duration_sec")>150) & (col("duration_sec")<300),"medium")\
                                            .otherwise("high")
    )

df_track=df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))
display(df_track)

# COMMAND ----------

# DBTITLE 1,writing DimTrack
df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimTrack/checkpoint")\
        .trigger(once=True)\
            .option("path","<container_name>@<storageAccountName>.dfs.core.windows.net/DimTrack/data")\
                .toTable("spotify_catalog.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC **DimDate**

# COMMAND ----------

df_date=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimDate/checkpoint")\
                .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
              .load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/DimDate")
display(df_date)

# COMMAND ----------

df_date=reusable().dropColumns(df_date,['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/DimDate/checkpoint")\
        .trigger(once=True)\
            .option("path","<container_name>@<storageAccountName>.dfs.core.windows.net/DimDate/data")\
                .toTable("spotify_catalog.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC **Fact stream**

# COMMAND ----------

# DBTITLE 1,Reading fact stream
df_factStream=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/FactStream/checkpoint")\
                .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
              .load("abfss://<container_name>@<storageAccountName>.dfs.core.windows.net/FactStream")
display(df_factStream)

# COMMAND ----------

# DBTITLE 1,writing fact stream
df_factStream=reusable().dropColumns(df_factStream,['_rescued_data'])

df_factStream.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation","<container_name>@<storageAccountName>.dfs.core.windows.net/FactStream/checkpoint")\
        .trigger(once=True)\
            .option("path","<container_name>@<storageAccountName>.dfs.core.windows.net/FactStream/data")\
                .toTable("spotify_catalog.silver.FactStream")