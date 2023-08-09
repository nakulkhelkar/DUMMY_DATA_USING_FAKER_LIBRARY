# -*- coding: utf-8 -*-
"""Untitled1.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/19QGpn2wwAOfWev5xz2FR8A8BD2yr1TCt
"""

# Commented out IPython magic to ensure Python compatibility.
# %pip install Faker

pip install findspark

pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from faker import Faker
import random
import re

spark = SparkSession.builder.appName("RandomDataGeneration").getOrCreate()
fake = Faker()

def generate_random_data(col_name):
    if col_name == 'touch_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'touch_name':
        return fake.catch_phrase()
    elif col_name == 'touch_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'touch_start_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'touch_end_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'touch_reply_by_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'offer_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'offer_name':
        return fake.catch_phrase()
    elif col_name == 'keycode_type':
        return fake.word(ext_word_list=['Type1', 'Type2', 'Type3'])
    elif col_name == 'sub_channel_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'sub_channel_name':
        return fake.catch_phrase()
    elif col_name == 'marketing_channel_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'marketing_channel_name':
        return fake.catch_phrase()
    elif col_name == 'creative_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'creative_name':
        return fake.catch_phrase()
    elif col_name == 'variant_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'variant_name':
        return fake.catch_phrase()
    elif col_name == 'variant_desc':
        return fake.sentence(nb_words=6)
    elif col_name == 'keycode_10th_position_byte':
        return fake.random_int(min=1000000000, max=9999999999)
    elif col_name == 'variant_population_split':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'cell_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'cell_name':
        return fake.catch_phrase()
    elif col_name == 'cell_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'selection_group_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'selection_group_name':
        return fake.catch_phrase()
    elif col_name == 'selection_parameters_name':
        return fake.catch_phrase()
    elif col_name == 'tactic_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'tactic_name':
        return fake.catch_phrase()
    elif col_name == 'tactic_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'tactic_start_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'tactic_type_name':
        return fake.word(ext_word_list=['TypeA', 'TypeB', 'TypeC'])
    elif col_name == 'cross_sell_keycode_value':
        return fake.random_int(min=1000000000, max=9999999999)
    elif col_name == 'campaign_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'campaign_name':
        return fake.catch_phrase()
    elif col_name == 'campaign_number':
        return fake.catch_phrase()
    elif col_name == 'campaign_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'campaign_start_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'campaign_end_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'finance_year':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'finance_month':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'campaign_keycode_camp_number':
        return fake.random_int(min=1000000000, max=9999999999)
    elif col_name == 'initiative_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'initiative_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'initiative_start_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'initiative_end_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'marketing_program_id':
        return fake.random_int(min=1, max=10000000000000000)
    elif col_name == 'marketing_program_name':
        return fake.catch_phrase()
    elif col_name == 'marketing_program_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'file_name':
        return fake.file_name(category='text')
    elif col_name == 'load_timestamp':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'campaign_number_new':
        return fake.random_int(min=1, max=10000000000000000)
    else:
        return None

generate_random_data_udf = udf(generate_random_data)

schema = StructType([
    StructField('touch_id', LongType(), False),
    StructField('touch_name', StringType(), True),
    StructField('touch_description', StringType(), True),
    StructField('touch_start_date', TimestampType(), True),
    StructField('touch_end_date', TimestampType(), True),
    StructField('touch_reply_by_date', TimestampType(), True),
    StructField('offer_id', LongType(), True),
    StructField('offer_name', StringType(), True),
    StructField('keycode_type', StringType(), True),
    StructField('sub_channel_id', LongType(), True),
    StructField('sub_channel_name', StringType(), True),
    StructField('marketing_channel_id', LongType(), True),
    StructField('marketing_channel_name', StringType(), True),
    StructField('creative_id', LongType(), True),
    StructField('creative_name', StringType(), True),
    StructField('variant_id', LongType(), True),
    StructField('variant_name', StringType(), True),
    StructField('variant_desc', StringType(), True),
    StructField('keycode_10th_position_byte', StringType(), True),
    StructField('variant_population_split', LongType(), True),
    StructField('cell_id', LongType(), True),
    StructField('cell_name', StringType(), True),
    StructField('cell_description', StringType(), True),
    StructField('selection_group_id', LongType(), True),
    StructField('selection_group_name', StringType(), True),
    StructField('selection_parameters_name', StringType(), True),
    StructField('tactic_id', LongType(), True),
    StructField('tactic_name', StringType(), True),
    StructField('tactic_description', StringType(), True),
    StructField('tactic_start_date', TimestampType(), True),
    StructField('tactic_type_name', StringType(), True),
    StructField('cross_sell_keycode_value', StringType(), True),
    StructField('campaign_id', LongType(), True),
    StructField('campaign_name', StringType(), True),
    StructField('campaign_number', StringType(), True),
    StructField('campaign_description', StringType(), True),
    StructField('campaign_start_date', TimestampType(), True),
    StructField('campaign_end_date', TimestampType(), True),
    StructField('finance_year', LongType(), True),
    StructField('finance_month', LongType(), True),
    StructField('campaign_keycode_camp_number', StringType(), True),
    StructField('initiative_id', LongType(), True),
    StructField('initiative_description', StringType(), True),
    StructField('initiative_start_date', TimestampType(), True),
    StructField('initiative_end_date', TimestampType(), True),
    StructField('marketing_program_id', LongType(), True),
    StructField('marketing_program_name', StringType(), True),
    StructField('marketing_program_description', StringType(), True),
    StructField('file_name', StringType(), True),
    StructField('load_timestamp', TimestampType(), True),
    StructField('campaign_number_new', LongType(), True)
])

df = spark.range(200).withColumn('id', lit(0).cast(IntegerType()))

# Generate random data for each column using the UDF

for col_name in schema.names:
    df = df.withColumn(col_name, generate_random_data_udf(lit(col_name)).cast(schema[col_name].dataType))

# Drop the 'id' column as it was added temporarily for generating rows
df = df.drop("id")

# Show the resulting DataFrame
df.show()

output_dir='/content/drive/MyDrive/Colab Notebooks'
df.coalesce(1).write.parquet(output_dir, mode="overwrite")

