import pyspark.sql.types as st
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession, SQLContext
from datetime import datetime

# Spark Session app
spark = SparkSession \
  .builder \
  .appName('creditas-exploration') \
  .getOrCreate()

# Create a SQL Context
sc = spark.sparkContext


# Directory origem with all the pre-processing datas.
path_local_container = "/home/jovyan/work/datasets/"

# Added local diretory with all files transient.
path_google_ads = path_local_container+"google_ads_media_costs.jsonl"
path_facebook_ads = path_local_container+"facebook_ads_media_costs.jsonl"
path_pageviews = path_local_container+"pageview.txt"
path_clf = path_local_container+"customer_leads_funnel.csv"

# Added local diretory Raw
path_raw_google = path_local_container+"raw/prd_lake_google_ads"
path_raw_facebook = path_local_container+"raw/prd_lake_facebook_ads"
path_raw_pageviews = path_local_container+"raw/prd_lake_pageviews"
path_raw_clf = path_local_container+"raw/prd_lake_clf"

# Added local diretory Refined
path_refined_most_expensive_campaign = path_local_container+"refined/most expensive campaign"
path_refined_most_profitable_campaign = path_local_container+"refined/most profitable campaign"
path_refined_most_effective_clicks = path_local_container+"refined/most effective_clicks"
path_refined_most_effective_leads = path_local_container+"refined/most effective_leads"


## Google Json Read
# Extract schema
SCHEMA = spark.read.json(path_google_ads).schema

# Load json.
df_read_json_google = spark \
    .read.json(path_google_ads, SCHEMA) \
    .withColumn('date', sf.date_format('date','yyyy-MM-dd')) \
    .withColumn('integration_date', sf.lit(datetime.utcnow())) \
    .withColumn('clicks', sf.col('clicks').cast('integer')) \
    .withColumn('cost', sf.col('cost').cast('decimal'))


# Save Dataframe in parquet.
df_read_json_google.write \
    .mode('overwrite') \
    .parquet(path_raw_google)


## Facebook Json Read
# Extract schema
SCHEMA = spark.read.json(path_facebook_ads).schema

# Load json.
df_read_json_face = spark \
    .read.json(path_facebook_ads, SCHEMA) \
    .withColumn('date', sf.date_format('date','yyyy-MM-dd')) \
    .withColumn('integration_date', sf.lit(datetime.utcnow())) \
    .withColumn('clicks', sf.col('clicks').cast('integer')) \
    .withColumn('cost', sf.col('cost').cast('decimal'))


# Save Dataframe in parquet.
df_read_json_face.write \
    .mode('overwrite') \
    .parquet(path_raw_facebook)


## Pageviews Txt Read
# Load json.
SCHEMA_PAGE = st.StructType([
    st.StructField('endpoint', st.StringType(), True),
    st.StructField('device', st.StringType(), True),
    st.StructField('referer', st.StringType(), True),
])

df_read_txt_page = spark.read \
    .option('delimiter','|') \
    .csv(path_pageviews, SCHEMA_PAGE)

# Find in endpoint column spliting when word "ad_creative_id" and "campaign_id"
split_col = sf.split(df_read_txt_page['endpoint'], 'ad_creative_id')
split_col1 = sf.split(df_read_txt_page['endpoint'], 'campaign_id')


# device Column - I do the substring for the recover only cod and not the word "device_id: g7DDoCqp9V"
# referer Column - I do the substring for the recover only refers and not the word "referer: http://www.facebook.com"
# ad_creative_id Column - I do the split colunm ex:"|20001&campaign_id=1003 |" and then use substring
#                         for unwanted characters and maintains only "20001"
df_read_txt_page_intermediate = df_read_txt_page \
    .withColumn('device', sf.trim(sf.substring('device', 12, 30))) \
    .withColumn('referer', sf.trim(sf.substring('referer', 11, 50))) \
    .withColumn('ad_creative_id', sf.trim(sf.substring(split_col.getItem(1), 2, 5))) \
    .withColumn('campaign_id', sf.trim(sf.substring(split_col1.getItem(1), 2, 6))) \
    .withColumn('integration_date', sf.lit(datetime.utcnow()))

# Remove endpoint Column
df_read_txt_page_final = df_read_txt_page_intermediate.drop('endpoint')

# Save Dataframe in parquet.
df_read_txt_page_final.write \
    .mode('overwrite') \
    .parquet(path_raw_pageviews)


## CLF (Customer Lead Funnel)
# Extract schema
SCHEMA_CLF = st.StructType([
    st.StructField('device_id', st.StringType(), True),
    st.StructField('lead_id', st.LongType(), True),
    st.StructField('registered_at', st.TimestampType(), True),
    st.StructField('credit_decision', st.StringType(), True),
    st.StructField('credit_decision_at', st.TimestampType(), True),
    st.StructField('signed_at', st.TimestampType(), True),
    st.StructField('revenue', st.DecimalType(15, 2), True), ])

df_read_csv_clf = spark.read \
    .option('delimiter',',') \
    .csv(path_clf, SCHEMA_CLF) \
    .withColumn('registered_at', sf.date_format('registered_at','yyyy-MM-dd HH:mm:ss')) \
    .withColumn('credit_decision_at', sf.date_format('credit_decision_at','yyyy-MM-dd HH:mm:ss')) \
    .withColumn('signed_at', sf.date_format('signed_at','yyyy-MM-dd HH:mm:ss')) \
    .withColumn('integration_date', sf.lit(datetime.utcnow()))

# Save Dataframe in parquet.
df_read_csv_clf.write \
    .mode('overwrite') \
    .parquet(path_raw_clf)


## Save DataFrame in MySql
MYSQL_USERNAME = "creditas_test";
MYSQL_PWD = "password";
MYSQL_CONNECTION_URL = "jdbc:mysql://mysql_murillo:3306/creditas";
DRIVER ='com.mysql.jdbc.Driver';

# Save Google ADS in Mysql
df_read_json_google.write.format('jdbc').options(
      url=MYSQL_CONNECTION_URL,
      driver=DRIVER,
      dbtable='google_ads',
      user=MYSQL_USERNAME,
      password=MYSQL_PWD).mode('overwrite').save()

# Save Facebook ADS in Mysql
df_read_json_face.write.format('jdbc').options(
      url=MYSQL_CONNECTION_URL,
      driver=DRIVER,
      dbtable='facebook_ads',
      user=MYSQL_USERNAME,
      password=MYSQL_PWD).mode('overwrite').save()

# Save Pageviews in Mysql
df_read_txt_page_final.write.format('jdbc').options(
      url=MYSQL_CONNECTION_URL,
      driver=DRIVER,
      dbtable='pageviews',
      user=MYSQL_USERNAME,
      password=MYSQL_PWD).mode('overwrite').save()

# Save CLF(Customer Lead Funnel) in Mysql
df_read_csv_clf.write.format('jdbc').options(
      url=MYSQL_CONNECTION_URL,
      driver=DRIVER,
      dbtable='customer_lead_funnef',
      user=MYSQL_USERNAME,
      password=MYSQL_PWD).mode('overwrite').save()

df_read_clf = spark.read.parquet(path_raw_clf)
df_read_pageview = spark.read.parquet(path_raw_pageviews)
df_read_google = spark.read.parquet(path_raw_google)
df_read_face = spark.read.parquet(path_raw_facebook)

df_read_clf.createOrReplaceTempView('customer_lead_funnel')
df_read_pageview.createOrReplaceTempView('pageviews')
df_read_google.createOrReplaceTempView('google_ads')
df_read_face.createOrReplaceTempView('facebook_ads')

# It most expensive descending, the most and then less
most_expensive = spark.sql(
"""
select result.type_ads, 
       result.campaigns_name,
       sum(result.cost) as total_cost 
from (
        select 'Google' type_ads,
               a.ad_creative_name campaigns_name, 
               a.cost  
          from google_ads a
          union all
        select 'Facebook' type_ads,
               b.facebook_campaign_name, 
               b.cost 
          from facebook_ads b) result
group by result.type_ads,
         result.campaigns_name
order by 3 desc
""")

union_campaigns = spark.sql(
"""
select 'Google Campaigns' type_ads,
       go.google_campaign_id campaigns_id,
       go.google_campaign_name campaigns_name,
       go.cost,
       go.impressions
  from google_ads go
union all
select 'Facebook Campaigns' type,
       fa.facebook_campaign_id,
       fa.facebook_campaign_name,
       fa.cost,
       fa.impressions
  from facebook_ads fa
""").createOrReplaceTempView('union_campaigns')

# It most profitable descending, the most and then less and impressions quantity
most_profitable = spark.sql(
"""
select camp.type_ads,
       camp.campaigns_name,
       sum(camp.impressions) impressions
  from customer_lead_funnel clf,
       pageviews page,
       union_campaigns camp
 where clf.credit_decision = 'A'
   and clf.device_id = page.device
   and page.campaign_id = camp.campaigns_id
group by camp.type_ads,
         camp.campaigns_name
order by 3 desc

""")

# It most effective descending, the less costs and most clicks
most_effective_clicks = spark.sql(
"""
select go.ad_creative_id,
       go.ad_creative_name,
       sum(go.clicks) clicks,
       sum(go.cost) costs
  from pageviews page,
       google_ads go
 where page.ad_creative_id = go.ad_creative_id
group by go.ad_creative_id,
         go.ad_creative_name
order by 4 asc
""")

# It most effective in generating leads is occur registration and high number of impressions
most_effective_leads = spark.sql(
"""

select go.ad_creative_id,
       go.ad_creative_name,
       sum(go.impressions) impressions
  from pageviews page,
       google_ads go,
       customer_lead_funnel clf
 where page.ad_creative_id = go.ad_creative_id
   and page.device = clf.device_id
   and clf.signed_at is not null
group by go.ad_creative_id,
         go.ad_creative_name
order by 3 desc
""")

path_refined_most_expensive_campaign = path_local_container+"refined/most expensive campaign"
path_refined_most_profitable_campaign = path_local_container+"refined/most profitable campaign"
path_refined_most_effective_clicks = path_local_container+"refined/most effective_clicks"
path_refined_most_effective_leads = path_local_container+"refined/most effective_leads"

# Saves Answers in refined diretory.
most_expensive.write \
    .mode('overwrite') \
    .parquet(path_refined_most_expensive_campaign)

most_profitable.write \
    .mode('overwrite') \
    .parquet(path_refined_most_profitable_campaign)

most_effective_clicks.write \
    .mode('overwrite') \
    .parquet(path_refined_most_effective_clicks)

most_effective_leads.write \
    .mode('overwrite') \
    .parquet(path_refined_most_effective_leads)
