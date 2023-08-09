from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from faker import Faker
import random
import re
spark = SparkSession.builder.appName("RandomDataGeneration").getOrCreate()
fake = Faker()

def generate_random_data(col_name):
    if col_name == 'indiv_key':
        return fake.random_int(min=1, max=100000)
    elif col_name == 'address_key':
        return fake.address().replace('\n', ' ')
    elif col_name == 'email_addr':
        return fake.email()
    elif col_name == 'broadlog_id':
        return fake.uuid4()
    elif col_name == 'event_date':
        return fake.date_time_this_decade().isoformat()
    elif col_name == 'delivery_cd':
        return fake.random_element(elements=('Delivery1', 'Delivery2', 'Delivery3'))
    elif col_name == 'depromotion_date':
        return None
    elif col_name == 'delivery_contact_date':
        return None
    elif col_name == 'status':
        return fake.random_int(min=0, max=2)
    elif col_name == 'status_desc':
        return fake.word(ext_word_list=['Success', 'Failure', 'Pending'])
    elif col_name == 'failure_reason_cd':
        return fake.random_int(min=100, max=999)
    elif col_name == 'failure_reason_desc':
        return fake.sentence(nb_words=6)
    elif col_name == 'folder':
        return fake.word(ext_word_list=['Inbox', 'Promotions', 'Spam'])
    elif col_name == 'channel_id':
        return fake.random_int(min=1, max=10)
    elif col_name == 'failure_text':
        return fake.text(max_nb_chars=500)
    elif col_name == 'campaign_label':
        return fake.catch_phrase()
    elif col_name == 'campaign_name':
        return fake.job()
    elif col_name == 'program_name':
        return fake.company()
    elif col_name == 'plan_name':
        return fake.catch_phrase()
    elif col_name == 'delivery_label':
        return fake.company_suffix()
    elif col_name == 'delivery_name':
        return fake.word(ext_word_list=['DeliveryName1', 'DeliveryName2', 'DeliveryName3'])
    elif col_name == 'touchpoint':
        return fake.random_element(elements=('Email', 'SMS', 'App', 'Web'))
    elif col_name == 'campaign_type':
        return fake.random_element(elements=('Marketing', 'Promotional', 'Transactional'))
    elif col_name == 'seed':
        return fake.random_element(elements=('A', 'B', 'C', 'D'))
    elif col_name == 'segment_code':
        return fake.random_element(elements=('Segment1', 'Segment2', 'Segment3'))
    elif col_name == 'cell_name':
        return fake.random_element(elements=('Cell1', 'Cell2', 'Cell3'))
    elif col_name == 'program_id':
        return fake.random_int(min=1000, max=9999)
    elif col_name == 'initiative_id':
        return fake.random_int(min=2000, max=9999)
    elif col_name == 'creative_id':
        return fake.random_int(min=3000, max=9999)
    elif col_name == 'tactic_id':
        return fake.random_int(min=4000, max=9999)
    elif col_name == 'cell_id':
        return fake.random_int(min=5000, max=9999)
    elif col_name == 'variant_id':
        return fake.random_int(min=6000, max=9999)
    elif col_name == 'wave_id':
        return fake.random_int(min=7000, max=9999)
    elif col_name == 'touch_id':
        return fake.random_int(min=8000, max=9999)
    elif col_name == 'lead_src_cd':
        return fake.random_element(elements=('Source1', 'Source2', 'Source3'))
    elif col_name == 'offer_touch_id':
        return fake.random_int(min=9000, max=9999)
    elif col_name == 'offer_id':
        return fake.random_int(min=10000, max=99999)
    elif col_name == 'plan_cd':
        return fake.random_element(elements=('PlanA', 'PlanB', 'PlanC', 'PlanD'))
    elif col_name == 'product_id':
        return fake.random_int(min=100000, max=999999)
    elif col_name == 'product_name':
        return fake.word(ext_word_list=['Product1', 'Product2', 'Product3'])
    elif col_name == 'digital_cmp_code':
        return fake.random_element(elements=('Digital1', 'Digital2', 'Digital3'))
    elif col_name == 'subject_line':
        return fake.catch_phrase()
    elif col_name == 'transaction_id':
        return fake.uuid4()
    elif col_name == 'first_name':
        return fake.first_name()
    elif col_name == 'middle_name':
        return fake.first_name()
    elif col_name == 'last_name':
        return fake.last_name()
    elif col_name == 'suffix':
        return fake.random_element(elements=('Jr', 'Sr', 'II', 'III', 'IV'))
    elif col_name == 'addr_1':
        return fake.building_number() + ' ' + fake.street_name()
    elif col_name == 'addr_2':
        return fake.secondary_address()
    elif col_name == 'city':
        return fake.city()
    elif col_name == 'state':
        return fake.state_abbr()
    elif col_name == 'zip_cd':
        return fake.zipcode()
    elif col_name == 'zip_4':
        return fake.zipcode_plus4()
    elif col_name == 'date_of_birth':
        return fake.date_of_birth(minimum_age=18, maximum_age=80)
    elif col_name == 'age':
        return fake.random_int(min=18, max=80)
    elif col_name == 'gender':
        return fake.random_element(elements=('M', 'F'))
    elif col_name == 'resident_member_num':
        return fake.random_int(min=1000000000000000, max=9999999999999999)
    elif col_name == 'club_member_customer_id':
        return fake.random_int(min=1000000000000000, max=9999999999999999)
    elif col_name == 'resident_club_cd':
        return fake.random_element(elements=('ABC', 'XYZ', '123'))
    elif col_name == 'member_join_date':
        return None
    elif col_name == 'member_loyalty_years':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_member_term_id':
        return fake.random_int(min=10000, max=99999)
    elif col_name == 'model_member_term_mille':
        return fake.random_int(min=10000, max=99999)
    elif col_name == 'model_member_term_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_member_term_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_member_mlta_id':
        return fake.random_int(min=20000, max=99999)
    elif col_name == 'model_member_mlta_mille':
        return fake.random_int(min=20000, max=99999)
    elif col_name == 'model_member_mlta_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_member_mlta_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_member_giwl_id':
        return fake.random_int(min=10000, max=99999)
    elif col_name == 'model_member_giwl_mille':
        return fake.random_int(min=10000, max=99999)
    elif col_name == 'model_member_giwl_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_member_giwl_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_term_broadmrkt_id':
        return fake.random_int(min=20000, max=99999)
    elif col_name == 'model_term_broadmrkt_mille':
        return fake.random_int(min=20000, max=99999)
    elif col_name == 'model_term_broadmrkt_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_term_broadmrkt_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_giwl_broadmrkt_id':
        return fake.random_int(min=30000, max=99999)
    elif col_name == 'model_giwl_broadmrkt_mille':
        return fake.random_int(min=30000, max=99999)
    elif col_name == 'model_giwl_broadmrkt_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_giwl_broadmrkt_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_member_term_ace_id':
        return fake.random_int(min=40000, max=99999)
    elif col_name == 'model_member_term_ace_mille':
        return fake.random_int(min=40000, max=99999)
    elif col_name == 'model_member_term_ace_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_member_term_ace_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_offer_seg_id':
        return fake.random_int(min=50000, max=99999)
    elif col_name == 'model_offer_seg_mille':
        return fake.random_int(min=50000, max=99999)
    elif col_name == 'model_offer_seg_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_offer_seg_scoring_date':
        return fake.date_this_year()
    elif col_name == 'model_ace_agent_sold_id':
        return fake.random_int(min=60000, max=99999)
    elif col_name == 'model_ace_agent_sold_mille':
        return fake.random_int(min=60000, max=99999)
    elif col_name == 'model_ace_agent_sold_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'model_ace_agent_sold_scoring_date':
        return fake.date_this_year()
    elif col_name == 'indiv_summary_create_date':
        return fake.date_this_year()
    elif col_name == 'keycode':
        return fake.random_int(min=70000, max=99999)
    elif col_name == 'qcode':
        return fake.random_int(min=70000, max=99999)
    elif col_name == 'cell_segment':
        return fake.random_element(elements=('SegmentA', 'SegmentB', 'SegmentC'))
    elif col_name == 'finder_num':
        return fake.random_int(min=80000, max=99999)
    elif col_name == 'serial_id':
        return fake.random_int(min=90000, max=99999)
    elif col_name == 'offer_end_date':
        return fake.date_this_year()
    elif col_name == 'vendor_cd':
        return fake.random_element(elements=('Vendor1', 'Vendor2', 'Vendor3'))
    elif col_name == 'application_form_num':
        return fake.random_int(min=100000, max=999999)
    elif col_name == 'barcode_version':
        return fake.random_int(min=1, max=10)
    elif col_name == 'form_description':
        return fake.sentence(nb_words=6)
    elif col_name == 'creative_notes':
        return fake.text(max_nb_chars=200)
    elif col_name == 'personalization':
        return fake.random_element(elements=('Yes', 'No'))
    elif col_name == 'row_check_value':
        return fake.uuid4()
    elif col_name == 'adobe_addr':
        return fake.address().replace('\n', ' ')
    elif col_name == 'delivery_id':
        return fake.random_int(min=200000, max=999999)
    elif col_name == 'recipient_id':
        return fake.random_int(min=300000, max=999999)
    elif col_name == 'failure_type':
        return fake.random_element(elements=('Type1', 'Type2', 'Type3'))
    elif col_name == 'campaign_id':
        return fake.random_int(min=400000, max=999999)
    elif col_name == 'package_id':
        return fake.random_int(min=500000, max=999999)
    elif col_name == 'milliman_bucket_score':
        return None
    elif col_name == 'hh_milliman_bucket_score':
        return None
    elif col_name == 'create_date':
        return fake.date_this_year()
    elif col_name == 'file_id':
        return fake.random_int(min=600000, max=999999)
    elif col_name == 'record_id':
        return fake.random_int(min=700000, max=999999)
    elif col_name == 'rpt_mail_date':
        return fake.date_this_year()
    elif col_name == 'model_member_term_score':
        return None
    elif col_name == 'model_member_mlta_score':
        return None
    elif col_name == 'model_member_giwl_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'model_term_broadmarket_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'model_giwl_broadmarket_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'model_member_term_ace_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'model_offer_seg_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'model_ace_agent_sold_score':
        return fake.random.uniform(0.1, 0.9)
    elif col_name == 'lexus_nexus_score':
        return None
    elif col_name == 'create_process_log_id':
        return fake.uuid4()
    elif col_name == 'update_process_log_id':
        return fake.uuid4()
    elif col_name == 'orgnl_source':
        return fake.word(ext_word_list=['Source1', 'Source2', 'Source3'])
    elif col_name == 'mail_date':
        return fake.date_this_year()
    elif col_name == 'us_carrier_route':
        return fake.random_element(elements=('Carrier1', 'Carrier2', 'Carrier3'))
    elif col_name == 'club_code':
        return fake.random_element(elements=('ClubA', 'ClubB', 'ClubC'))
    elif col_name == 'update_date':
        return fake.date_this_year()
    elif col_name == 'file_name':
        return fake.file_name(category='text')
    elif col_name == 'load_timestamp':
        return None
    else:
        return 
generate_random_data_udf = udf(generate_random_data)

schema = StructType([
    StructField('indiv_key', LongType(), True),
    StructField('address_key', StringType(), True),
    StructField('email_addr', StringType(), True),
    StructField('broadlog_id', StringType(), True),
    StructField('event_date', TimestampType(), True),
    StructField('delivery_cd', StringType(), True),
    StructField('depromotion_date', TimestampType(), True),
    StructField('delivery_contact_date', TimestampType(), True),
    StructField('status', LongType(), True),
    StructField('status_desc', StringType(), True),
    StructField('failure_reason_cd', LongType(), True),
    StructField('failure_reason_desc', StringType(), True),
    StructField('folder', StringType(), True),
    StructField('channel_id', LongType(), True),
    StructField('failure_text', StringType(), True),
    StructField('campaign_label', StringType(), True),
    StructField('campaign_name', StringType(), True),
    StructField('program_name', StringType(), True),
    StructField('plan_name', StringType(), True),
    StructField('delivery_label', StringType(), True),
    StructField('delivery_name', StringType(), True),
    StructField('touchpoint', StringType(), True),
    StructField('campaign_type', StringType(), True),
    StructField('seed', StringType(), True),
    StructField('segment_code', StringType(), True),
    StructField('cell_name', StringType(), True),
    StructField('program_id', LongType(), True),
    StructField('initiative_id', LongType(), True),
    StructField('creative_id', LongType(), True),
    StructField('tactic_id', LongType(), True),
    StructField('cell_id', LongType(), True),
    StructField('variant_id', LongType(), True),
    StructField('wave_id', LongType(), True),
    StructField('touch_id', LongType(), True),
    StructField('lead_src_cd', StringType(), True),
    StructField('offer_touch_id', LongType(), True),
    StructField('offer_id', LongType(), True),
    StructField('plan_cd', StringType(), True),
    StructField('product_id', LongType(), True),
    StructField('product_name', StringType(), True),
    StructField('digital_cmp_code', StringType(), True),
    StructField('subject_line', StringType(), True),
    StructField('transaction_id', StringType(), True),
    StructField('first_name', StringType(), True),
    StructField('middle_name', StringType(), True),
    StructField('last_name', StringType(), True),
    StructField('suffix', StringType(), True),
    StructField('addr_1', StringType(), True),
    StructField('addr_2', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state', StringType(), True),
    StructField('zip_cd', StringType(), True),
    StructField('zip_4', StringType(), True),
    StructField('date_of_birth', DateType(), True),
    StructField('age', LongType(), True),
    StructField('gender', StringType(), True),
    StructField('resident_member_num', StringType(), True),
    StructField('club_member_customer_id', StringType(), True),
    StructField('resident_club_cd', StringType(), True),
    StructField('member_join_date', DateType(), True),
    StructField('member_loyalty_years', LongType(), True),
    StructField('model_member_term_id', StringType(), True),
    StructField('model_member_term_mille', StringType(), True),
    StructField('model_member_term_version', StringType(), True),
    StructField('model_member_term_scoring_date', StringType(), True),
    StructField('model_member_mlta_id', StringType(), True),
    StructField('model_member_mlta_mille', StringType(), True),
    StructField('model_member_mlta_version', StringType(), True),
    StructField('model_member_mlta_scoring_date', StringType(), True),
    StructField('model_member_giwl_id', StringType(), True),
    StructField('model_member_giwl_mille', StringType(), True),
    StructField('model_member_giwl_version', StringType(), True),
    StructField('model_member_giwl_scoring_date', StringType(), True),
    StructField('model_term_broadmrkt_id', StringType(), True),
    StructField('model_term_broadmrkt_mille', StringType(), True),
    StructField('model_term_broadmrkt_version', StringType(), True),
    StructField('model_term_broadmrkt_scoring_date', StringType(), True),
    StructField('model_giwl_broadmrkt_id', StringType(), True),
    StructField('model_giwl_broadmrkt_mille', StringType(), True),
    StructField('model_giwl_broadmrkt_version', StringType(), True),
    StructField('model_giwl_broadmrkt_scoring_date', StringType(), True),
    StructField('model_member_term_ace_id', StringType(), True),
    StructField('model_member_term_ace_mille', StringType(), True),
    StructField('model_member_term_ace_version', StringType(), True),
    StructField('model_member_term_ace_scoring_date', StringType(), True),
    StructField('model_offer_seg_id', StringType(), True),
    StructField('model_offer_seg_mille', StringType(), True),
    StructField('model_offer_seg_version', StringType(), True),
    StructField('model_offer_seg_scoring_date', StringType(), True),
    StructField('model_ace_agent_sold_id', StringType(), True),
    StructField('model_ace_agent_sold_mille', StringType(), True),
    StructField('model_ace_agent_sold_version', StringType(), True),
    StructField('model_ace_agent_sold_scoring_date', StringType(), True),
    StructField('indiv_summary_create_date', StringType(), True),
    StructField('keycode', StringType(), True),
    StructField('qcode', StringType(), True),
    StructField('cell_segment', StringType(), True),
    StructField('finder_num', StringType(), True),
    StructField('serial_id', LongType(), True),
    StructField('offer_end_date', DateType(), True),
    StructField('vendor_cd', StringType(), True),
    StructField('application_form_num', StringType(), True),
    StructField('barcode_version', StringType(), True),
    StructField('form_description', StringType(), True),
    StructField('creative_notes', StringType(), True),
    StructField('personalization', StringType(), True),
    StructField('row_check_value', StringType(), True),
    StructField('adobe_addr', StringType(), True),
    StructField('delivery_id', StringType(), True),
    StructField('recipient_id', StringType(), True),
    StructField('failure_type', StringType(), True),
    StructField('campaign_id', LongType(), True),
    StructField('package_id', LongType(), True),
    StructField('milliman_bucket_score', DecimalType(9, 0), True),
    StructField('hh_milliman_bucket_score', DecimalType(9, 0), True),
    StructField('create_date', TimestampType(), True),
    StructField('file_id', LongType(), True),
    StructField('record_id', LongType(), True),
    StructField('rpt_mail_date', StringType(), True),
    StructField('model_member_term_score', DecimalType(15, 10), True),
    StructField('model_member_mlta_score', DecimalType(15, 10), True),
    StructField('model_member_giwl_score', DecimalType(15, 10), True),
    StructField('model_term_broadmarket_score', DecimalType(15, 10), True),
    StructField('model_giwl_broadmarket_score', DecimalType(15, 10), True),
    StructField('model_member_term_ace_score', DecimalType(15, 10), True),
    StructField('model_offer_seg_score', DecimalType(15, 10), True),
    StructField('model_ace_agent_sold_score', DecimalType(15, 10), True),
    StructField('lexus_nexus_score', DecimalType(15, 10), True),
    StructField('create_process_log_id', LongType(), True),
    StructField('update_process_log_id', LongType(), True),
    StructField('orgnl_source', StringType(), True),
    StructField('mail_date', DateType(), True),
    StructField('us_carrier_route', StringType(), True),
    StructField('club_code', StringType(), True),
    StructField('update_date', TimestampType(), True),
    StructField('file_name', StringType(), True),
    StructField('load_timestamp', TimestampType(), True)
])
df = spark.range(200).withColumn('id', lit(0).cast(IntegerType()))
# for col in schema.fields:
#     col_name = col.name
#     df = df.withColumn(col_name, generate_random_data(lit(col_name)))
# df = df.drop("id")
for col_name in schema.names:
    df = df.withColumn(col_name, generate_random_data_udf(lit(col_name)).cast(schema[col_name].dataType))
