import os
import csv
from mongodb_utils import get_db
from foodpanda import file_path,web_name

country_list=['Taiwan','Hong Kong','India','Pakistan','Romania']
file_list=['raw','cuisine','deduplicate','promo']

# fields
raw_head = [
    'running_date',
    'request_lat',
    'request_lng',
    'city_name',
    'resto_id',
    'is_active',
    'resto_name',
    'rank_in_app',
    'resto_address',
    'resto_postcode',
    'resto_lat',
    'resto_lng',
    'web_url',
    'rating',
    'review_num',
    'review_with_comment_number',
    'minimum_order_amount',
    'minimum_delivery_fee',
    'minimum_delivery_time',
    'minimum_pickup_time',
    'customer_type',
    'chain_id',
    'chain_name',
    'is_new',
    'delivery_fee_type',
    'is_promoted',
    'maximum_express_order_amount',
    'budget',
    'promo_id',
    'promo_name',
    'promo_description',
    'promo_start_date',
    'promo_end_date',
    'promo_opening_time',
    'promo_closing_time',
    'promo_condition_type',
    'promo_condition_object',
    'promo_minimum_order_value',
    'promo_discount_type',
    'promo_discount_amount',
    'promo_bogo_discount_unit',
    'promo_bogo_vendor_id',
    'promo_bogo_promotional_limit',
]
cuisines_head = [
    'running_date',
    'type',
    'city_name',
    'resto_id',
    'is_active',
    'cuisines',
]
promo_head = [
    'running_date',
    'type',
    'city_name',
    'resto_id',
    'is_active',
    'resto_name',
    'promo_id',
    'promo_name',
    'promo_description',
    'promo_start_date',
    'promo_end_date',
    'promo_opening_time',
    'promo_closing_time',
    'promo_condition_type',
    'promo_condition_object',
    'promo_minimum_order_value',
    'promo_discount_type',
    'promo_discount_amount',
    'promo_bogo_discount_unit',
    'promo_bogo_vendor_id',
    'promo_bogo_promotional_limit',
]
deduplicate_head = [
    "running_date",
    "city_name",
    "resto_id",
    "resto_name",
    "resto_address",
    "resto_postcode",
    "resto_lat",
    "resto_lng",
    "web_url",
    "rating",
    "review_num",
    "review_with_comment_number",
    "minimum_order_amount",
    "minimum_delivery_fee",
    "minimum_pickup_time",
    "customer_type",
    "chain_id",
    "chain_name",
    "is_new",
    "delivery_fee_type",
    "is_promoted",
    "maximum_express_order_amount",
    "budget"]

# 创建链接数据库的对象
c_mongo=get_db()

filepath=os.path.join(file_path,web_name)

if __name__ == '__main__':
      for country in country_list:
          for file in file_list:
              table_name=country+'_'+file
              data_info=c_mongo.all_items(table_name)
              path=os.path.join(filepath,country)
              if not os.path.exists(path):
                  os.makedirs(path)
              filename=os.path.join(path,table_name+'.csv')
              with open(filename,'a',encoding='utf-8',newline='')as f:
                  writer=None
                  if file=='raw':
                      writer=csv.DictWriter(f,fieldnames=raw_head)

                  elif file=='cuisine':
                      writer = csv.DictWriter(f, fieldnames=cuisines_head)

                  elif file=='deduplicate':
                      writer = csv.DictWriter(f, fieldnames=deduplicate_head)

                  elif file=='promo':
                      writer = csv.DictWriter(f, fieldnames=promo_head)

                  if not os.path.getsize(filename):
                      writer.writeheader()
                  for data in data_info:
                      writer.writerow(data)
      print('数据成功导出')