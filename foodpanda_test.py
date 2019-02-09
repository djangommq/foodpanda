# -*- coding: utf-8 -*-
import csv
import datetime
import os
import time
import requests
import sys
import traceback
import importlib


importlib.reload(sys)

headers = {
    'Connection': 'keep-alive',
    'Accept': '*/*',
    'User-Agent': 'foodpanda/2.15 (iPhone; iOS 10.1.1; Scale/3.00)',
    'X-FP-API-KEY': 'iphone',
    'App-version': 'foodpanda_2.15',
}

date = datetime.datetime.now().strftime("%Y-%m-%d")
file_path = os.path.dirname(os.path.split(os.path.realpath(__file__))[0]) + '/crawlerOutput/' + date + '/'
web_name = 'foodpanda/'
raw_file_name = 'raw.csv'
cuisine_file_name = 'cuisine.csv'
deduplicate_file_name = 'deduplicate.csv'
promo_file_name = 'promo.csv'

# raw数据去重列表
resto_ids = []

# api,国家,城市,左上角纬度,左上角经度,右下角纬度,右下角经度
def getData(api, country_name, city_name, lat_top_left, lng_top_left, lat_bottom_right, lng_bottom_right):
    api_url = "http://" + api + "/api/v5/vendors"
    i=float(lat_bottom_right)
    while i<float(lat_top_left):
        j = float(lng_top_left)
        while j<float(lng_bottom_right):
            lat = i
            lng = j

            #模拟ios请求
            url_params = {
                'latitude': lat,
                'longitude': lng,
                'include': 'cuisines,payment_types,discounts,food_characteristics',
                'payment_type': 'adyen,cod,hosted,applepay,no_payment',
                'serialize_null': 'false',
                'os': 'iOS'
            }
            try:
                # 发送请求
                resp = requests.get(api_url, params=url_params, headers=headers, timeout=60)
                # 时间
                date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # 数据
                data = resp.json()
                print('%s\t%s%s\t%d\t%s' % (date, country_name, city_name, len(data['data']['items']), resp.url))
                # 数据,纬度,经度,城市名称,国家名称
                save(data, lat, lng, city_name, country_name)
            except Exception as e:
                print(e)
                time.sleep(2)  # 出现错误后等待一个小时重试
            j+=0.02
        i+=0.02

# 数据,纬度,经度,城市名称,国家名称
def save(response, lat, lng, city_name, country_name):
    """
    {'status_code': 200, 
    'data': {'available_count': 0, 'returned_count': 0, 'events': None, 'close_reasons': [], 'banner': None, 'items': [], 'aggregations': {'cuisines': None, 'foodCharacteristics': None, 'quickFilters': None}}}
    """
    data = response['data']['items']

    # 城市名称,纬度,经度,数据字典的长度
    saveLog(city_name, lat, lng, len(data))


    for x in range(len(data)):
        i = data[x]
        try:
            raw_data = {
                # date of the week when we run the tool
                'running_date': date,
                # the lat/lng when we 'open' the app and the restaurants shown in app are around this lat/lng point
                'request_lat': lat,
                'request_lng': lng,
                'city_name': i['city']['name'],
                'resto_id': i['id'],
                'is_active': i['is_active'],
                'resto_name': i['name'],
                'rank_in_app': x,
                'resto_address': i['address'] if 'address' in i else 'NA',
                'resto_postcode': i['post_code'] if 'post_code' in i else 'NA',
                'resto_lat': i['latitude'] if not i['latitude'] is None else 0,
                'resto_lng': i['longitude'] if not i['longitude'] is None else 0,
                'web_url': i['web_path'],
                'rating': i['rating'],
                'review_num': i['review_number'],
                'review_with_comment_number': i['review_with_comment_number'],
                'minimum_order_amount': i['minimum_order_amount'],
                'minimum_delivery_fee': i['minimum_delivery_fee'],
                'minimum_delivery_time': i['minimum_delivery_time'],
                'minimum_pickup_time': i['minimum_pickup_time'],
                'customer_type': i['customer_type'],
                'chain_id': i['chain']['id'],
                'chain_name': i['chain'].get('name', 'NA') if not i['chain'].get('name', 'NA') == 'character' else 'NA',
                'is_new': i['is_new'],
                'delivery_fee_type': i['delivery_fee_type'],
                'is_promoted': i['is_promoted'],
                'maximum_express_order_amount': i['maximum_express_order_amount'],
                'budget': i['budget'],
            }
            if not raw_data['resto_address'] == 'Foodpanda Test Restaurant':
                for d in i['discounts']:
                    try:
                        promo_data = {
                            'promo_id': d['id'],
                            'promo_name': d['name'],
                            'promo_description': d.get('description', 'NA') if not d['description'] is None else 'NA',
                            'promo_start_date': d['start_date'],
                            'promo_end_date': d['end_date'],
                            'promo_opening_time': d['opening_time'],
                            'promo_closing_time': d['closing_time'],
                            'promo_condition_type': d['condition_type'] if not d['condition_type'] is None else 'NA',
                            'promo_condition_object': d['condition_object'] if not d[
                                                                                       'condition_object'] is None else 'NA',
                            'promo_minimum_order_value': d['minimum_order_value'] if not d[
                                                                                             'minimum_order_value'] is None else 'NA',
                            'promo_discount_type': d['discount_type'],
                            'promo_discount_amount': d['discount_amount'],
                            'promo_bogo_discount_unit': d['bogo_discount_unit'],
                            'promo_bogo_vendor_id': d['vendor_id'],
                            'promo_bogo_promotional_limit': d['promotional_limit'],
                        }
                    except Exception:
                        promo_data = {}
                    raw_data = dict(raw_data,**promo_data)
                    if raw_data['resto_id'] not in resto_ids:
                        resto_ids.append(raw_data['resto_id'])
        except:
            traceback.print_exc()
            pass


# 城市名称,纬度,经度,数据字典的长度
def saveLog(cityname, lat, lng, number):
    date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 'C:\\Users\\Administrator\\Desktop/crawlerOutput/2018-11-01/log/foodpanda/'
    # 创建日志文件
    isExists = os.path.exists(file_path + 'log/' + web_name)
    if not isExists:
        os.makedirs(file_path + 'log/' + web_name)

    # 打开日志文件,并写入
    with open(file_path + 'log/' + web_name + 'log.csv', 'a',newline='',encoding='utf-8') as logfile:
        logcsv = csv.DictWriter(logfile, fieldnames=['running_date', 'cityname', 'lat', 'lng', 'number'])
        if (not os.path.getsize(file_path + 'log/' + web_name + 'log.csv')):
            logcsv.writeheader()
        logcsv.writerow({
            'running_date': date,
            'cityname': cityname,
            'lat': lat,
            'lng': lng,
            'number': number,
        })



def eachData(id):
    # 获取当前路径/input_data/
    dir_path = os.path.split(os.path.realpath(__file__))[0] + '/input_data/'
    # 文件夹列表[locations.csv]
    dir = os.listdir(dir_path)
    with open(os.path.split(os.path.realpath(__file__))[0] + '/api.csv') as api:
        # 读取csv文件,此处读取到的数据是将每行数据当做列表返回的
        reader = csv.reader(api)
        # 将一行行数据列表放入列表rows中
        rows = [row for row in reader]
    del rows[0]

    # 将rows列表转化成字典,key值为地区名称
    #{'India': 'in-public.foodapi.io', ...
    apis = dict(rows)

    # file_path:'C:\\Users\\Administrator\\Desktop/crawlerOutput/2018-11-01/' 判断是否存在
    isExists = os.path.exists(file_path)

    if not isExists:
        # web_name = 'foodpanda/'      C:\Users\Administrator\Desktop\crawlerOutput\2018-11-01\foodpanda
        os.makedirs(file_path + web_name)
    for i in dir:
        if (os.path.isfile(dir_path + i)):
            # 读取文件夹input_data下的locations.csv
            with open(dir_path + i) as location:
                reader = csv.reader(location)
                citys = [row for row in reader]
            del citys[0]
            # citys:<class 'list'>: [['Taiwan', 'TaiWan', '25.256470', '120.112935', '21.946079', '121.926649'],[....
            for city in citys:
                # 国家,城市,左上角纬度,左上角经度,右下角纬度,右下角经度
                num = city[2]
                num=int(num)
                if num==id:
                    country_name=city[0]
                    city_name=city[1]
                    getData(apis[country_name], country_name,city_name , city[3], city[4], city[5], city[6])


if __name__ == '__main__':
    eachData(5)
    print(len(resto_ids))