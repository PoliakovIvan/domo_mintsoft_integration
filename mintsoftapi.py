import requests
import pandas as pd
from datetime import datetime, timedelta
import schedule
import time
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def dailyUpdate():
    current_date = datetime.now()
    FORMATTED_DATE = current_date.strftime('%Y-%m-%d')
    order_list(FORMATTED_DATE)

def weeklyUpdate():
    current_date = datetime.now()
    FORMATTED_DATE = current_date - timedelta(days=7)
    order_list(FORMATTED_DATE)


def order_list(FORMATTED_DATE):
    print('starting..')
    pageNo = 1
    failed_count = 0
    failed_data = []
    all_data=[]
    BASE_URL = os.getenv('MINTSOFT_URL')
    

    connection, cursor = create_conn({
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
    })

    if connection is None or cursor is None:
        return 
    
    print('Start of the process...')
    while True:
        orders_data = []
        print(f'Page #: {pageNo}')
        # -- GET MINTSOFT DATA --
        URL_ORDER = f"{BASE_URL}/Order/List?PageNo={pageNo}&SinceDate={FORMATTED_DATE}"#&SinceDate={FORMATTED_DATE}2023-01-01
        API_KEY = os.getenv('API_KEY'),
        headers = {
            'ms-apikey': API_KEY 
        }
        try:
            response = requests.get(URL_ORDER, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()
  
            # Checking for uniqueness of a value
            for item in data:
                if item not in all_data:
                    data = data
            if data==[]: 
                print("No more data to fetch.")
                break
            pageNo += 1
        except requests.exceptions.RequestException as e:
            print("An error occurred while executing the request:", e)
            break 

        # -- MAP MINTSOFT DATA --
        for order in data:

            orderId = order["ID"]
            orderNumber = order["OrderNumber"]
            externalOrderReference = order["ExternalOrderReference"]
            orderDate = order["OrderDate"]
            despatchDate = order["DespatchDate"]
            companyName = order["CompanyName"]
            county = order["County"]
            orderCountry = order["Country"]["Name"]
            warehouseId = order["WarehouseId"]
            channelName = order["Channel"]["Name"]
            courierServiceName = order["CourierServiceName"]
            orderStatusId = order["OrderStatusId"]
            clientId = order["ClientId"]
            numberOfParcels = order['NumberOfParcels']
            totalItems = order["TotalItems"]
            totalWeight = order["TotalWeight"]
            orderValue = order['OrderValue']
            courierServiceTypeID = order["CourierServiceTypeId"]
            trackingNumber = order["TrackingNumber"]
            warehouseInfo = warehouse(warehouseId, BASE_URL)
            statusName = statuses(orderStatusId, BASE_URL)
            clientName = clients(clientId, BASE_URL)
            courierServiceType = courierType(courierServiceTypeID, BASE_URL)


            order_string = (
                f'{clientName}, {orderId}, {orderNumber}, {externalOrderReference}, '
                f'{orderDate}, {despatchDate}, {orderCountry}, {trackingNumber}, {companyName}, '
                f'{county}, {statusName}, {warehouseInfo}, {channelName}, '
                f'{courierServiceName}, {totalItems}, {totalWeight}, {orderValue}, '
                f'{courierServiceType}, {numberOfParcels}'
            )
            for elements in order_string:
                if elements is None:
                    elements = 'NULL' 

            split_order_string = order_string.split(', ')
            count_elements = len(split_order_string)
            if count_elements == 19:
                # orders_data.append(order_string)
                print(f'Successfully added {orderId}')
            else:
                print(f'Not added {orderId}, len: {count_elements}')
                failed_data.append(order_string)
                failed_count += 1
                del split_order_string[9]
                order_string = ', '.join(split_order_string)
                print(f'Fixed: {order_string}')

            orders_data.append(order_string) 
            
            for order_string in orders_data:
                fields = order_string.split(', ')

                clientName, orderId, orderNumber, externalOrderReference, \
                orderDate, despatchDate, orderCountry, trackingNumber, companyName, \
                county, statusName, warehouseInfo, channelName, courierServiceName, \
                totalItems, totalWeight, orderValue, courierServiceType, numberOfParcels = fields
                orderDate = orderDate if orderDate != 'None' else None
                despatchDate = despatchDate if despatchDate != 'None' else None

                insert_query = ("""
                    INSERT INTO domo (client_name, order_id, order_number, external_order_reference,
                                        order_date, despatch_date, order_country, tracking_number, company_name,
                                        county, status_name, warehouse_info, channel_name, courier_service_name,
                                        total_items, total_weight, order_value, courier_service_type, number_of_parcels)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (order_id) DO UPDATE SET
                    client_name = EXCLUDED.client_name,
                    order_number = EXCLUDED.order_number,
                    external_order_reference = EXCLUDED.external_order_reference,
                    order_date = EXCLUDED.order_date,
                    despatch_date = EXCLUDED.despatch_date,
                    order_country = EXCLUDED.order_country,
                    tracking_number = EXCLUDED.tracking_number,
                    company_name = EXCLUDED.company_name,
                    county = EXCLUDED.county,
                    status_name = EXCLUDED.status_name,
                    warehouse_info = EXCLUDED.warehouse_info,
                    channel_name = EXCLUDED.channel_name,
                    courier_service_name = EXCLUDED.courier_service_name,
                    total_items = EXCLUDED.total_items,
                    total_weight = EXCLUDED.total_weight,
                    order_value = EXCLUDED.order_value,
                    courier_service_type = EXCLUDED.courier_service_type,
                    number_of_parcels = EXCLUDED.number_of_parcels;
                """)

                cursor.execute(insert_query, (
                    clientName, int(orderId), orderNumber, externalOrderReference,
                    orderDate, despatchDate, orderCountry, trackingNumber, companyName,
                    county, statusName, warehouseInfo, channelName, courierServiceName,
                    int(totalItems), float(totalWeight), float(orderValue),
                    courierServiceType, int(numberOfParcels)
                ))
                
                try:
                    connection.commit()

                except (Exception, psycopg2.Error) as error:
                    print("Error PostgreSQL", error)
                    connection.rollback()
                    raise
    print(f'fixed orders: {failed_count}')  
    print(failed_data)          
                               
 
    close_conn(connection, cursor)
    # -- API request to DOMO --
    #domoapi(orders_data)
        


def close_conn(connection, cursor):
    if cursor:
        cursor.close()
    if connection:
        connection.close()
    print("Connection to PostgreSQL cloused")

def create_conn(config):
    try:
        connection = psycopg2.connect(**config)
        cursor = connection.cursor()
        print ("database connected")
        return connection, cursor
    except:
        print('Connection error')
        return None, None

    

# -- GET Warehouse info from Minsoft --
def warehouse(warehouseId_order, BASE_URL):
    URL_WAREHOUSE = f"{BASE_URL}/Warehouse"
    API_KEY = os.getenv('API_KEY'),

    headers = {
        'ms-apikey': API_KEY 
    }
    try:
        response = requests.get(URL_WAREHOUSE, headers=headers)
        data = response.json()

        for warehouse in data:
            if warehouseId_order == warehouse["ID"]:
                warehouseName = warehouse["Name"]
                warehouseCounty = warehouse["County"]
                return f'{warehouseName} / {warehouseCounty}'

    except requests.exceptions.RequestException as e:
        print("An error occurred while executing the request:", e)
        return 'Warehouse not found'

# -- GET Statuses info from Minsoft --
def statuses(orderStatusId, BASE_URL):
    URL_STATUS = f"{BASE_URL}/Order/Statuses"
    API_KEY = os.getenv('API_KEY'),

    headers = {
        'ms-apikey': API_KEY 
    }
    try:
        response = requests.get(URL_STATUS, headers=headers, timeout=60)
        data = response.json()

        for status in data:
            if orderStatusId == status["ID"]:
                statusName = status["Name"]
                return statusName

    except requests.exceptions.RequestException as e:
        print("An error occurred while executing the request:", e)
        return 'Status not found'

# -- GET Clients info from Minsoft --
def clients(clientId, BASE_URL):
    URL_CLIENTS = "{BASE_URL}/Client"
    API_KEY = os.getenv('API_KEY'),

    headers = {
        'ms-apikey': API_KEY 
    }
    try:
        response = requests.get(URL_CLIENTS, headers=headers, timeout=60)
        data = response.json()

        for client in data:
            if clientId == client["ID"]:
                clientName = client["Name"]
                return clientName

    except requests.exceptions.RequestException as e:
        print("An error occurred while executing the request:", e)
        return 'Client name not found'

# -- GET Courier Type info from Minsoft --
def courierType(courierServiceTypeID, BASE_URL):
    URL_COURIERTYPE = f"{BASE_URL}/Courier/ServiceTypes"
    API_KEY = os.getenv('API_KEY'),

    headers = {
        'ms-apikey': API_KEY 
    }
    try:
        response = requests.get(URL_COURIERTYPE, headers=headers, timeout=60)
        data = response.json()

        for serviceType in data:
            if courierServiceTypeID == serviceType["ID"]:
                courierServiceTypeName = serviceType["Name"]
                return courierServiceTypeName

    except requests.exceptions.RequestException as e:
        print("An error occurred while executing the request:", e)
        return 'Courier service type not found'

# -- API request to DOMO --
def domoapi(orders_data):

    # -- DOMO auth --
    BASE_DOMO_URL=os.getenv('DOMO_URL')
    URL_AUTH = f"{BASE_DOMO_URL}/oauth/token?grant_type=client_credentials&scope=data"
    payload = ""
    headers = {
    'Authorization': os.getenv('DOMO_TOKEN'),
    }
    response = requests.request("GET", URL_AUTH, headers=headers, data=payload)
    data_token = response.json()
    access_token = data_token['access_token']

    # -- Updata Dataset --
    URL_UPDATE_DATASET = f"{BASE_DOMO_URL}/v1/datasets/9746ee40-b9d5-43f0-bdcc-ece691184e5c/data?updateMethod=REPLACE"
    payload = orders_data

    payload_str = ''
    for elements in payload:
        # Cheaked lenght of row
        count_elements = elements.split(',')
        count_elements = len(count_elements)
        if count_elements == 19:
            payload_str = payload_str + elements + "\r\n"
    payload_str = payload_str.encode('utf-8')

    headers = {
        'Content-Type': "text/csv",
        "Accept": 'application/json',
        'Authorization': f'bearer {access_token}',
        'Cookie': 'JSESSIONID=node01upy1y24fu9lf1ccghez1c8fre2467.node0'
    }

    try:
        response = requests.request("PUT", URL_UPDATE_DATASET, headers=headers, data=payload_str)
        print('End of the process!')
    except response.exceptions.RequestException as e:
        print("An error occurred while executing the response:", e)

    return response

# current_date = datetime.now()
# FORMATTED_DATE = current_date.strftime('%Y-%m-%d')
# order_list(FORMATTED_DATE)



# schedule.every(3).hours.do(dailyUpdate)
schedule.every().day.at("22:00").do(weeklyUpdate)

while True:
    schedule.run_pending()
    time.sleep(1) 

