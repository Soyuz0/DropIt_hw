from flask import Flask, request
import pandas as pd
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import holidayapi

app = Flask(__name__)
TimeSlots_Data = None
Deliveries_Data = None

@dataclass
class Address:
    street: str
    city: str
    line1: str
    line2: str
    country: str
    postcode: str

class TimeSlot:
    timeslot_id: int
    start_time: str
    end_time: str
    city: str

@dataclass
class Delivery:
    delivery_id: int
    user: str
    status: str
    timeslot_id: int

def load_data():
    global TimeSlots_Data
    global Deliveries_Data
    TimeSlots_Data = pd.read_json('TimeSlots.json').T
    TimeSlots_Data['holiday'] = TimeSlots_Data.apply (lambda row: searchHoliday(row.start_time[0:10]), axis=1)
    TimeSlots_Data = TimeSlots_Data[TimeSlots_Data['holiday'] == 0]
    TimeSlots_Data = TimeSlots_Data.T.to_dict()
    print(TimeSlots_Data)
    Deliveries_Data = pd.read_csv('Deliveries.csv')
    

def searchGeoApi(searchTerm):
    import requests
    from requests.structures import CaseInsensitiveDict
    API_KEY = '5be222726bcb49ee88a8a27e77b06b3f'
    url = "https://api.geoapify.com/v1/geocode/search?text=" + searchTerm+ "&format=json&apiKey=" + API_KEY
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"

    resp = requests.get(url, headers=headers)
    cont = resp.json()
    return(cont['results'][0])


def searchHoliday(date):
    API_KEY = 'd6b6b924-ee8d-4cb8-bffb-3bd6a46aa493'
    hapi = holidayapi.v1(API_KEY)
    parameters = {
        'country': 'US',
        'year':    int(date[0:4]),
        'month':  int(date[5:7]),
        'day':      int(date[8:10]),
        'public':   True
    }
    holidays = hapi.holidays(parameters)
    return len(holidays['holidays'])


def NewDelivery(user, timeslot_id):
    delivery_id = 0
    if len(Deliveries_Data) > 0:
        delivery_id = max(Deliveries_Data['delivery_id'])+1
    timestolt_id_s = [TimeSlots_Data[key]['timeslot_id'] for key in TimeSlots_Data.keys()]
    if timeslot_id not in timestolt_id_s:
        return 'time slot not found'
    if len(Deliveries_Data[Deliveries_Data['timeslot_id'] == timeslot_id]) >= 2:
        return 'time slot already booked'
    return Delivery(delivery_id, user, 'booked', timeslot_id)


@app.route('/resolve-address', methods = ['POST'])
def resolve_address():
    request_data = request.get_json()
    if request_data:
        if 'searchTerm' in request_data:
            searchTerm = request_data['searchTerm']
            result = searchGeoApi(searchTerm)
            address = Address(result['street'], 
                result['city'],
                result['address_line1'],
                result['address_line2'],
                result['country'],
                result['postcode'])
            return address.__dict__ 
            
    return {'message': 'error request'}, 400


@app.route('/timeslots', methods = ['POST'])
def resolve_timeslots():
    request_data = request.get_json()
    if request_data:
        if 'address' in request_data:
            address_dict = request_data['address']
            attr = ['city', 'line1', 'line2', 'street', 'country', 'postcode']
            if not set(attr).issubset(address_dict.keys()):
                return {'message': 'error request'}, 400
            return_data = {}
            for timeslot in TimeSlots_Data.values():
                if timeslot['city'] == address_dict['city']:
                    return_data[(len(return_data))] = timeslot
            return return_data, 200
    return {'message': 'error request'}, 400



@app.route('/deliveries', methods = ['POST'])
@app.route('/deliveries/<deliveriy_id>/<status>', methods = ['POST'])
def deliveries_post(deliveriy_id = None, status = None):
    global Deliveries_Data
    if deliveriy_id:
        if status != 'complete':
            return {'message': 'error status doesn;t except anithing but complete'}, 400
        ind = Deliveries_Data.index[Deliveries_Data['delivery_id'] == int(deliveriy_id)].tolist()
        if len(ind) == 0:
            return {'message': f'delivery {deliveriy_id} does not exist'}, 400
        ind = ind[0]
        if Deliveries_Data['status'][ind] == 'complete':
            return {'message': f'delivery {deliveriy_id} already completed'}, 400
        Deliveries_Data['status'][ind] = status
        Deliveries_Data.to_csv('Deliveries.csv', index=False)
        return {'message': f'completed delivery {deliveriy_id}'}, 200
    
    request_data = request.get_json()
    if request_data:
        attr = ['user', 'timeslotId']
        if not set(attr).issubset(request_data.keys()):
            return {'message': 'error request'}, 400
        delivery = NewDelivery(request_data['user'], request_data['timeslotId'])
        if isinstance(delivery, str):
            return {'message': delivery}, 400
        new_df = pd.DataFrame([delivery.__dict__])
        Deliveries_Data = pd.concat([Deliveries_Data, new_df], ignore_index=True)
        Deliveries_Data.to_csv('Deliveries.csv', index=False)
        return delivery.__dict__, 200 

    return {'message': 'error request'}, 400


@app.route('/deliveries/<deliveriy_id>', methods = ['DELETE'])
def deliveries_delete(deliveriy_id = None):
    global Deliveries_Data
    ind = Deliveries_Data.index[Deliveries_Data['delivery_id'] == int(deliveriy_id)].tolist()
    if len(ind) != 1:
        return {'message': f'delivery {deliveriy_id} does not exist'}, 400
    Deliveries_Data = Deliveries_Data[Deliveries_Data['delivery_id'] != int(deliveriy_id)]
    Deliveries_Data.to_csv('Deliveries.csv', index=False)
    return {'message': f'delivery {deliveriy_id} deleted'}, 200

@app.route('/deliveries/<Timerange>', methods = ['Get'])
def deliveries_get(Timerange = None):
    attr = ['daily', 'weekly']
    if Timerange not in attr:
        return {'message': 'error not a valied time range'}, 400
    today = date.today()
    start = today
    end = start + timedelta(days=1)
    if Timerange == 'weekly':
        start = today - timedelta(days=today.weekday())
        end = start + timedelta(days=6)
    
    start = str(start.strftime('%Y-%m-%d %H:%M:%S'))
    end = str(end.strftime('%Y-%m-%d %H:%M:%S'))
    return_delveried = {}
    
    for index, delivery in Deliveries_Data.iterrows():
        timeslot_id = delivery['timeslot_id']
        timeslots = [v for v in TimeSlots_Data.values() if v['timeslot_id'] == timeslot_id]
        if len(timeslots) == 1:
            timeslot = timeslots[0]
            start_time = str(datetime.strptime(timeslot['start_time'], '%Y-%m-%d %H:%M:%S'))
            if start <= start_time <= end:
                return_delveried[str(len(return_delveried))] = ({
                    'delivery_id': delivery['delivery_id'],
                    'user': delivery['user'],
                    'status': delivery['status'],
                    'timeslot_id': timeslot['timeslot_id'],
                    'start_time': timeslot['start_time'],
                    'end_time': timeslot['end_time']
                })
    return (return_delveried), 200

if __name__ == '__main__':
    load_data()
    app.run(port=5000, threaded=True)