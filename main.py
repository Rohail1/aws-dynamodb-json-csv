import json
import re
import csv
from datetime import datetime


class DynamodbObject:


    def __init__(self, dict):
        self.obj = dict['Item']
        self.dict_obj = None

    def to_dict(self):
        """ DynamoDB object hook to return python values """
        dict_object = {}
        for key in self.obj:
            try:
                # First - Try to parse the self.obj as DynamoDB parsed
                if 'BOOL' in self.obj[key]:
                    dict_object[key] = self.obj[key]['BOOL']
                if 'S' in self.obj[key]:
                    val = self.obj[key]['S']
                    try:
                        dict_object = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%f')
                    except:
                        dict_object[key] = str(val)
                if 'SS' in self.obj[key]:
                    dict_object[key] = list(self.obj[key]['SS'])
                if 'N' in self.obj[key]:
                    if re.match("^-?\d+?\.\d+?$", self.obj[key]['N']) is not None:
                        dict_object[key] = float(self.obj[key]['N'])
                    else:
                        try:
                            dict_object[key] = int(self.obj[key]['N'])
                        except:
                            dict_object[key] = int(self.obj[key]['N'])
                if 'B' in self.obj[key]:
                    dict_object[key] = str(self.obj[key]['B'])
                if 'NS' in self.obj[key]:
                    dict_object[key] = set(self.obj[key]['NS'])
                if 'BS' in self.obj[key]:
                    dict_object[key] = set(self.obj[key]['BS'])
                if 'M' in self.obj[key]:
                    dict_object[key] = self.obj[key]['M']
                if 'L' in self.obj[key]:
                    dict_object[key] = self.obj[key]['L']
                if 'NULL' in self.obj[key] and self.obj[key]['NULL'] is True:
                    dict_object[key] = None
            except Exception as ex:
                print(key)
                print(ex)
                dict_object = self.obj

        self.dict_obj = dict_object
        return self.dict_obj


decoder = json.JSONDecoder()
data_list = []
# field_names = ['datetime', 'device_name', 'Elevation', 'Longitude', 'UV_light', 'Latitude', 'soil_humidity', 'Audio', 'CO_level', 'smoke', 'Air_quality', 'Vis_light', 'IR_light', 'humidity', 'Temperature']
field_names = ['integration_id', 'recipeId', 'user_id', 'board_id','account_id']
with open('sampledata.json', 'r') as content_file:

    content = content_file.readlines()
    for line in content:

        sensor_data = DynamodbObject(json.loads(line)).to_dict()
        data_list.append(sensor_data)

with open('data.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=field_names)
    writer.writeheader()
    writer.writerows(data_list)

