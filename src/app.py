from pymongo import MongoClient
from tables import *
import os
import re


class Animal(IsDescription):
    time = StringCol(16)
    date = StringCol(16)
    serial_number = Int64Col()
    signal_strength = Int16Col()
    battery_voltage = Int16Col()
    first_sensor_value = Int32Col()
    second_sensor_value = StringCol(16)


print('init mongoDB...')
client = MongoClient('localhost', 27017)
db_names = client.list_database_names()

file_name = "data.h5"
print("purge...")
os.remove(file_name)
h5file = open_file(file_name, mode="a", title="database file")

for farm_id in db_names:
    if len(farm_id) != 11:
        continue
    print("farm id :"+farm_id)
    db = client[farm_id]
    colNames = db.list_collection_names()
    cpt = 0
    colNames.sort()
    group = h5file.create_group("/", "_"+farm_id, 'Farm id')
    for colName in colNames:
        print(str(cpt) + "/" + str(len(colNames)) + " " + colName + "...")
        collection = db[colName]
        animals = collection.find_one()["animals"]
        for animal in animals:
            tag_data = animal["tag_data"]
            serial_number = tag_data[0]["serial_number"]

            node = "_"+str(serial_number)
            where = "/_"+farm_id
            if "_"+str(serial_number) not in group:
                table = h5file.create_table(group, node, Animal, "Animal id")
            else:
                table = h5file.get_node(where, node)

            an = table.row
            for entry in tag_data:
                an['time'] = str(entry["time"])
                an['date'] = str(entry["date"])
                an['serial_number'] = int(serial_number)
                if entry['signal_strength'] is not None and re.sub("[^0-9]", "", entry["signal_strength"]) != '':
                    an['signal_strength'] = int(re.sub("[^0-9]", "", entry["signal_strength"]))
                if entry['battery_voltage'] is not None and re.sub("[^0-9]", "", entry["battery_voltage"]) != '':
                    an['battery_voltage'] = int(re.sub("[^0-9]", "", entry["battery_voltage"]))
                an['first_sensor_value'] = int(entry["first_sensor_value"])
                if 'second_sensor_value' in entry:
                    an['second_sensor_value'] = str(entry["second_sensor_value"])
                an.append()
            table.flush()
        cpt = cpt + 1
        # if cpt >= 1:
        #     break
print('finished')
