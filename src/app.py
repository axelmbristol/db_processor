from pymongo import MongoClient
from tables import *
import os


class Animal(IsDescription):
    time = StringCol(16)
    date = StringCol(16)
    serial_number = Int64Col()
    signal_strength = Int16Col()
    battery_voltage = StringCol(16)
    first_sensor_value = Int32Col()
    second_sensor_value = StringCol(16)


print('init mongoDB...')
client = MongoClient('localhost', 27017)
db_names = client.list_database_names()

for farm_id in db_names:
    if len(farm_id) != 11:
        continue
    print("farm id :"+farm_id)
    db = client[farm_id]
    colNames = db.list_collection_names()
    cpt = 0
    colNames.sort()
    file_name = "data.h5"
    h5file = open_file(file_name, mode="w", title="database file")
    group = h5file.create_group("/", "_"+farm_id, 'Farm id')
    for colName in colNames:
        print(str(cpt) + "/" + str(len(colNames)) + " " + colName + "...")
        collection = db[colName]
        animals = collection.find_one()["animals"]
        for animal in animals:
            tag_data = animal["tag_data"]
            serial_number = tag_data[0]["serial_number"]
            table = h5file.create_table(group, "_"+str(serial_number), Animal, "Animal id")
            an = table.row
            for entry in tag_data:
                an['time'] = str(entry["time"])
                an['date'] = str(entry["date"])
                an['serial_number'] = int(serial_number)
                an['signal_strength'] = int(entry["signal_strength"])
                an['battery_voltage'] = str(entry["battery_voltage"])
                an['first_sensor_value'] = int(entry["first_sensor_value"])
                if 'second_sensor_value' in entry:
                    an['second_sensor_value'] = str(entry["second_sensor_value"])
                an.append()
            table.flush()
        # cpt = cpt + 1
        # if cpt >= 1:
        #     break
print('finished')
