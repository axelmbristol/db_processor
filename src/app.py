import os
import re
import uuid
from datetime import datetime

import tables
from cassandra.cluster import Cluster
from pymongo import MongoClient
from tables import *


class Animal(IsDescription):
    timestamp = Int32Col()
    serial_number = Int64Col()
    signal_strength = Int16Col()
    battery_voltage = Int16Col()
    first_sensor_value = Int32Col()
    # x_max = Int8Col()
    # x_min = Int8Col()
    # y_max = Int8Col()
    # y_min = Int8Col()
    # z_max = Int8Col()
    # z_min = Int8Col()


class Animal2(IsDescription):
    timestamp = Int32Col()
    serial_number = Int64Col()
    signal_strength_min = Int16Col()
    signal_strength_max = Int16Col()
    battery_voltage = Int16Col()
    first_sensor_value = Int32Col()


print('init mongoDB...')
client = MongoClient('localhost', 27017)
db_names = client.list_database_names()
db_type = 0


def by_size(words, size):
    return [word for word in words if len(word) == size]


def purge_file(filename):
    print("purge...")
    try:
        os.remove(filename)
    except FileNotFoundError:
        print("file not found.")


def get_epoch_from_datetime(date, time):
    return int(datetime.strptime((date + " " + time), '%d/%m/%y %I:%M:%S %p').timestamp())


def add_record_to_table(table, data):
    sn = data[0]["serial_number"]
    table_row = table.row
    for record in data:
        timestamp = get_epoch_from_datetime(record["date"], record["time"])
        sn = int(sn)
        signal_strength = -1
        battery_voltage = -1
        if record['signal_strength'] is not None and re.sub("[^0-9]", "", record["signal_strength"]) != '':
            signal_strength = - int(re.sub("[^0-9]", "", record["signal_strength"]))
        if record['battery_voltage'] is not None and re.sub("[^0-9]", "", record["battery_voltage"]) != '':
            battery_voltage = int(re.sub("[^0-9]", "", record["battery_voltage"]))
        else:
            try:
                #value is sometimes strored in hex
                battery_voltage = int(record["battery_voltage"], 16)
            except (ValueError, TypeError) as ex:
                print(ex)

        first_sensor_value = int(record["first_sensor_value"])
        x_min, x_max, y_min, y_max, z_min, z_max = 0, 0, 0, 0, 0, 0
        ssv = ""
        if "second_sensor_values_xyz" in record and record["second_sensor_values_xyz"] is not None:
            ssv = str(record["second_sensor_values_xyz"])
            split = ssv.split(":")
            # print(split)
            if len(split) == 6:
                x_min, x_max, y_min, y_max, z_min, z_max = int(split[0]), int(split[1]), int(split[2]), int(
                    split[3]), int(split[4]), int(split[5])
            # print(x_min, x_max, y_min, y_max, z_min, z_max)
        table_row['timestamp'] = timestamp
        table_row['serial_number'] = int(sn)
        table_row['signal_strength'] = signal_strength
        table_row['battery_voltage'] = battery_voltage
        table_row['first_sensor_value'] = first_sensor_value
        # animal_h5_table_row['x_min'] = x_min
        # animal_h5_table_row['y_min'] = y_min
        # animal_h5_table_row['z_min'] = z_min
        # animal_h5_table_row['x_max'] = x_max
        # animal_h5_table_row['y_max'] = y_max
        # animal_h5_table_row['z_max'] = z_max
        table_row.append()
    table.flush()


def add_record_to_table_sum(table, data):
    serial = int(data[0]["serial_number"])
    table_row = table.row
    timestamp = get_epoch_from_datetime(data[0]["date"], data[0]["time"])
    signal_strength_array = []
    activity_level_array = []
    battery_voltage_array = []

    for record in data:
        signal_strength = -1
        battery_voltage = -1
        if record['signal_strength'] is not None and re.sub("[^0-9]", "", record["signal_strength"]) != '':
            signal_strength = - int(re.sub("[^0-9]", "", record["signal_strength"]))
        if record['battery_voltage'] is not None and re.sub("[^0-9]", "", record["battery_voltage"]) != '':
            battery_voltage = int(re.sub("[^0-9]", "", record["battery_voltage"]))
        else:
            try:
                battery_voltage = int(record["battery_voltage"], 16)
            except (ValueError, TypeError) as ex:
                print(ex)
        first_sensor_value = int(record["first_sensor_value"])
        signal_strength_array.append(signal_strength)
        battery_voltage_array.append(battery_voltage)
        activity_level_array.append(first_sensor_value)

    table_row['timestamp'] = timestamp
    table_row['serial_number'] = int(serial)
    table_row['signal_strength_min'] = min(signal_strength_array)
    table_row['signal_strength_max'] = max(signal_strength_array)
    table_row['battery_voltage'] = min(battery_voltage_array)
    table_row['first_sensor_value'] = sum(activity_level_array)
    table_row.append()
    del activity_level_array
    del battery_voltage_array
    del signal_strength_array
    table.flush()


if db_type == 0:
    rows = 0
    farm_count = 0
    FILTERS = tables.Filters(complib='blosc', complevel=9)
    compression = False
    h5file = None

    for farm_id in by_size(db_names, 11):
        farm_count += 1
        print("farm id :"+farm_id)
        if compression:
            purge_file(farm_id+"_compressed.h5")
            h5file = tables.open_file(farm_id+"_data_compressed_blosc.h5", "w", driver="H5FD_CORE", filters=FILTERS)
        else:
            purge_file(farm_id+".h5")
            h5file = tables.open_file(farm_id+".h5", "w", driver="H5FD_CORE")

        group_f = h5file.create_group("/", "resolution_f", 'raw data')
        group_m = h5file.create_group("/", "resolution_m", 'resolution per month')
        group_w = h5file.create_group("/", "resolution_w", 'resolution per week')
        group_d = h5file.create_group("/", "resolution_d", 'resolution per day')
        table_f = h5file.create_table(group_f, "data", Animal, "Animal data in full resolution")
        table_m = h5file.create_table(group_m, "data", Animal2, "Animal data activity level averaged by month")
        table_w = h5file.create_table(group_w, "data", Animal2, "Animal data activity level averaged by week")
        table_d = h5file.create_table(group_d, "data", Animal2, "Animal data activity level averaged by day")

        db = client[farm_id]
        colNames = db.list_collection_names()
        colNames.sort()
        collection_count_w = 0
        collection_count_m = 0
        collection_count = 0
        data_w = []
        data_m = []

        #group collection by day
        collections = []
        group = []
        for index, collection_name in enumerate(colNames):
            if len(collection_name.split("_")) == 5:
                group.append(collection_name)
                continue
            if len(group) > 0:
                collections.append(group)
                group = []
            else:
                collections.append([collection_name])

        for index, collection_name in enumerate(colNames):
            collection_count = collection_count + 1
            collection_count_m = collection_count_m + 1
            collection_count_w = collection_count_w + 1

            collection = db[collection_name]
            animals = collection.find_one()["animals"]

            next_ = colNames[index + 1]
            if len(next_.split("_")) == 5:
                animals_next = db[next_].find_one()["animals"]

            for animal in animals:
                tag_data_raw = animal["tag_data"]
                #removes duplicates
                tag_data = [i for n, i in enumerate(tag_data_raw) if i not in tag_data_raw[n + 1:]]
                add_record_to_table(table_f, tag_data)
                add_record_to_table_sum(table_d, tag_data)
                rows += len(tag_data)
                data_m.extend(tag_data)
                data_w.extend(tag_data)

            if collection_count_m == 30:
                collection_count_m = 0
                add_record_to_table_sum(table_m, data_m)
                del data_m
                data_m = []

            if collection_count_w == 7:
                collection_count_w = 0
                add_record_to_table_sum(table_w, data_w)
                del data_w
                data_w = []

            print(str(farm_count) + "/" + str(len(db_names)) + " " + str(collection_count) + "/" + str(len(colNames)) + " " + collection_name + "...")
            # if cpt >= 1:
            #     break

        break

    print("finished added %s rows to pytable" % str(rows))

if db_type == 1:
    rows = 0
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    db_names = ["70101200027_small"]
    for farm_id in db_names:
        # if len(farm_id) < 11:
        #     continue
        print("farm id :" + farm_id)
        db = client[farm_id]
        colNames = db.list_collection_names()
        collection_count = 0
        colNames.sort()
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS \"%s\"
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '5' }
            """ % farm_id)
        print("setting keyspace...")
        session.set_keyspace(farm_id)
        table_name = "data"

        try:
            # session.execute(
            #     "CREATE TABLE if not exists " + "\"" + table_name + "\"" + " (id Text, epoch Int,control_station bigint,serial_number bigint," +
            #     "signal_strength Int,battery_voltage Int,first_sensor_value Int,x_min Int,x_max Int,y_min Int,y_max Int,z_min Int,z_max Int, PRIMARY KEY(id))")
            session.execute(
                "CREATE TABLE if not exists " + "\"" + "test" + "\"" + " (id Int, PRIMARY KEY(id))")

            n = 30000
            for x in range(0, n):

                query = """INSERT INTO """ + "\"" + str(farm_id) + "\"" + "." + "\"" + "test" + "\"" + """ (id) VALUES (%s)"""

                future = session.execute_async(query % int(x))
                block_future_res = future.result()
                block_future_res.response_future
                #print(block_future_res.response_future)
                #print((x/max)*100)
                a = 0

        except Exception as e:
            print(e)

        exit(0)

        for collection_name in colNames:
            collection = db[collection_name]
            animals = collection.find_one()["animals"]
            for animal in animals:
                tag_data = animal["tag_data"]
                serial_number = tag_data[0]["serial_number"]

                for entry in tag_data:
                    ss = -1
                    if entry['signal_strength'] is not None and re.sub("[^0-9]", "", entry["signal_strength"]) != '':
                        ss = int(re.sub("[^0-9]", "", entry["signal_strength"]))
                    bv = -1
                    if entry['battery_voltage'] is not None and re.sub("[^0-9]", "", entry["battery_voltage"]) != '':
                        bv = int(re.sub("[^0-9]", "", entry["battery_voltage"]))
                    x_min, x_max, y_min, y_max, z_min, z_max = 0, 0, 0, 0, 0, 0
                    ssv = ""
                    if 'second_sensor_value' in entry:
                        ssv = str(entry["second_sensor_value"])
                        split = ssv.split(":")
                        x_min, x_max, y_min, y_max, z_min, z_max = split[0], split[1], split[2], split[3], split[4], split[5]
                        print(x_min + " " + x_max + " " + y_min + " " + y_max + " " + z_min + " " + z_max)

                    date_string = entry["date"] + " " + entry["time"]
                    epoch = int(datetime.strptime(date_string, '%d/%m/%y %I:%M:%S %p').timestamp())

                    farm = farm_id.split("_")

                    # query = """INSERT INTO """ + "\"" + str(
                    #     farm_id) + "\"" + "." + "\"" + table_name + "\"" + """ (id, epoch,""" +\
                    #     """control_station, serial_number, signal_strength, battery_voltage, first_sensor_value, """ +\
                    #     """x_min, x_max, y_min, y_max, z_min, z_max) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    query = """INSERT INTO """ + "\"" + str(farm_id) + "\"" + "." + "\"" + "test" + "\"" + """ (id) VALUES (%s)"""

                    id = str(epoch)+"-"+str(serial_number)+" "+str(uuid.uuid4())
                    try:
                        # session.execute_async(query, (
                        #     id, epoch, int(farm[0]), int(serial_number), ss, bv, int(entry["first_sensor_value"]), x_min, x_max,
                        #     y_min, y_max, z_min, z_max))

                        session.execute_async(query % int(rows))

                        rows += 1

                    except Exception as e:
                        print("error while insert into")
                        print(e)
                    # try:
                    #     session.execute_async(query, (epoch, int(farm[0]), int(serial_number), ss, bv, int(entry["first_sensor_value"]),  x_min, x_max, y_min, y_max, z_min, z_max))
                    # except Exception as e:
                    #     print("error while insert into")
                    #     print(e)

            collection_count = collection_count + 1
            # if cpt >= 1:
            #     break
            print(str(collection_count) + "/" + str(len(colNames)) + " " + collection_name + "...")
    print("finished added %s rows to cassandra" % str(rows))
