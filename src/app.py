import os
import re
import uuid
from datetime import datetime

import tables
from cassandra.cluster import Cluster
from numpy import array
from pymongo import MongoClient
from tables import *
import numpy as np


class Animal(IsDescription):
    epoch = Int32Col()
    serial_number = Int64Col()
    signal_strength = Int16Col()
    battery_voltage = Int16Col()
    first_sensor_value = Int32Col()
    x_max = Int8Col()
    x_min = Int8Col()
    y_max = Int8Col()
    y_min = Int8Col()
    z_max = Int8Col()
    z_min = Int8Col()


print('init mongoDB...')
client = MongoClient('localhost', 27017)
db_names = client.list_database_names()

db_type = 0

# todo remove
# db_names = ["70101200027_small"]


def purge_file(filename):
    print("purge...")
    try:
        os.remove(filename)
    except FileNotFoundError:
        print("file not found.")


if db_type == 0:
    rows = 0
    farm_count = 0
    FILTERS = tables.Filters(complib='blosc', complevel=9)
    con = False
    h5file = None
    if con:
        purge_file("data_compressed_blosc.h5")
        h5file = tables.open_file("data_compressed_blosc.h5", "w", driver="H5FD_CORE", filters=FILTERS)
    else:
        purge_file("data.h5")
        h5file = tables.open_file("data.h5", "w", driver="H5FD_CORE")

    group = h5file.create_group("/", "data", 'Farm id')
    table = h5file.create_table(group, "data", Animal, "Animal id")
    for farm_id in db_names:
        farm_count += 1
        if len(farm_id) != 11:
            continue
        print("farm id :"+farm_id)
        db = client[farm_id]
        colNames = db.list_collection_names()
        cpt = 0
        colNames.sort()

        for colName in colNames:
            collection = db[colName]
            animals = collection.find_one()["animals"]
            for animal in animals:
                tag_data = animal["tag_data"]
                serial_number = tag_data[0]["serial_number"]
                an = table.row
                for entry in tag_data:
                    date_string = entry["date"] + " " + entry["time"]
                    epoch = int(datetime.strptime(date_string, '%d/%m/%y %I:%M:%S %p').timestamp())
                    serial_number = int(serial_number)
                    signal_strength = -1
                    battery_voltage = -1
                    if entry['signal_strength'] is not None and re.sub("[^0-9]", "", entry["signal_strength"]) != '':
                        signal_strength = int(re.sub("[^0-9]", "", entry["signal_strength"]))
                    if entry['battery_voltage'] is not None and re.sub("[^0-9]", "", entry["battery_voltage"]) != '':
                        battery_voltage = int(re.sub("[^0-9]", "", entry["battery_voltage"]))
                    first_sensor_value = int(entry["first_sensor_value"])
                    x_min, x_max, y_min, y_max, z_min, z_max = 0, 0, 0, 0, 0, 0
                    ssv = ""
                    if "second_sensor_values_xyz" in entry and entry["second_sensor_values_xyz"] is not None:
                        ssv = str(entry["second_sensor_values_xyz"])
                        split = ssv.split(":")
                        # print(split)
                        if len(split) == 6:
                            x_min, x_max, y_min, y_max, z_min, z_max = int(split[0]), int(split[1]), int(split[2]), int(split[3]), int(split[4]), int(split[5])
                        # print(x_min, x_max, y_min, y_max, z_min, z_max)
                    rows += 1
                    an['epoch'] = epoch
                    an['serial_number'] = int(serial_number)
                    an['signal_strength'] = signal_strength
                    an['battery_voltage'] = battery_voltage
                    an['first_sensor_value'] = first_sensor_value
                    an['x_min'] = x_min
                    an['y_min'] = y_min
                    an['z_min'] = z_min
                    an['x_max'] = x_max
                    an['y_max'] = y_max
                    an['z_max'] = z_max
                    an.append()
                table.flush()
            cpt = cpt + 1
            print(str(farm_count) + "/" + str(len(db_names)) + " " + str(cpt) + "/" + str(len(colNames)) + " " + colName + "...")
            # if cpt >= 1:
            #     break

    print("finished added %s rows to pytable" % str(rows))

if db_type == 1:
    rows = 0
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    for farm_id in db_names:
        if len(farm_id) < 11:
            continue
        print("farm id :" + farm_id)
        db = client[farm_id]
        colNames = db.list_collection_names()
        cpt = 0
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

            max = 100
            for x in range(0, max):

                query = """INSERT INTO """ + "\"" + str(farm_id) + "\"" + "." + "\"" + "test" + "\"" + """ (id) VALUES (%s)"""

                future = session.execute_async(query % int(x))
                block_future_res = future.result()
                block_future_res.response_future

                print(block_future_res.response_future)


                #print((x/max)*100)
                a = 0

        except Exception as e:
            print(e)

        exit(0)

        for colName in colNames:
            collection = db[colName]
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

            cpt = cpt + 1
            # if cpt >= 1:
            #     break
            print(str(cpt) + "/" + str(len(colNames)) + " " + colName + "...")
    print("finished added %s rows to cassandra" % str(rows))
