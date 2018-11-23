import os
import re
import uuid
from datetime import datetime

import tables
from cassandra.cluster import Cluster
from pymongo import MongoClient
from tables import *
import os.path
from collections import defaultdict
import dateutil.relativedelta
import time


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


def add_record_to_table_sum(table, timestamp_f, serial_number_f, signal_strenght_max, signal_strenght_min, battery_voltage_min, activity_level_avg):
    table_row = table.row
    table_row['timestamp'] = int(timestamp_f)
    table_row['serial_number'] = int(serial_number_f)
    table_row['signal_strength_min'] = signal_strenght_min
    table_row['signal_strength_max'] = signal_strenght_max
    table_row['battery_voltage'] = battery_voltage_min
    table_row['first_sensor_value'] = activity_level_avg
    table_row.append()
    table.flush()


def add_record_to_table_single(table, timestamp_s, serial_number_s, signal_strenght_s, battery_voltage_s, activity_level_s):
    table_row = table.row
    table_row['timestamp'] = int(timestamp_s)
    table_row['serial_number'] = int(serial_number_s)
    table_row['signal_strength'] = signal_strenght_s
    table_row['battery_voltage'] = battery_voltage_s
    table_row['first_sensor_value'] = activity_level_s
    table_row.append()
    table.flush()


def is_same_day(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    return dt1.day == dt2.day


def is_same_month(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    return dt1.month == dt2.month


def get_elapsed_days(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    rd = dateutil.relativedelta.relativedelta(dt2, dt1)
    return rd.days


def get_elapsed_hours(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    rd = dateutil.relativedelta.relativedelta(dt2, dt1)
    return rd.hours


def get_elapsed_minutes(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    rd = dateutil.relativedelta.relativedelta(dt2, dt1)
    return rd.minutes


def get_elapsed_time_string(time_initial, time_next):
    dt1 = datetime.fromtimestamp(time_initial)
    dt2 = datetime.fromtimestamp(time_next)
    rd = dateutil.relativedelta.relativedelta(dt2, dt1)
    return '%02d:%02d:%02d:%02d' % (rd.days, rd.hours, rd.minutes, rd.seconds)


def process_raw_file(farm_id):
    start_time = time.time()
    h5file_raw = tables.open_file("C:\\Users\\fo18103\PycharmProjects\mongo2pytables\src\\" + farm_id + "_raw.h5", "r")
    data = h5file_raw.root.resolution_f.data
    list_raw = [(x['timestamp'], x['serial_number'], x['signal_strength'], x['battery_voltage'], x['first_sensor_value']) for x in data.iterrows()]
    groups = defaultdict(list)

    for obj in list_raw:
        groups[obj[1]].append(obj)

    grouped_list = list(groups.values())
    animal_list_grouped_by_serialn = [i for n, i in enumerate(grouped_list) if i not in grouped_list[n + 1:]]

    #init new .h5 file for receiving sorted data
    FILTERS = tables.Filters(complib='blosc', complevel=9)
    compression = False
    if compression:
        purge_file(farm_id + "_data_compressed_blosc.h5")
        h5file = tables.open_file(farm_id + "_data_compressed_blosc.h5", "w", driver="H5FD_CORE", filters=FILTERS)
    else:
        purge_file(farm_id + ".h5")
        h5file = tables.open_file(farm_id + ".h5", "w", driver="H5FD_CORE")

    group_f = h5file.create_group("/", "resolution_f", 'raw data')
    group_m = h5file.create_group("/", "resolution_m", 'resolution per month')
    group_w = h5file.create_group("/", "resolution_w", 'resolution per week')
    group_d = h5file.create_group("/", "resolution_d", 'resolution per day')
    group_h = h5file.create_group("/", "resolution_h", 'resolution per hour')
    group_h_h = h5file.create_group("/", "resolution_h_h", 'resolution per 30 minutes')

    table_f = h5file.create_table(group_f, "data", Animal, "Animal data in full resolution")
    table_m = h5file.create_table(group_m, "data", Animal2, "Animal data activity level averaged by month")
    table_w = h5file.create_table(group_w, "data", Animal2, "Animal data activity level averaged by week")
    table_d = h5file.create_table(group_d, "data", Animal2, "Animal data activity level averaged by day")
    table_h = h5file.create_table(group_h, "data", Animal2, "Animal data activity level averaged by hour")
    table_h_h = h5file.create_table(group_h_h, "data", Animal2, "Animal data activity level averaged by 30 minutes")
    cpt_animal_group, cpt_individual = 0, 0
    for animal_group in animal_list_grouped_by_serialn:
        cpt_animal_group += 1
        animal_group_s = sorted([animal_group], key=lambda x: x[0])
        activity_level_array_h_h, activity_level_array_h, activity_level_array_d, activity_level_array_w, activity_level_array_m = [], [], [], [], []
        battery_voltage_array_h_h, battery_voltage_array_h, battery_voltage_array_d, battery_voltage_array_w, battery_voltage_array_m = [], [], [], [], []
        signal_strength_array_h_h, signal_strength_array_h, signal_strength_array_d, signal_strength_array_w, signal_strength_array_m = [], [], [], [], []
        time_initial_h_h, time_initial_h, time_initial_d, time_initial_w, time_initial_m, time_next = 0, 0, 0, 0, 0, 0
        time_initial_s_h_h, time_initial_s_h, time_initial_s_d, time_initial_s_w, time_initial_s_m, time_next_s = False, False, False, False, False, False
        for individual in animal_group_s:
            cpt_individual += 1
            cpt_record = 0
            print(str(cpt_animal_group) + "/" + str(len(animal_list_grouped_by_serialn)) + " " + get_elapsed_time_string(start_time, time.time()) + " ...")
            for record in individual:
                cpt_record += 1
                #print(str(cpt_animal_group) + "/" + str(len(animal_list_grouped_by_serialn)) + " " + str(cpt_record) + "/" + str(len(individual)) + " " + get_elapsed_time_string(start_time, time.time()) + " ...")
                add_record_to_table_single(table_f, record[0], record[1], record[2], record[3], record[4])
                if time_initial_s_h_h is False:
                    time_initial_s_h_h = True
                    time_initial_h_h = record[0]
                if time_initial_s_h is False:
                    time_initial_s_h = True
                    time_initial_h = record[0]
                if time_initial_s_d is False:
                    time_initial_s_d = True
                    time_initial_d = record[0]
                if time_initial_s_w is False:
                    time_initial_s_w = True
                    time_initial_w = record[0]
                if time_initial_s_m is False:
                    time_initial_s_m = True
                    time_initial_m = record[0]
                time_next = record[0]
                activity_level_array_h_h.append(record[4])
                battery_voltage_array_h_h.append(record[3])
                signal_strength_array_h_h.append(record[2])
                activity_level_array_h.append(record[4])
                battery_voltage_array_h.append(record[3])
                signal_strength_array_h.append(record[2])
                activity_level_array_d.append(record[4])
                battery_voltage_array_d.append(record[3])
                signal_strength_array_d.append(record[2])
                activity_level_array_w.append(record[4])
                battery_voltage_array_w.append(record[3])
                signal_strength_array_w.append(record[2])
                activity_level_array_m.append(record[4])
                battery_voltage_array_m.append(record[3])
                signal_strength_array_m.append(record[2])

                elapsed_days_h_h = get_elapsed_minutes(time_initial_d, time_next)
                if elapsed_days_h_h > 30:
                    time_initial_s_h = False
                    activity_level_array_h_h, battery_voltage_array_h_h, signal_strength_array_h_h = [], [], []
                if elapsed_days_h_h == 30:
                    time_initial_s_h_h = False
                    signal_strength_min = min(signal_strength_array_h_h)
                    signal_strength_max = max(signal_strength_array_h_h)
                    battery_voltage_min = min(battery_voltage_array_h_h)
                    activity_level_avg = sum(activity_level_array_h_h)
                    add_record_to_table_sum(table_h_h, time_initial_h_h,  record[1], signal_strength_max, signal_strength_min, battery_voltage_min, activity_level_avg)
                    activity_level_array_h_h, battery_voltage_array_h_h, signal_strength_array_h_h = [], [], []

                elapsed_days_h = get_elapsed_hours(time_initial_d, time_next)
                if elapsed_days_h > 1:
                    time_initial_s_h = False
                    activity_level_array_h, battery_voltage_array_h, signal_strength_array_h = [], [], []
                if elapsed_days_h == 1:
                    time_initial_s_h = False
                    signal_strength_min = min(signal_strength_array_h)
                    signal_strength_max = max(signal_strength_array_h)
                    battery_voltage_min = min(battery_voltage_array_h)
                    activity_level_avg = sum(activity_level_array_h)
                    add_record_to_table_sum(table_h, time_initial_h,  record[1], signal_strength_max, signal_strength_min, battery_voltage_min, activity_level_avg)
                    activity_level_array_h, battery_voltage_array_h, signal_strength_array_h = [], [], []

                # elapsed_days_d = get_elapsed_days(time_initial_d, time_next)
                # if elapsed_days_d > 1:
                #     time_initial_s_d = False
                #     activity_level_array_d, battery_voltage_array_d, signal_strength_array_d = [], [], []
                if not is_same_day(time_initial_d, time_next):
                    time_initial_s_d = False
                    signal_strength_min = min(signal_strength_array_d)
                    signal_strength_max = max(signal_strength_array_d)
                    battery_voltage_min = min(battery_voltage_array_d)
                    activity_level_avg = sum(activity_level_array_d)
                    add_record_to_table_sum(table_d, time_initial_d,  record[1], signal_strength_max, signal_strength_min, battery_voltage_min, activity_level_avg)
                    activity_level_array_d, battery_voltage_array_d, signal_strength_array_d = [], [], []

                elapsed_days_w = get_elapsed_days(time_initial_w, time_next)
                if elapsed_days_w > 7:
                    time_initial_s_w = False
                    activity_level_array_w, battery_voltage_array_w, signal_strength_array_w = [], [], []
                if elapsed_days_w == 7:
                    time_initial_s_w = False
                    signal_strength_min = min(signal_strength_array_w)
                    signal_strength_max = max(signal_strength_array_w)
                    battery_voltage_min = min(battery_voltage_array_w)
                    activity_level_avg = sum(activity_level_array_w)
                    add_record_to_table_sum(table_w, time_initial_w,  record[1], signal_strength_max, signal_strength_min, battery_voltage_min, activity_level_avg)
                    activity_level_array_w, battery_voltage_array_w, signal_strength_array_w = [], [], []

                elapsed_days_m = get_elapsed_days(time_initial_m, time_next)
                if elapsed_days_m > 30:
                    time_initial_s_m = False
                    activity_level_array_m, battery_voltage_array_m, signal_strength_array_m = [], [], []
                if elapsed_days_m == 30:
                    time_initial_s_m = False
                    signal_strength_min = min(signal_strength_array_m)
                    signal_strength_max = max(signal_strength_array_m)
                    battery_voltage_min = min(battery_voltage_array_m)
                    activity_level_avg = sum(activity_level_array_m)
                    add_record_to_table_sum(table_m, time_initial_m,  record[1], signal_strength_max, signal_strength_min, battery_voltage_min, activity_level_avg)
                    activity_level_array_m, battery_voltage_array_m, signal_strength_array_m = [], [], []

    print("finished processing raw file.")


def generate_raw_file(farm_id):
    print("start generating raw file...")
    rows = 0
    farm_count = 0
    FILTERS = tables.Filters(complib='blosc', complevel=9)
    compression = False
    h5file = None
    farm_count += 1
    print("farm id :"+farm_id)
    if compression:
        purge_file(farm_id+"_data_compressed_blosc_raw.h5")
        h5file = tables.open_file(farm_id+"_data_compressed_blosc_raw.h5", "w", driver="H5FD_CORE", filters=FILTERS)
    else:
        purge_file(farm_id+"_raw.h5")
        h5file = tables.open_file(farm_id+"_raw.h5", "w", driver="H5FD_CORE")

    group_f = h5file.create_group("/", "resolution_f", 'raw data')
    table_f = h5file.create_table(group_f, "data", Animal, "Animal data in full resolution")

    db = client[farm_id]
    colNames = db.list_collection_names()
    colNames.sort()
    collection_count = 0

    for collection in colNames:
        collection_count += 1
        animals = db[collection].find_one()["animals"]

        for animal in animals:
            tag_data_raw = animal["tag_data"]
            #removes duplicates
            tag_data = [i for n, i in enumerate(tag_data_raw) if i not in tag_data_raw[n + 1:]]
            add_record_to_table(table_f, tag_data)
            rows += len(tag_data)

        del animals
        print(str(farm_count) + "/" + str(len(db_names)) + " " + str(collection_count) + "/" + str(len(colNames)) + " " + collection + "...")
        # if cpt >= 1:
        #     break

    print("finished added %s rows to pytable" % str(rows))
    print("finished generating raw file.")


if db_type == 0:
    farms = by_size(db_names, 11)
    for farm_id in farms:
        if os.path.isfile("C:\\Users\\fo18103\PycharmProjects\mongo2pytables\src\\"+farm_id+"_raw.h5"):
            print("exist.")
            process_raw_file(farm_id)
        else:
            print("does not exist.")
            generate_raw_file(farm_id)
            process_raw_file(farm_id)


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

        for collection_names_in_day in colNames:
            collection = db[collection_names_in_day]
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
            print(str(collection_count) + "/" + str(len(colNames)) + " " + collection_names_in_day + "...")
    print("finished added %s rows to cassandra" % str(rows))
