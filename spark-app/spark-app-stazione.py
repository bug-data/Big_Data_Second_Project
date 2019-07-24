from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from influxdb import InfluxDBClient


def read_json_file(x):
    data = json.loads(x[1])
    rad_Wmq = 0
    rad_Wmq_coun = 0
    if 'rad W/mq' in data:
        rad_Wmq = float(data["rad W/mq"])
        rad_Wmq_coun = 1

    wind_speed_avg = 0
    wind_speed_avg_count = 0
    if 'wind_speed_media' in data:
        wind_speed_avg = float(data["wind_speed_media"])
        wind_speed_avg_count = 1

    wind_speed_max = 0
    wind_speed_max_count = 0
    if 'wind_speed_max' in data:
        wind_speed_max = float(data["wind_speed_max"])
        wind_speed_max_count = 1

    return ((1), (data["id_dato"], data["data_ora"], 1, float(data["temp1_media"]), float(data["temp1_min"]),
                  float(data["temp1_max"]), float(data["ur1_media"]), float(data["ur1_min"]), float(data["ur1_max"]),
                  float(data["pioggia_mm"]), rad_Wmq, rad_Wmq_coun, float(data["wind_dir"]), wind_speed_avg,
                  wind_speed_avg_count, wind_speed_max, wind_speed_max_count, float(data["pressione_mbar"]),
                  float(data["pressione_standard_mbar"]), float(data["temp1_min"]), float(data["temp1_max"]) ))


def reduce_json(x, y):
    temp_min = x[19]
    if temp_min>=y[19]:
        temp_min=y[19]

    temp_max = x[20]
    if temp_max <= y[20]:
        temp_max = y[20]
        

    return (
    y[0], y[1], y[2] + x[2], y[3] + x[3], y[4] + x[4], y[5] + x[5], y[6] + x[6], y[7] + x[7], y[8] + x[8], y[9] + x[9],
    y[10] + x[10], y[11] + x[11], y[12] + x[12], y[13] + x[13], y[14] + x[14], y[15] + x[15], y[16] + x[16],
    y[17] + x[17], y[18] + x[18], temp_min, temp_max)


def map_save_influx(x):
    data = x[1]
    count_11 = data[11]
    if data[11] == 0:
        count_11 = 1.0
    count_14 = data[14]
    if data[14] == 0:
        count_14 = 1.0
    count_16 = data[16]
    if data[16] == 0:
        count_16 = 1.0
    json_body = [
        {
            "measurement": "Stazione",
            "tags": {
                "brushId": data[0]
            },
            "time": data[1],
            "fields": {
                "temp1_media": (data[3] / data[2]),
                "temp1_min": (data[4] / data[2]),
                "temp1_max": (data[5] / data[2]),
                "ur1_media": (data[6] / data[2]),
                "ur1_min": (data[7] / data[2]),
                "ur1_max": (data[8] / data[2]),
                "pioggia_mm": (data[9] / data[2]),
                "rad_wmq": (data[10] / count_11),
                "wind_dir": (data[12] / data[2]),
                "wind_speed_media": (data[13] / count_14),
                "wind_speed_max": (data[15] / count_16),
                "pressione_mbar": (data[17] / data[2]),
                "pressione_standard_mbar": (data[18] / data[2]),
                "escursione_termica": (data[20]-data[19])
            }
        }
    ]
    client.write_points(json_body)
    return x


if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)

    kvs = KafkaUtils.createDirectStream(ssc, ["weather"], {"metadata.broker.list": "kafka:9092"})

    client = InfluxDBClient(host='influx', port=8086)
    client.switch_database('pantheon')
    lines = kvs.map(lambda x: read_json_file(x)).reduceByKey(lambda x, y: reduce_json(x, y)).map(
        lambda x: map_save_influx(x))

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()

