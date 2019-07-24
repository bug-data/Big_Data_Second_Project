from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from influxdb import InfluxDBClient


def read_json_file(x):
    data = json.loads(x[1])

    return ((data["cod_nodo"]),(data["id_dato"],data["data_ora"],1,float(data["soil_0_water_media"]),float(data["soil_0_temp_media"]),float(data["soil_1_water_media"]),float(data["soil_1_temp_media"]) ))

def reduce_json(x,y):
    return (y[0],y[1],y[2]+x[2], y[3]+x[3], y[4]+x[4], y[5]+x[5], y[6]+x[6])

def map_save_influx(x):
    data=x[1]
    json_body = [
        {
            "measurement": "Nodi",
            "tags": {
                "brushId": data[0],
                "cod_nodo": x[0]
            },
            "time": data[1],
            "fields": {
                "soil_0_water_media": (data[3] / data[2]),
                "soil_0_temp_media": (data[4] / data[2]),
                "soil_1_water_media": (data[5] / data[2]),
                "soil_1_temp_media": (data[6] / data[2])
            }
        }
    ]
    client.write_points(json_body)

    return x
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)

    kvs = KafkaUtils.createDirectStream(ssc, ["node1","node2", "node3", "node4", "node5", "node7", "node8", "node9", "node10"], {"metadata.broker.list": "kafka:9092"})

    client = InfluxDBClient(host='influx', port=8086)
    client.switch_database('pantheon')
    lines = kvs.map(lambda x: read_json_file(x)).reduceByKey(lambda x,y: reduce_json(x,y)).map(lambda x: map_save_influx(x))

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()

