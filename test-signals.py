import bw2data as bd
from blinker import signal

# dl stands for data lineage
# bw2data has to be installed so far from branch data_lineage e.g. with:
# pip install -e git+https://github.com/cswh/brightway2-data.git@data_lineage

def dl_emit_database_written(data):
    print(data)
    "TODO: something meaningful with client.emit"

def dl_emit_activity_saved(activity_dataset):
    print(activity_dataset)
    "TODO: something meaningful with client.emit"

def dl_emit_exchange_saved(exchange_dataset):
    print(exchange_dataset)
    "TODO: something meaningful with client.emit"

signal("bw2data.database_written").connect(dl_emit_database_written)
signal("bw2data.activity_saved").connect(dl_emit_activity_saved)
signal("bw2data.exchange_saved").connect(dl_emit_exchange_saved)

# TODO: test with ecospold_importer
# TODO: test with premise
# TODO: test with act.save()