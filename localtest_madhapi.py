from hapiclient import hapi
from hapiplot import hapiplot
import madtest_config
import os, os.path
import datetime
import traceback

esketit = datetime.datetime.now()

server = "http://localhost:8000/hapi"
dataset = "test1"#"ttb/baseline_none/PT1M/XYZ"
start = "2020-05-10T00:00Z"
stop = "2020-05-14T00:00Z"
parameters = ''


DATA_EXISTS = True

# for now i still need to manually edit info.json to get 
# right timestamps, currently says 2016 because of local instData


try:
    if not DATA_EXISTS:
        status, data = madtest_config.hapi_handler(dataset,
                                start,
                                stop,
                                parameters)
        thisDataFile = os.path.join(madtest_config.HAPI_HOME, "data") + "/" + dataset + ".csv"
        with open(thisDataFile, "w") as f:
            f.write(data)
    data, meta = hapi(server, dataset, parameters, start, stop)
except:
    traceback.print_exc()
    

    # try again
    try:
        data, meta = hapi(server, dataset, parameters, start, stop)
    except:
        traceback.print_exc()
        raise ValueError("oh no")

wedone = datetime.datetime.now()
print(f"{(wedone-esketit).seconds} seconds elapsed")
print(meta)
print(data)
hapiplot(data,meta)

