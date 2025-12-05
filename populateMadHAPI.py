"""
test/starter to generate info json records and corresponding data
"""

import madrigal.metadata
import madhapi_api
import os, os.path
import madtest_config
import datetime
import json
import dateutil
import madrigalWeb.madrigalWeb
import time
import pandas
import h5py
import numpy
import io

def get_data(id, format="csv"):
    """
    assumes the data we want already exists in the 
    /data endpoint. if not, return None
    FIX ME: parms??????
    FIX ME: format???
    FIX ME: stream???
    """
    thisDataFile = os.path.join(madtest_config.HAPI_HOME, "data") + "/" + id + "." + format
    if os.path.exists(thisDataFile):
        with open(thisDataFile, "r") as f:
            data = f.read()
        return(data)
    else:
        return(None)

def generate_info_json(id, madParms):
    """
    generate info record corresponding to dset id and madParms
    """

    # first check if info obj we want already exists. if not,
    # generate it
    thisInfoFile = os.path.join(madtest_config.HAPI_HOME, "info") + "/" + id + ".json"
    if os.path.exists(thisInfoFile):
        return(thisInfoFile)
    
    # madParms come in with time parms

    kinst, kindat = madhapi_api.madhapiID_toMadrigalID(id)
    parmJsonList = madhapi_api.generate_parm_json_headers(madParms)

    # now we want start and stop date for this dataset
    # have it match with instData years for cedar site
    instData = madrigal.metadata.MadrigalInstrumentData()
    instYears = instData.getInstrumentYears(kinst)

    # redo this better later, more specificity by day, FIX ME
    infoStartDate = datetime.datetime(year=instYears[0],
                                      month=1,
                                      day=1,
                                      tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    infoStopDate = datetime.datetime(year=instYears[-1],
                                        month=12,
                                        day=31,
                                        tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    
    infoDict = {
        "HAPI" : madtest_config.HAPI_VERSION,
        "status" : {"code" : 1200, "message" : "OK"},
        "startDate" : infoStartDate,
        "stopDate" : infoStopDate,
        "parameters" : parmJsonList
    }
    with open(thisInfoFile, "w") as f:
        json.dump(infoDict, f)
    return(thisInfoFile)
    
    



def generate_data_isprint(startDT,
                          endDT,
                          kinst,
                          kindat,
                          madParms,
                          filterList,
                          stream_flag=False,
                          stream=None):
    """
    generate/format madrigal data to csv style using isprint
    caution: slow
    """
    # make sure to check whether unix time == ymdhms

    # handle parm filters
    timeList = (startDT.year, startDT.month, startDT.day, 0, 0, 0,
            endDT.year, endDT.month, endDT.day, 23, 59, 59)
    filterStr = ' '.join(filterList)
    # handle the case when an experiment extends beyond the date boundaries, and so 
    # filtering must be done at the isprint level
    if filterStr.find('date1') == -1:
        newFilterStr = filterStr + madhapi_api.getTimeParms(timeList, 1, 0)

    # hardcode a dummy user
    user_fullname = "Madrigal HAPI User"
    user_email = "madrigal@hapi.com"
    user_affiliation = "None"

    # download data from madrigal one file at a time
    # copy logic as in get_madfile_service to avoid actually downloading stuff
    madDB = madrigalWeb.madrigalWeb.MadrigalData("https://cedar.openmadrigal.org")
    matchingExps = madDB.getExperiments(kinst, startDT.year, startDT.month, startDT.day,
                                        startDT.hour, startDT.minute, startDT.second,
                                        endDT.year, endDT.month, endDT.day, endDT.hour,
                                        endDT.minute, endDT.second)
    
    # get list of all experiment files given the expList
    expFileList = madhapi_api.getExperimentFileList(madDB, matchingExps, False)

    # filter expFileList using kindat
    expFileList = madhapi_api.filterExperimentFilesUsingKindat(expFileList, kindat)

    datastr = ""
    status = 0
    for thisFile in expFileList:
        try:
            data = madDB.isprint(thisFile.name,
                                 ','.join(madParms) if len(madParms) > 1 else madParms[0],
                                 newFilterStr,
                                 user_fullname,
                                 user_email,
                                 user_affiliation,
                                 None,
                                 verbose=True)
            
        except:
            # assume isprint timed out - try again by breaking the experiment into pieces
            expTimeList = madhapi_api.getTimesOfExperiment(expFileList, thisFile.expId)
            numIter = 50 # number of pieces to break exp into
            for j in range(numIter):
                newParms = madhapi_api.getTimeParms(expTimeList, numIter, j)
                time.sleep(0.5)
                try:
                    data = madDB.isprint(thisFile.name,
                                  ','.join(madParms) if len(madParms) > 1 else madParms[0],
                                  filterStr + newParms,
                                  user_fullname,
                                  user_email,
                                  user_affiliation,
                                  verbose=True)
                except:
                    print('Failure analyzing file %s with slice %s' % (thisFile.name, newParms))
                    continue

        # here, you have data for one file
        # now reformat it in a way that hapi likes
        # probably need to rework time parms somehow? hapi wants isotime
        data = madhapi_api.cleanDataTime(data)
        datastr += data

        if stream_flag:
            # Write then flush
            stream.wfile.write(bytes(datastr, "utf-8"))
            datastr = ""

    if stream_flag:
        return(datastr, stream)
    else:
        return(datastr)


def generate_data_pandas(startDT,
                          endDT,
                          kinst,
                          kindat,
                          madParms,
                          #filterList, # no filter list needed here
                          stream_flag=False,
                          stream=None):
    """
    generate/format madrigal data to csv style using pandas
    """
    # hardcode a dummy user
    user_fullname = "Madrigal HAPI User"
    user_email = "madrigal@hapi.com"
    user_affiliation = "None"

    # find data from madrigal
    madDB = madrigalWeb.madrigalWeb.MadrigalData("https://cedar.openmadrigal.org")
    matchingExps = madDB.getExperiments(kinst, startDT.year, startDT.month, startDT.day,
                                        startDT.hour, startDT.minute, startDT.second,
                                        endDT.year, endDT.month, endDT.day, endDT.hour,
                                        endDT.minute, endDT.second)
    
    # get list of all experiment files given the expList
    expFileList = madhapi_api.getExperimentFileList(madDB, matchingExps, False)

    # filter expFileList using kindat
    expFileList = madhapi_api.filterExperimentFilesUsingKindat(expFileList, kindat)

    datastr = "" # datastr can literally be treated as csv
    for thisFile in expFileList:
        data = io.StringIO()

        # TMP ONLY: im downloading the file first for local tests
        # in prod, do not download file, just read it directly
        mytempfile = "hapitemp.hdf5"

        madDB.downloadFile(thisFile.name, mytempfile, user_fullname, user_email, user_affiliation, format="hdf5")
        
        with h5py.File(mytempfile, "r") as f:
            thisDF = pandas.DataFrame(numpy.array(f["Data/Table Layout"]), columns=madParms)
            thisDF.to_csv(data)
            datatoadd = data.getvalue()
            datatoadd = madhapi_api.cleanDataTime(datatoadd, isprint=False) # want to do this in a smarter/more efficient way, FIX ME

            datastr += datatoadd

        if stream_flag:
            # Write then flush
            stream.wfile.write(bytes(datastr, "utf-8"))
            datastr = ""

    if stream_flag:
        return(datastr, stream)
    else:
        return(datastr)

