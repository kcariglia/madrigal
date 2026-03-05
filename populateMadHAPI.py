"""
test/starter to generate info json records and corresponding data
"""

import madrigal.metadata
import madrigal.data
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
import traceback

SQLMAD = "https://192.52.65.29"

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

def generate_info_json(id, infoStart, infoStop, madParms):
    """
    generate info record corresponding to dset id, startTime, stopTime, and madParms
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
    #instData = madrigal.metadata.MadrigalInstrumentData()
    #instYears = instData.getInstrumentYears(kinst)

    # redo this better later, more specificity by day, FIX ME
    # infoStartDate = datetime.datetime(year=instYears[0],
    #                                   month=1,
    #                                   day=1,
    #                                   tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    # infoStopDate = datetime.datetime(year=instYears[-1],
    #                                     month=12,
    #                                     day=31,
    #                                     tzinfo=datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    
    infoStartDate = infoStart.strftime("%Y-%m-%dT%H:%M:%S")
    infoStopDate = infoStop.strftime("%Y-%m-%dT%H:%M:%S")

    # want to be able to get extra info needed for SPASE, use id/kinst/kindat

    # fname includes full path to expDir
    #madhapi_fname_dict = {} # fname: startDT, endDT, parmList
    #madhapi_catalog_dict = {} # kinst: kindat: startDT, stopDT, parmSet

    infoDict = {

        # mandatory info response attributes
        "HAPI" : madtest_config.HAPI_VERSION,
        "status" : {"code" : 1200, "message" : "OK"},
        "startDate" : infoStartDate,
        "stopDate" : infoStopDate,
        "parameters" : parmJsonList

        # optional info response attributes
        #"cadence" : what if its irregular?????
        #"description" : ?????
        #"location" : # instrument location from metadata
        #"resourceURL" : # use the w3id link? - only works at experiment level
        #"creationDate" : # get this from fileTab? use os
        #"modificationDate" : # get this from fileTab metadata
        #"contact" : # use inst + exp pi
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
    madDB = madrigalWeb.madrigalWeb.MadrigalData(SQLMAD)
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
    madDB = madrigalWeb.madrigalWeb.MadrigalData(SQLMAD)
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
            # what's the biggest piece of this numpy array we can read at a time
            # if it is too big to read in one go?
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



def generate_madhapi_hdf_catalog_by_category(category=14):
    """
    will take a very long time probably.

    creates a database of:
    filename: {startDT, stopDT, madParmList}

    assumes existence/installation of sqlmad metadata db

    """
    try:
        import sqlite3
        import madrigal.data
        import madrigal.metadata

        madDB = madrigal.metadata.MadrigalDB()

        os.access(os.path.join(madDB.getMetadataDir(), "metadata.db"), os.R_OK)
    except:
        raise ImportError("Need to install sqlmad first")

    # for starters, lets do only magnetometers.
    # magnetometers = instType 14
    # get every file associated with every magnetometer 2000 - 2025
    startDT = datetime.datetime(year=2000, month=1, day=1)
    endDT = datetime.datetime(year=2025, month=12, day=31, hour=23, minute=59, second=59)

    # first get list of kinsts in this inst category
    query = f"SELECT kinst FROM instTab WHERE category={category}"

    try:
        connector = sqlite3.connect(os.path.join(madDB.getMetadataDir(), "metadata.db"))
        cursor = connector.cursor()

        result = cursor.execute(query)
        resList = result.fetchall()
        kinstList = [item[0] for item in resList]

        connector.close()
    except:
        traceback.print_exc()
        connector.close()

    # lets do a couple different dictionaries.
    madhapi_fname_dict = {} # fname: startDT, endDT, parmList
    madhapi_catalog_dict = {} # kinst: kindat: startDT, stopDT, parmSet
    for kinst in kinstList:
        # find data from madrigal
        madWebDB = madrigalWeb.madrigalWeb.MadrigalData("https://cedar.openmadrigal.org")

        try:
            matchingExps = madWebDB.getExperiments(kinst, startDT.year, startDT.month, startDT.day,
                                                startDT.hour, startDT.minute, startDT.second,
                                                endDT.year, endDT.month, endDT.day, endDT.hour,
                                                endDT.minute, endDT.second)
        except:
            # probably timed out, try again
            try:
                matchingExps = madWebDB.getExperiments(kinst, startDT.year, startDT.month, startDT.day,
                                                startDT.hour, startDT.minute, startDT.second,
                                                endDT.year, endDT.month, endDT.day, endDT.hour,
                                                endDT.minute, endDT.second)
            except:
                # skip this kinst for now
                print(f"skipping kinst {kinst}")
                continue
        
        # get list of all experiment files given the expList
        expFileList = madhapi_api.getExperimentFileList(madWebDB, matchingExps, False)

        # omit this part, we want all kindats
        # filter expFileList using kindat
        #expFileList = madhapi_api.filterExperimentFilesUsingKindat(expFileList, kindat)

        if kinst not in madhapi_catalog_dict.keys():
            madhapi_catalog_dict[kinst] = {}
        print(f"expFileList is {len(expFileList)} files long")
        for thisFile in expFileList:
            thisFileName = thisFile.name.replace("/opt/openmadrigal/madroot/experiments", "/data/cloud1/geospace/madrigal/experiments")
            metaFileName = thisFile.name.replace("openmadrigal", "openmadrigal_sql")
            if thisFileName not in madhapi_fname_dict.keys():
                try:
                    madFileObj = madrigal.data.MadrigalFile(thisFileName)
                    madMetaFileObj = madrigal.metadata.MadrigalMetaFile(madDB, initFile=os.path.join(os.path.dirname(metaFileName), "fileTab.txt"))
                    # found this experiment
                except:
                    # couldn't find this experiment or file, try the next
                    traceback.print_exc()
                    print(f"filename is {thisFileName}, meta dir is {os.path.dirname(metaFileName)}")
                    continue

                madParmInfo = madrigal.data.MadrigalParameters()
                thisFileStart = datetime.datetime(*madFileObj.getEarliestTime())
                thisFileEnd = datetime.datetime(*madFileObj.getLatestTime())
                # note parms are PARM CODES, not mnems
                thisFileParms = madFileObj.getMeasuredParmList()
                thisFileParms = [madParmInfo.getParmMnemonic(parm) for parm in thisFileParms]
                # NOW we have mnems

                madhapi_fname_dict[thisFileName] = (thisFileStart, thisFileEnd, thisFileParms)

                thisFileKindat = madMetaFileObj.getKindatByFilename(thisFileName)
                if thisFileKindat not in madhapi_catalog_dict[kinst].keys():
                    madhapi_catalog_dict[kinst][thisFileKindat] = [thisFileStart, thisFileEnd, thisFileParms]
                else:
                    # check start time
                    if thisFileStart < madhapi_catalog_dict[kinst][thisFileKindat][0]:
                        madhapi_catalog_dict[kinst][thisFileKindat][0] = thisFileStart
                    # check end time
                    if thisFileEnd > madhapi_catalog_dict[kinst][thisFileKindat][1]:
                        madhapi_catalog_dict[kinst][thisFileKindat][1] = thisFileEnd
                    # conglomerate parms
                    madhapi_catalog_dict[kinst][thisFileKindat][2] = list(set(madhapi_catalog_dict[kinst][thisFileKindat][2] + thisFileParms))

    madhapi_fname_df = pandas.DataFrame.from_dict(madhapi_fname_dict)
    madhapi_catalog_df = pandas.DataFrame.from_dict(madhapi_catalog_dict)
    madhapi_fname_df.to_hdf(os.path.join(madDB.getMetadataDir(), "madhapi.hdf5"), key="files")
    madhapi_catalog_df.to_hdf(os.path.join(madDB.getMetadataDir(), "madhapi.hdf5"), key="catalog")
    print(f"done creating hdf5 catalog at {datetime.datetime.now()}")


def generate_madhapi_catalog_json():
    """
    generate the catalog.json file for the hapi server, reading
    from the hdf5 catalog
    """
    madDB = madrigal.metadata.MadrigalDB()
    madInstObj = madrigal.metadata.MadrigalInstrument(madDB)
    madKindatObj = madrigal.metadata.MadrigalKindat(madDB)
    madhapi_hdf_catalog = os.path.join(madDB.getMetadataDir(), "madhapi.hdf5")
    catalogDF = pandas.read_hdf(madhapi_hdf_catalog, key="catalog") 
    filesDF = pandas.read_hdf(madhapi_hdf_catalog, key="files")
    catalogDict = catalogDF.to_dict() # kinst: kindat: startDT, stopDT, parmSet
    filesDict = filesDF.to_dict() # fname: startDT, endDT, parmList

    # catalog response should generate id, title, info_json
    # id is kinst_kindat, title is "{kinst.name};{kindat.name}"
    catalogJson = {
        "HAPI": madtest_config.HAPI_VERSION,
        "catalog": [],
        "status": {
            "code": 1200,
            "message": "OK request successful"
        }
    }
    for kinst in catalogDict.keys():
        for kindat in catalogDict[kinst].keys():
            thisCatalogDict = {}
            thisCatalogDict["id"] = str(kinst) + "_" + str(kindat)

            try:
                kindatDesc = madKindatObj.getKindatDescription(kindat, kinst=kinst)
                if kindatDesc is None:
                    # possibly no kinst_kindat combo in db. just look for kindat only
                    kindatDesc = madKindatObj.getKindatDescription(kindat)
            except:
                # possibly no kinst_kindat combo in db. just look for kindat only
                kindatDesc = madKindatObj.getKindatDescription(kindat)
            thisCatalogDict["title"] = madInstObj.getInstrumentName(kinst) + ", " + kindatDesc

            # generate info responses
            thisInfoFile = generate_info_json(thisCatalogDict["id"], catalogDict[kinst][kindat][0], catalogDict[kinst][kindat][1], catalogDict[kinst][kindat][2])

            #with open(thisInfoFile, "r") as f:
            #    infoDict = json.load(f)

            thisCatalogDict["info"] = {
                "startDate" : catalogDict[kinst][kindat][0],
                "stopDate" : catalogDict[kinst][kindat][1],
                "parameters" : catalogDict[kinst][kindat][2]
            }
            catalogJson["catalog"].append(thisCatalogDict)

    thisCatalogFile = os.path.join(madtest_config.HAPI_HOME, "catalog.json")
    with open(thisCatalogFile, "w") as f:
        json.dump(catalogJson, f)

