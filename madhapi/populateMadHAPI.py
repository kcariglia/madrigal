"""
Populate the Madrigal-HAPI server catalog.

K Cariglia, 10/13/26
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

SQLMAD = "http://127.0.0.1:9000/"

def generate_madhapi_hdf_catalog_by_category(category, startDT, endDT):
    """
    creates a database of:
    filename: {startDT, stopDT, madParmList}
    kinst: {kindat: {startDT, stopDT, parmSet}}

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

    madhapi_fname_dict = {} # fname: startDT, endDT, parmList
    madhapi_catalog_dict = {} # kinst: kindat: startDT, stopDT, parmSet
    for kinst in kinstList:
        # find data from madrigal
        madDB = madrigal.metadata.MadrigalDB()
        expFileList = madDB.getFileListFromMetadata(kinstList=[kinst],
                                                    startDate=startDT,
                                                    endDate=endDT)

        if kinst not in madhapi_catalog_dict.keys():
            madhapi_catalog_dict[kinst] = {}
        print(f"expFileList is {len(expFileList)} files long")
        for thisFile in expFileList:
            thisFileName = thisFile
            metaFileName = thisFile
            #print(f"file is {thisFile}")
            fullFileName = thisFileName.replace("/opt/openmadrigal_sql/madroot/experiments", "/data/cloud1/geospace/madrigal/experiments")
            if fullFileName not in madhapi_fname_dict.keys():
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
                thisFileParms = sorted([madParmInfo.getParmMnemonic(parm).lower() for parm in thisFileParms])
                # NOW we have mnems

                madhapi_fname_dict[fullFileName] = (thisFileStart, thisFileEnd, thisFileParms)

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
                    madhapi_catalog_dict[kinst][thisFileKindat][2] = sorted(list(set(madhapi_catalog_dict[kinst][thisFileKindat][2] + thisFileParms)))

    final_fname_dict = {
        "file" : madhapi_fname_dict.keys(),
        "start" : [madhapi_fname_dict[file][0] for file in madhapi_fname_dict.keys()],
        "end" : [madhapi_fname_dict[file][1] for file in madhapi_fname_dict.keys()],
        "parms" : [madhapi_fname_dict[file][2] for file in madhapi_fname_dict.keys()]
    }
    final_catalog_dict = {
        "kinst" : [],
        "kindat" : [],
        "start" : [],
        "end" : [],
        "parms" : []
    }
    for kinst in madhapi_catalog_dict.keys():
        for kindat in madhapi_catalog_dict[kinst].keys():
            try:
                sDT = madhapi_catalog_dict[kinst][kindat][0]
                eDT = madhapi_catalog_dict[kinst][kindat][1]
                parms = madhapi_catalog_dict[kinst][kindat][2]
                if not parms:
                    continue
                final_catalog_dict["kinst"].append(kinst)
                final_catalog_dict["kindat"].append(kindat)
                final_catalog_dict["start"].append(sDT)
                final_catalog_dict["end"].append(eDT)
                final_catalog_dict["parms"].append(parms)
            except:
                continue

    madhapi_fname_df = pandas.DataFrame(final_fname_dict)
    madhapi_catalog_df = pandas.DataFrame(final_catalog_dict)
    # ensure parm column is string
    madhapi_fname_df["parms"] = madhapi_fname_df["parms"].astype(str)
    madhapi_catalog_df["parms"] = madhapi_catalog_df["parms"].astype(str)
    madhapi_fname_df.to_hdf(os.path.join(madDB.getMetadataDir(), "madhapi.hdf5"), key="files", format="table", append=True, min_itemsize={"file":200, "parms":250})
    madhapi_catalog_df.to_hdf(os.path.join(madDB.getMetadataDir(), "madhapi.hdf5"), key="catalog", format="table", append=True, min_itemsize={"parms":250})
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
    catalogDF = catalogDF.reset_index(drop=True)
    filesDF = filesDF.reset_index(drop=True)
    #catalogDict = catalogDF.to_dict() # kinst: kindat: startDT, stopDT, parmSet
    #filesDict = filesDF.to_dict() # fname: startDT, endDT, parmList
    print(catalogDF.info())
    print(catalogDF.dtypes)
    print(filesDF.info())
    print(filesDF.dtypes)
    print(catalogDF)
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
    for idx in range(len(catalogDF)):
        thisCatalogDict = {}
        print(f"kinst is {catalogDF["kinst"][idx]} and kindat is {catalogDF["kindat"][idx]}")
        kinst = int(catalogDF["kinst"][idx].astype(int))
        kindat = int(catalogDF["kindat"][idx].astype(int))
        sDT = catalogDF["start"][idx]
        eDT = catalogDF["end"][idx]
        parms = [parm.replace(" ", "") for parm in str(catalogDF["parms"][idx])[1:-1].replace("'", "").split(",")]
        try:
            kindatDesc = madKindatObj.getKindatDescription(kindat, kinst=kinst)
            if kindatDesc is None:
                # possibly no kinst_kindat combo in db. just look for kindat only
                kindatDesc = madKindatObj.getKindatDescription(kindat)
        except:
            # possibly no kinst_kindat combo in db. just look for kindat only
            kindatDesc = madKindatObj.getKindatDescription(kindat)
        try:
            thisCatalogDict["id"] = str(int(kinst)) + "_" + str(int(kindat))
            thisCatalogDict["title"] = madInstObj.getInstrumentName(kinst) + ", " + kindatDesc
            thisCatalogDict["info"] = {
                    "startDate" : sDT.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "stopDate" : eDT.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "parameters" : parms
                }
        except:
            traceback.print_exc()
            continue # tmp only
        catalogJson["catalog"].append(thisCatalogDict)

    
    thisCatalogFile = os.path.join(madtest_config.HAPI_HOME, "catalog.json")
    with open(thisCatalogFile, "w") as f:
        json.dump(catalogJson, f)
