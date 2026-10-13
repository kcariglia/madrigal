"""
Defines the catalog() info() and data() functions needed for the latest version
of the Madrigal-HAPI server. 

K Cariglia
8/26/26
"""
# standard python imports
import datetime
import dateutil
import json
import os, os.path
import traceback
import urllib

# third party imports 
import pandas

# madrigal imports
import madrigal.metadata

MADHAPI_HOME = "/opt/madrigal/home_madtest" # change me
MADRIGAL = "http://127.0.0.1:9000/" # madtest

def catalog(depth=None, dataset=None):
    """
    Return the HAPI catalog response
    """

    madhapi_catalog = os.path.join(MADHAPI_HOME, "catalog.json")

    with open(madhapi_catalog, "r") as f:
        catalog_dict = json.load(f)

    catalog_response = []

    try:
        if depth == 'all':
            # attach info dict
            if dataset is not None:
                # TODO: want master list of 
                # all possible dataset id's for easy indexing
                available_datasets = [entry['id'] for entry in catalog_dict['catalog']]
                catalog_index = available_datasets.index(dataset)
                return(catalog_dict['catalog'][catalog_index])
            else:
                return(catalog_dict['catalog'])
        else:
            # omit info dict
            catalog_response = [{'id': entry['id'], 'title': entry['title']} for entry in catalog_dict['catalog']]
            return(catalog_response)
    except:
        traceback.print_exc()


def info(dataset_id):
    """
    Return the HAPI info response
    """
    # first check if info obj we want already exists. if not,
    # generate it
    try:
        thisInfoFile = os.path.join(MADHAPI_HOME, "info") + "/" + dataset_id + ".json"
        if os.path.exists(thisInfoFile):
            with open(thisInfoFile, "r") as f:
                info_dict = json.load(f)
                return(info_dict)
        else:
            kinst, kindat = _get_kinst_kindat(dataset_id)

            # how do account for time boundaries? parms per file?
            #TODO
            print("AAAAAAAAAAAAAAAA")
            pass
    except:
        traceback.print_exc()



def data(dataset_id, parameters, start, stop, format="csv"):
    """
    Return the HAPI data response
    """
    start = dateutil.parser.parse(start)
    stop = dateutil.parser.parse(stop)
    startDT = start.replace(tzinfo=datetime.timezone.utc)
    endDT = stop.replace(tzinfo=datetime.timezone.utc)
    kinst, kindat = _get_kinst_kindat(dataset_id)
    #print(f"DEBUG: parameters came in as {parameters}. is it none? {not parameters}")
    madParms = _map_parms(kinst, kindat, parameters)
    #print(f"DEBUG: madParms are {madParms}")

    query = MADRIGAL + "/getHAPIService.py?"
    query += "startDT=" + startDT.strftime("%Y-%m-%dT%H:%M:%SZ") + "&"
    query += "endDT=" + endDT.strftime("%Y-%m-%dT%H:%M:%SZ") + "&"
    query += "kinst=" + str(kinst) + "&"
    query += "kindat=" + str(kindat) + "&"
    for parm in madParms:
        query += "madParms=" + parm + "&"

    with urllib.request.urlopen(query) as q:
        if q.status == 200:
            datastr = q.read()
            data = datastr.decode("utf-8")

    return data


def _get_kinst_kindat(dataset_id):
    """
    Convert HAPI dataset ID to Madrigal kinst, kindat pair
    """
    try:
        kinst, kindat = dataset_id.split("_")
        int(kinst) 
        int(kindat)
        return(kinst, kindat)
    except:
        # invalid ID
        raise ValueError(f"Invalid ID: {dataset_id}")


def _map_parms(kinst, kindat, parameters):
    """
    Convert HAPI request parameters to Madrigal standard
    parameters
    """
    standardTimeParms = ['year', 'month', 'day', 'hour', 'min', 'sec', 'ut1_unix']

    if not parameters:
        # read parameters from catalog
        madDB = madrigal.metadata.MadrigalDB()
        madhapi_hdf_catalog = os.path.join(madDB.getMetadataDir(), "madhapi.hdf5")
        catalogDF = pandas.read_hdf(madhapi_hdf_catalog, key="catalog")
        catalogDF = catalogDF.reset_index(drop=True) 
        catalogIdx = catalogDF[(catalogDF['kinst'] == int(kinst)) & (catalogDF['kindat'] == int(kindat))].index
        strParms = catalogDF["parms"][catalogIdx].to_string()
        catalogParms = strParms[strParms.find("[")+1:-1].replace("'", "").split(",")
        print(f"parms were {catalogDF["parms"][catalogIdx].to_string()} and are now {catalogParms}")
        parmList = [parm.replace(" ", "") for parm in catalogParms]
        parms = standardTimeParms + parmList
        parms = [parm.lower() for parm in parms]
        return(parms)
    else:
        # ensure parameters is a list
        if not type(parameters) == list:
            parameters = [parameters]

        if "Time" in parameters:
            parameters.remove("Time")
        
        # return a list of desired madrigal parm mnems
        parms = standardTimeParms + parameters
        parms = [parm.lower() for parm in parms]
        return(parms)
