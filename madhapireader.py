"""
docs here later
"""

import dateutil
import datetime
import madhapi_api
import madrigalWeb.madrigalWeb
import time
import fnmatch
import populateMadHAPI
import urllib.request

# tmp only
import ssl


def do_data_madrigal(
    id: str,
    timemin: str,
    timemax: str,
    parameters: list[str],

    catalog=None,
    floc=None,

    stream_flag=False,
    stream=None,
) -> tuple[int, str]:
    """
    do_data function like csv and supermag example
    """
    timemin = dateutil.parser.parse(timemin)
    startDT = timemin.replace(tzinfo=datetime.timezone.utc)
    timemax = dateutil.parser.parse(timemax)
    endDT = timemax.replace(tzinfo=datetime.timezone.utc)

    kinst, kindat = madhapi_api.madhapiID_toMadrigalID(id)

    # will want kinst/kindat in order to map parms
    # map parameters to dict
    madParms = madhapi_api.map_parms(kinst, kindat, parameters)
    # do something abt filter list?
    filterList = [] # FIX ME?

    if stream_flag:
        # FIX ME: streaming data
        # not fully tested
        datastr, stream = populateMadHAPI.generate_data_isprint(startDT,
                                          endDT,
                                          kinst,
                                          kindat,
                                          madParms,
                                          filterList,
                                          stream_flag,
                                          stream)
    else:

        query = populateMadHAPI.SQLMAD + "/getHAPIService.py?"
        query += "startDT=" + startDT.strftime("%Y-%m-%dT%H:%M:%SZ") + "&"
        query += "endDT=" + endDT.strftime("%Y-%m-%dT%H:%M:%SZ") + "&"
        query += "kinst=" + str(kinst) + "&"
        query += "kindat=" + str(kindat) + "&"
        for parm in madParms:
            query += "madParms=" + parm + "&"

        # tmp only
        context = ssl._create_unverified_context()

        with urllib.request.urlopen(query, context=context) as q:
            if q.status == 200:
                datastr = q.read()
                datastr = str(datastr.decode("utf-8"))

                thisDataFile = populateMadHAPI.get_data(id, datastr)
        # datastr = populateMadHAPI.generate_data_pandas(startDT,
        #                                   endDT,
        #                                   kinst,
        #                                   kindat,
        #                                   madParms)

    # use id to generate info records
    # also need parms
    
    thisInfoFile = populateMadHAPI.generate_info_json(id, startDT, endDT, madParms)

    # at this point, we have created an info record that has
    # already converted time parms as necessary (as far as metadata is concerned)
    if datastr is not None:
        status = 1200

    if len(datastr) == 0:
        status = 1201  # status 1200 is HAPI "OK- no data for time range"

    return status, datastr



    


