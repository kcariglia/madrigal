"""
some tools needed for the madrigal hapi reader
"""
import time
import madrigal.metadata
import numpy
import datetime
import fnmatch
import math


def getExperimentFileList(server, expList, verbose):
    """
    Taken from madrigalWeb.globalIsprint.py

    getExperimentFileList returns a list of MadrigalExperimentFile objects given an experiment list.

    Inputs::

        server - the active MadrigalData object to get information from
        
        expList - the list of desired MadrigalExperiment objects
        
        verbose - if True, print verbose output

    Returns:

        a list of MadrigalExperimentFile objects
    """
    retList = []

    for i, exp in enumerate(expList):
        time.sleep(0.5)
        if verbose:
            print(('checking experiment %i of %i' % (i,len(expList))))
        try:
            theseExpFiles = server.getExperimentFiles(exp.id)
        except:
            # skip experiments with no files
            continue
        for expFile in theseExpFiles:
            retList.append(expFile)

    return retList


def filterExperimentFilesUsingKindat(expFileList, kindat):
    """
    Taken from madrigalWeb.globalIsprint.py

    filterExperimentFilesUsingKindat returns a subset of the experiment files in expFileList whose kindat is found in kindat argument.

    Input:

        expFileList - a list of MadrigalExperimentFile objects to be filtered

        kindat - the kindat argument passed in by the user - comma separated list of kind of data codes.  If names are given, the
                argument must be enclosed in double quotes.  An asterick will perform matching as in glob.

    Returns:

        a subset of expFileList whose kindat values are accepted
    """
    # FIX ME: kindat list
    kindat = str(kindat)
    strList = kindat.split(',')

    # create lists of kindat ints, kindat names, and kindat regular expressions
    kindatCodeList = []
    kindatNameList = []

    for item in strList:
        try:
            value = int(item)
            kindatCodeList.append(value)
            continue
        except:
            pass
        # a non-integer found
        testName = '*' + item.lower().replace(' ', '_') + '*'
        kindatNameList.append(testName)

    # now loop through each experiment file, and add it to a new list if its accepted
    retList = []
    for expFile in expFileList:
        # code match
        if expFile.kindat in kindatCodeList:
            retList.append(expFile)
            continue
        # description match
        try:
            kindatDesc = expFile.kindatdesc.lower()
        except:
            continue
        kindatDesc = kindatDesc.replace(' ', '_')
        for kindatName in kindatNameList:
            if fnmatch.fnmatch(kindatDesc, kindatName):
                retList.append(expFile)
                break

    return retList


def getTimeParms(expTimeList, numIter, j):
    """
    Taken from madrigalWeb.globalIsprint.py

    getTimeParms creates arguments to be passed to isprint to get only a slice of an experiment's data

        Input:

            expTimeList: a list of experiment start and end times:startyear, startmonth, startday, starthour,
                startmin, startsec, endyear, endmonth, endday, endhour, endmin, endsec

            numIter - the number of pieces to break the experiment into

            j - this iteration

        Returns - a string in the form ' date1=01/20/1998 time1=09:00:00 date2=01/20/1998 time2=10:30:00 ' that
        will cause isprint to only examine a slice of the data.
    """
    expStartTime = time.mktime((expTimeList[0],
                                expTimeList[1],
                                expTimeList[2],
                                expTimeList[3],
                                expTimeList[4],
                                expTimeList[5],0,0,-1))
    expEndTime = time.mktime((expTimeList[6],
                             expTimeList[7],
                             expTimeList[8],
                             expTimeList[9],
                             expTimeList[10],
                             expTimeList[11],0,0,-1))
    totalExpTime = expEndTime - expStartTime
    begSliceTime = int(((j/float(numIter)) * totalExpTime) + expStartTime)
    endSliceTime = int((((j+1)/float(numIter)) * totalExpTime) + expStartTime)

    begTimeList = time.localtime(begSliceTime)
    endTimeList = time.localtime(endSliceTime)

    
    return ' date1=%i/%i/%i time1=%02i:%02i:%02i date2=%i/%i/%i time2=%02i:%02i:%02i ' % (begTimeList[1],
                                                                                          begTimeList[2],
                                                                                          begTimeList[0],
                                                                                          begTimeList[3],
                                                                                          begTimeList[4],
                                                                                          begTimeList[5],
                                                                                          endTimeList[1],
                                                                                          endTimeList[2],
                                                                                          endTimeList[0],
                                                                                          endTimeList[3],
                                                                                          endTimeList[4],
                                                                                          endTimeList[5])


def getTimesOfExperiment(expList, expId):
    """
    Taken from madrigalWeb.globalIsprint.py

    getTimesOfExperiment returns a list of the start and end time of the experiment given expId.

    Input:

        expList - the list of MadrigalExperiment objects

        expId - the experiment id

    Returns:

        a list of:
            (startyear,
            startmonth,
            startday,
            starthour,
            startmin,
            startsec,
            endyear,
            endmonth,
            endday,
            endhour,
            endmin,
            endsec)
    """

    retList = None
    for exp in expList:
        if exp.id == expId:
            retList = (exp.startyear,
                       exp.startmonth,
                       exp.startday,
                       exp.starthour,
                       exp.startmin,
                       exp.startsec,
                       exp.endyear,
                       exp.endmonth,
                       exp.endday,
                       exp.endhour,
                       exp.endmin,
                       exp.endsec)

    return retList

def map_parms(kinst, kindat, parameters):
    """
    
    """
    # default value of parameters is empty string-- which corresponds to all
    # parameters originally included in file
    # dont really want derivable parms unless explicitly stated 
    # do we want derivable parms at all?

    # maybe dont need kindat? instParms will always be a superset of parms potentially associated w/ data

    if parameters == '':
        # data parameters for our test example:
        '''
        YEAR: Year (universal time), units: y
        MONTH: Month (universal time), units: m
        DAY: Day (universal time), units: d
        HOUR: Hour (universal time), units: h
        MIN: Minute (universal time), units: m
        SEC: Second (universal time), units: s
        RECNO: Logical Record Number, units: N/A
        KINDAT: Kind of data, units: N/A
        KINST: Instrument Code, units: N/A
        UT1_UNIX: Unix seconds (1/1/1970) at start, units: s
        UT2_UNIX: Unix seconds (1/1/1970) at end, units: s
        BN_NT: Geodetic Northward component of geomagnetic field in nT, units: nT
        BE_NT: Geodetic Eastward component of geomagnetic field in nT, units: nT
        BD_NT: Geodetic Downward component of geomagnetic field in nT, units: nT
        '''
        # lets omit recno, kinst and kindat,
        # only because they are madrigal-specific
        # time parameters are standard
        standardTimeParms = ['year', 'month', 'day', 'hour', 'min', 'sec', 'ut1_unix']#, 'ut2_unix'] ???
        instParmObj = madrigal.metadata.MadrigalInstrumentParameters()
        instParms = instParmObj.getParameters(kinst)
        # for our example, instParms = ['bn_nt', 'be_nt'. 'bd_nt']
        return(standardTimeParms + instParms)
    else:
        # idk yet
        pass

    # return a list of desired madrigal parm mnems
    return None


def generate_parm_json_headers(madParms):
    """
    """
    # parmsList is a list of dicts containing info about a single parameter
    parmsList = []
    madParmInfo = madrigal.data.MadrigalParameters()

    # first 7 madParms are standard time parms, we will treat
    # this as a single time parameter 
    timeDict = {
        "name": "Time",
        "type": "isotime",
        "units": "UTC",
        "fill": "null",
        "length": 24
    }
    parmsList.append(timeDict)

    for thisParm in madParms[7:]:
        thisParmDict = {}

        thisParmDict["name"] = thisParm

        if madParmInfo.isInteger(thisParm):
            thisParmDict["type"] = "integer"
        elif madParmInfo.isString(thisParm):
            thisParmDict["type"] = "string"
        else:
            thisParmDict["type"] = "double"

        if thisParmDict["type"] == "string":
            thisParmDict["length"] = madParmInfo.getStringLen(thisParm)

        thisParmDict["units"] = madParmInfo.getParmUnits(thisParm)
        thisParmDict["fill"] = "NaN"
        thisParmDict["description"] = madParmInfo.getSimpleParmDescription(thisParm)
        # how to handle array types?
        parmsList.append(thisParmDict)
        
    return(parmsList)


def madhapiID_toMadrigalID(id):
    """
    convert HAPI id -> Madrigal kinst/kindat
    """

    # assume we do the kinst/kindat breakdown here
    # hardcoded 4 now
    kinst = 8309
    kindat = 17560

    return((kinst, kindat))


def cleanDataTime(data):
    """
    converts madrigal time parms in data str to isotime, as hapi wants
    for use with isprint
    """
    newdatastr = ""
    for line in data.split('\n'):
        thisRow = line.split()

        if len(thisRow) < 7:
            # no data in this row
            continue

        # get non time data
        thisRecord = thisRow[7:]
        utctime = int(math.floor(float(thisRow[6])))
        utcDT = datetime.datetime.fromtimestamp(utctime, tz=datetime.timezone.utc)
        thisDT = datetime.datetime(year=int(thisRow[0]),
                                   month=int(thisRow[1]),
                                   day=int(thisRow[2]),
                                   hour=int(thisRow[3]),
                                   minute=int(thisRow[4]),
                                   second=int(thisRow[5]),
                                   tzinfo=datetime.timezone.utc)
        # ensure dt matches utc timestamp
        if thisDT != utcDT:
            raise ValueError(f"mismatched dts {utcDT}, {thisDT}")
        
        isoDT = thisDT.strftime("%Y-%m-%dT%H:%M:%SZ")
        thisLine = isoDT + "," + ",".join(thisRecord) + "\n"
        newdatastr += thisLine
    return(newdatastr)

