""" supermag_config.py, specific config file for SuperMAG web pass-thru

 Part of the HAPI Python Server.  The code and documentation resides at:

    https://github.com/hapi-server/server-python

 See accompanying 'superhapi.py' file for the pass-thru reader.

"""

import madhapireader


HAPI_HOME= 'home_madtest/'
title = 'Test Madrigal HAPI server'
api_datatype = 'web' 
floc={}
hapi_handler = madhapireader.do_data_madrigal
tags_allowed = [] # allowed subparams
loaded_config =	True # required, used to verify config variables exists on load

# Since we are based on the SuperMAG example...
# Currently SuperMAG can't stream because it is a web pass-thru
stream_flag=False  
# In theory, could stream by figuring out maximum interval SuperMAG website
# allows, then loop over that and stream each chunk.  But this would require
# discussion/permission from SuperMAG since their limits are for a reason.

# extra config vars i might need
# maybe just parse capabilities? 
HAPI_VERSION = "3.3"