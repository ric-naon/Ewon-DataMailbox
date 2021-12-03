#!/usr/bin/env python

"""
SyncData.py is used to gather and save data from Ewon historical data mailbox 
Params: 
    ewonID - Desired ewon to gather data from
    filepath to desired save location
    devid - EWON API dev ID
    token - EWON API token

"""
#-----------------------------------------------------------------------------------------------------------------
# author:  Ricardo Naon
# version: 1.0
# email: rnaon@msiexpress.com
# status: Development
#------------------------------------------------------------------------------------------------------------------

import requests
import acct_man 
import pandas as pd
import sys
import os
import time


#Global dictionary to store all tag values for a specific ewon
tag_dict=dict()

def parsejson(data):
    """
    Parse the JSON data returned from the POST request
    Function Accepts raw JSON data in format shown here: 
    https://developer.ewon.biz/system/files_force/rg-0005-00-en-reference-guide-for-dmweb-api.pdf?download=1  
    
    Function Updates the global tag_dict and returns the 'moredataavialable' and 'lasttransactionid' flags  
    
    """
    #Loop through top level items
    for k,v in data.items():
        #Check for successful request
        if(k=='success' and v !=True): 
            sys.exit("Unsuccessful data request")
        if(k=='ewons'):
            #Loop through Ewon items
            for k1,v1 in v[0].items():
                if(k1=='tags'):
                    #Loop through tag items
                    for tags in v1:
                        if tags['id'] not in tag_dict:
                            #Check for data associated with tag
                            if 'history' not in tags:
                                pass #Ignore tag if no data is available
                            else:
                                #Add tag to tag_dict and update its data
                                tag_dict.setdefault(tags['id'],[tags['name'],pd.DataFrame(tags['history'])])
                        else:
                            if 'history' not in tags:
                                pass
                            else:
                                #Append data to existing tag in tag_dict
                                tag_dict[tags['id']][1]=tag_dict[tags['id']][1].append(pd.DataFrame(tags['history']), ignore_index=True)
                                
    #Return more data available and the last transaction ID
    if 'moreDataAvailable' in data:    
        return(data['moreDataAvailable'],data['transactionId'])
    else:
        return(False,data['transactionId'])

def syncrequest(lastid,ewonid, accountinfo):
    """
    Subsaquent sync requests using the last transaction ID
    : param lastid: integer, ewonid: integer
    :returns raw JSON data
    """
    
    #Submit Post Request
    data=requests.post('https://data.talk2m.com/syncdata',\
                data={'t2mdevid':accountinfo.devid,\
                      't2mtoken' :accountinfo.token,\
                      'ewonIds':ewonid,\
                      'createTransaction':'true',\
                      'lastTransactionId':lastid}).json()
    return(parsejson(data))
    
def initialrequest(ewonid,accountinfo):
    """
    This function provides the initial request to syncdata with a specific ewon specified by argument ewonid
    : param ewonid:integer
    """
    #Make sure dictionary is empty
    tag_dict.clear()
    
    #Submit initial Post Request
    data=requests.post('https://data.talk2m.com/syncdata',\
                data={'t2mdevid':accountinfo.devid,\
                      't2mtoken' :accountinfo.token,\
                      'ewonIds':ewonid,\
                      'createTransaction':'true'}).json()
    #Parse JSON
    return(parsejson(data))           

def deletedata(ewonid,lastid,accountinfo):
    """
    This Function deletes data up to the transaction id specified by lastid
    """
    r=requests.post('https://data.talk2m.com/delete',\
                data={'t2mdevid' : accountinfo.devid,\
                      't2mtoken' : accountinfo.token,\
                      'transactionId' : lastid,\
                      'ewonId' : ewonid \
                      }).json()
    if r['success'] != True:
        sys.exit("Bad delete request")

def syncdata(ewonid, accountinfo):
    """
    Handles different steps required to sync and delete data
    """
    #Send the initial POST request
    moreavail,lastid=initialrequest(ewonid,accountinfo)
    
    #Delete data up to last transaction ID (Only works with certain access level which I do not have)
    #deletedata(ewonid,lastid, accountinfo)
    while moreavail==True:
        #Loop until no more data available
        moreavail,lastid=syncrequest(lastid,ewonid,accountinfo)
        #deletedata(ewonid,lastid, accountino)
    #deletedata(ewonid,lastid, accountinfo)                          

def main():
    """
    main function of program
    handles command line arguments and saving files
    
    """

    if(len(sys.argv)<5):
        sys.exit("Error: not enough arguments")
    
    fp=sys.argv[2]
    if not os.path.exists(fp):
        sys.exit("Error Bad file path")
    
    ewonid=int(sys.argv[1])
    if not ewonid:
        sys.exit("No Ewons given")
        
    account = acct_man.accountInfo(sys.argv[4],sys.argv[3])
    #account.getewons()   
    #account.getstatus()   
    
    print("Syncing data...")
    syncdata(ewonid, account)
    print("Sync Complete")
    
    #Send data from pandas dataframe to csv and save at specified file path
    for tag,val in tag_dict.items():
        val[1].to_csv(fp + "/" + val[0] + "_" + time.strftime("%Y-%m-%d") + ".csv")

        

if __name__=="__main__":
    main()