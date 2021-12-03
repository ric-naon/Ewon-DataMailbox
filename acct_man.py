"""
Helper file for SyncData
Holds the account info object and utility functions
 """


import sys
import requests
import json

class accountInfo:
    """
    Object defined to manage the account info
    """
    
    def __init__(self,devid,token):
        self.devid=devid
        self.token=token
    
    
    #Utility functions to help with account management
    def getewons(self):
        r=requests.post('https://data.talk2m.com/getewons',\
                data={'t2mdevid':self.id,\
                      't2mtoken' :self.token\
                      }).json()
        with open('ewons.json', 'w', encoding='utf-8') as f:
            json.dump(r, f, ensure_ascii=False, indent=4)
        if r['success'] != True:
            sys.exit("Unsuccessful request in getewons")
        return(r['ewons'])
  
    def getstatus(self):
        r=requests.post('https://data.talk2m.com/getstatus',\
                data={'t2mdevid':self.id,\
                      't2mtoken' :self.token\
                      }).json()
        with open('status.json', 'w', encoding='utf-8') as f:
            json.dump(r, f, ensure_ascii=False, indent=4)   
        if r['success'] != True:
                sys.exit("Unsuccessful request in status")

        return(r['ewons'])