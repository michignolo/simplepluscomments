# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
#!/usr/bin/python
"""
Created on Tue Dec 23 11:32:46 2014

@author: michelangelo puliga, IMT Alti Studi Lucca,
michelangelo.puliga@imtlucca.it
"""


import time
import os
import glob
import sys
import random
import json
import codecs
import pickle
import httplib2
import numpy as np
from dateutil import parser as dparser

from apiclient import discovery
from oauth2client import client
from oauth2client import file
from oauth2client import tools



def initPlus():
    #
    # method to initialize the Plus methods with  
    # pickled data
    # 
    # code based on sample_tools.py from
    # google-api-python-client example
    # 
    name = "plus"
    version = "v1"
    credentials =    pickle.load( open( "./credentials.pickle", "rb" ) ) 
    flags =    pickle.load( open( "./flags.pickle", "rb" ) )
    http = credentials.authorize(http = httplib2.Http())
    service = discovery.build(name, version, http=http)
    return service

def getPlusEvents(filein):
    #
    # extract from an external file of youtube comments the ones
    # having replyCount field greater than 0
    # get the urlEvent list
    #
    
    op = codecs.open(filein,"r","utf-8")
    allEvents = set()
    for l in op.readlines():
        js = json.loads(l.rstrip())
        if(int(js["replyCount"]) > 0):
            allEvents.add(js["urlEvent"])
    op.close()
    return allEvents
    






    
def getDate(dt):
    #
    # utility to convert to timestamp 
    #
    
	dat = dparser.parse(dt)
	tsp = time.mktime(dat.timetuple())
	return int(tsp)
	


def extractJs(js):
    #
    # extracting only a subset of the json fields
    #
    vals = []
    for it in js['items']:

        author =  it['actor']['displayName']
        googleId = it['actor']['id']
        inReplyTo = it['inReplyTo'][0]['id']
        comment = it['object']['content']
        published = getDate(it['published'])
        dic = {'author.name' : author, 'inReplyTo' : inReplyTo, 'comment' : comment, 'published' : published, 'googlePlusUserId' : googleId }
        vals.append(dic)
    return vals
    


def getPlus(service,ts,outfile):
    #
    # getting the replies to a comment identified by
    # a specific urlEvent
    # 
    # input: 
    
    errors = open("Rerror.txt","a+")

    try:
        request = service.comments().list(activityId = ts)
    except Exception as e:
        errors.write(str(e)+"\n")
        errors.close()
        request = None
        
  
  
    vals = []
    while request is not None:
        try:
            activities_doc = request.execute()
            vals.append(extractJs(activities_doc))
            request = service.comments().list_next(request, activities_doc) ## query the comments service of G+ api
        except Exception as e:
            st = "Download: %s, Exception: %s"%(ts,str(e))
            errors.write(st+"\n")
            errors.close()
            request = None

    
    ou = codecs.open(outfile,'a+','utf-8') ## save it in a utf-8 compliant file
    
    for v in vals:
        for c in v:
            ou.write(json.dumps(c)+"\n")
    ou.close()
    
    return True

"""
Simple reply downloader of eventUrls from youtube comments.
"""

service = initPlus()
filelist = ["file1.json" , "file2.json"] ## list of files with comments (to be replaced with glob.glob (*.json) )

diffs = []
for evfile in filelist:
    
    allEvents = getPlusEvents(evfile)
    print  evfile, len(allEvents)

    outjfile = evfile.replace(".json","_reply_test.json") ## save in this file the replies

        
    
    for ts in allEvents:
        a = time.time()
        getPlus (service, ts,outjfile) ## one call for each eventUrl
        b = time.time()    
        print ts, b - a 
        diffs.append(b-a)

print np.mean(diffs), np.std(diffs)


    

