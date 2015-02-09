#!/usr/bin/env python
import subprocess
import  os, sys, time, datetime
import urllib2, urllib

try: import simplejson as json
except ImportError: import json

headers={ 'Content-Type': 'application/json' }

data=[]
fn=[]
#list all the files in the dir.
cat = subprocess.Popen(["hadoop", "fs", "-ls", "results/Panda/OverflowMatrix/SourceDestinationStatusCounts"], stdout=subprocess.PIPE)
for fl in cat.stdout:
    #print fl
    if not fl.count('part'): continue
    f=fl.split()
    fn.append(f[7])

ct=datetime.datetime.now()
toImport=ct-datetime.timedelta(days=4)
print 'importing from day: ',toImport

for f in fn:
    print f
    cat = subprocess.Popen(["hadoop", "fs", "-cat", f], stdout=subprocess.PIPE) 
    for row in cat.stdout:
        #print row
        row=row.replace("(","").replace(")","").replace(","," ")
        w=row.split()
        if (len(w)<5): 
            print 'problematic row:', row
            continue
        nr={}
        nr['dat']=int(w[0])
        if nr['dat'] != toImport.year*10000+toImport.month*100+toImport.day:
            continue
        nr['sou']=w[1]
        nr['des']=w[2]
        if w[3]=='finished': nr['sta']=0
        if w[3]=='failed': nr['sta']=1
        if w[3]=='cancelled': nr['sta']=2
        nr['cou']=int(w[4])
        data.append(nr)

print data 

json_data = json.dumps(data)

try:
    req = urllib2.Request("http://waniotest.appspot.com/overflowflow",json_data,{ 'Content-Type': 'application/json' })
    opener = urllib2.build_opener()
    f = opener.open(req,timeout=50)
    res=f.read()
    print res
    f.close()
except:
    print "# Can't upload to GAE", sys.exc_info()[0]
