'''
Created on Aug 25, 2013
# Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>

@author: jlp
'''

#TODO
#Using AT LEAST WIth array field needs an unwind - count - rewind.
#Using at least with typo also fails as the in clause isnt supported by aggregation, needs to become ors

from bson import Binary, Code
from bson.json_util import dumps
import json
import  re
import pymongo
from bottle import run,route,static_file,request
from metaphone import doublemetaphone

#Top level page
@route('/')
def query_index():
    return static_file('frontpage.html',root='./html')

#Anything in html directory send as is

@route('/html/<filename:path>')
def send_static(filename):
    return static_file(filename, root='./html')

def garbled_regex(name):
    regex = ""
    
    
    #One EXtra Letter
    namelen = len(name)
    for f in range(0,namelen+1) :
        regex = regex + '^' + name[:f] + '.' + name[f:] + '$|' 
    
    for f in range(0,namelen) :
        regex = regex + '^' + name[:f] + '.' + name[f+1:] + '$|' 
        regex = regex + '^' + name[:f] + name[f+1:] + '$|' 
        regex = regex + '^' + name[:f] + name[f+1:f+2] + name [f:f+1] + name[f+2:] + '$|' 
    
    regex=regex[:-1]    
    
    return regex;

def garbled(name,field):
    regex = re.compile(garbled_regex(name))
    namelist = [];
    
    termcursor = database["nominals_v2_"+field].find({ "p":{"$in":[name[0],name[1]]},"_id":regex},{"_id":True})
    for name in termcursor:
        namelist.append(name["_id"])
    
    print "Names:"
    print namelist;
    return {"$in":namelist};

#Simple regex version
def naive_garbled(name,field):
    regex = re.compile(garbled_regex(name))
    return regex
  
    
def soundslike(word):
    t = doublemetaphone(word)
    return t[0]+t[1]

@route('/search/simple')
def search_simple():
    namefields = ["firstname","lastname","middlenameone","middlenametwo"]
    fieldnames = ["firstname","lastname","middlenameone","middlenametwo","address","phone"]
    queryterms = {}
    queryfields = {}
    original = {}
    
    allnamesfield = "allnames"
    
    for fieldname in fieldnames:
        if fieldname in request.query and len(request.query[fieldname]) >0 :
            queryterms[fieldname] = request.query[fieldname].upper()
            original[fieldname] = request.query[fieldname].upper()
            queryfields[fieldname] = fieldname
            print "Looking for " + fieldname + ":" + queryterms[fieldname]

 
    combiner = request.query.combiner
    permute = request.query.permute
    nmatch = request.query.nmatch
    anyfield = request.query.anyfield
    print "Combiner is: " + combiner
    print "Permute is: " + permute
    print "nmatch is: " + nmatch
    print "Anyfield is: " + anyfield

    if permute == "soundlike":
        allnamesfield = "allmetanames"
        for namefield in namefields:
            if namefield in queryfields:
                queryterms[namefield] = soundslike(queryterms[namefield])
                queryfields[namefield] = "meta"+namefield

    
    if permute == "typo":
        for namefield in namefields:
            if namefield in queryfields:
                queryterms[namefield] = naive_garbled(queryterms[namefield],'names')

    if permute == "typovocab":
        for namefield in namefields:
            if namefield in queryfields:
                queryterms[namefield] = garbled(queryterms[namefield],'names')
        
        
            
    if anyfield == "true":
        for fieldname in fieldnames:
            if fieldname in request.query and len(request.query[fieldname]) >0 :
                queryfields[fieldname] = allnamesfield
            
            
    query = {}
    orvals = [];
    andvals = [];
    if combiner == "all":
        #AND is simple
        for fieldname in fieldnames:
            if fieldname in queryfields:
                andvals.append({queryfields[fieldname]:queryterms[fieldname]})
            query["$and"] = andvals
    else:
        if combiner == "one":
            for fieldname in fieldnames:
                if fieldname in queryfields:
                    orvals.append({queryfields[fieldname]:queryterms[fieldname]})
                query["$or"] = orvals;
        else:
            #Run an aggregation Query - then a query by ID
            for fieldname in fieldnames:
                if fieldname in queryfields:
                    orvals.append({queryfields[fieldname]:queryterms[fieldname]})
                    query["$or"] = orvals;
            
            #Match on the OR of the fields - parallel - individual indexes
            #Project a score by adding one for each match
            #Convert each field to a aggregation boolean term {$eq:[a,b]}
            
            if anyfield != "true":
                innermatches=[];
                for fieldname in fieldnames:
                    if fieldname in queryfields:
                        #If queryterms[fieldname] is a $in clause we need a different model
                        if type(queryterms[fieldname]) == dict and queryterms[fieldname]["$in"]:
                            qvals = queryterms[fieldname]["$in"]
                            for qval in qvals:
                                bterm = {"$cond":[{"$eq":["$"+queryfields[fieldname],qval]},1,0]}
                                innermatches.append(bterm)
                        else:
                            bterm = {"$cond":[{"$eq":["$"+queryfields[fieldname],queryterms[fieldname]]},1,0]}
                            innermatches.append(bterm)
                            
                #If we are using Allfields then we need to unwind or innermatches wont work
                #We then need to sum then back together again
                aquery = { "$match" : query}
                aproject = { "$project" : { "c" : { "$add" : innermatches} } }
                afilter = { "$match" : { "c" : { "$gte" : int(nmatch)}}} # We end up with a "" in the array
                aproject2 = {"$project" : { "_id":"$_id" }}
                alimit = {"$limit":250}
                
                
                counts = database.nominals_v2.aggregate([aquery,aproject,afilter,aproject2,alimit])
                print [aquery,aproject,afilter,aproject2,alimit]
                
            else :
                
                innermatches=[];
                for fieldname in fieldnames:
                    if fieldname in queryfields:
                        #If queryterms[fieldname] is a $in clause we need a different model
                        if type(queryterms[fieldname]) == dict and queryterms[fieldname]["$in"]:
                            qvals = queryterms[fieldname]["$in"]
                            for qval in qvals:
                                bterm = {"$cond":[{"$eq":["$"+queryfields[fieldname],qval]},original[fieldname],""]}
                                innermatches.append(bterm)
                        else:
                            bterm = {"$cond":[{"$eq":["$"+queryfields[fieldname],queryterms[fieldname]]},original[fieldname],""]}
                            innermatches.append(bterm)
                            
                #If we are using Allfields then we need to unwind or innermatches wont work
                #We then need to sum then back together again
                aquery = { "$match" : query}
                aunwind = { "$unwind" : "$"+allnamesfield }
                aproject = { "$project" : { "c" : { "$concat" : innermatches} } }
                agroup = { "$group" : { "_id" : "$_id" , "c" : { "$addToSet" : "$c"}}}
                aunwind2 = { "$unwind" : "$c" }
                agroup2 = { "$group" : { "_id" : "$_id" , "c" : { "$sum" : 1}}}
                afilter = { "$match" : { "c" : { "$gte" : int(nmatch)+1}}} # We end up with a "" in the array
                aproject2 = {"$project" : { "_id":"$_id" }}
                alimit = {"$limit":250}
                
                
      
                    #Need to unwind and regroup if using allnames or metaallnames
                    
                    
                counts = database.nominals_v2.aggregate([aquery,aunwind,aproject,agroup,aunwind2,agroup2,afilter,aproject2,alimit])
                print [aquery,aunwind,aproject,agroup,aunwind2,agroup2,afilter,aproject2,alimit]
                
            print dumps(counts)
            if len(counts["result"]) <1:
                query["$or"] = [{"_id":"xyzzyzzy"}]; #Quick hack for demo
            else:    
                query["$or"] = counts["result"];
 
                
   
            
    print dumps(query)
    

    try:
            cursor = database.nominals_v2.find(query).limit(10)
    except Exception as e:
            print "Unable to query database" + e.strerror
    
    resultlist=[]
    for person in cursor:
        person['_id'] = str(person['_id'])
        resultlist.append(person)
        
    #Also want an explanation
    #explanation = dumps(cursor.explain())
    explanation = dumps(database.nominals_v2.find(query).explain())
    #Little round trip from BSON->JSON->Object to avoif Mongo Types and Bottle
    return { 'results' : resultlist, 'explain' : json.loads(explanation)}


connection_string = "mongodb://localhost"
connection = pymongo.MongoClient(connection_string)
database = connection.people

run(host='localhost', port=8080)
