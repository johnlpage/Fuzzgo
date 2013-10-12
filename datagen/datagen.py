import pymongo
from metaphone import doublemetaphone

import random
from random import choice 


def soundslike(word):
    if word is None:
        return None
    t = doublemetaphone(word)
    return t[0]+t[1]

def damage_term(word):
    rw = word
    p = random.randint(0,1000)
    if p > 999:
        o = random.randint(0,1)
        l = random.randint(0,len(word)-1)
        if o == 0 :
            rw = word[:l]+word[l+1:]
        if o == 1 :
            rw =  word[:l]+chr(random.randint(65,90))+word[l+1:]
    return rw
    


connection_string = "mongodb://localhost"
connection = pymongo.MongoClient(connection_string)#
connection.drop_database('people');
database = connection.people


database.nominals_v2.drop();
database.nominals_v2_vocab.drop();
#Need this index up front
database.nominals_v2_names.ensure_index([("p",pymongo.ASCENDING),
                                         ("_id",pymongo.ASCENDING)],unique=True)   


males = open('male.txt').read().splitlines()
females = open('female.txt').read().splitlines()
lasts = open('last.txt').read().splitlines()
postcodes = open("psmall.csv").read().splitlines();
streets = open("streets.csv").read().splitlines();
stdcodes = open("stdcodes.txt").read().splitlines();


for x in xrange(1,1000):
    records = []
    names = []
    for y in xrange(1,5000):
        firstname=None
        lastname=None
        middleone=None
        middletwo=None
        lastname =  choice(lasts).partition(' ')[0]
        lastname = damage_term(lastname)
        gender = random.randint(0,1)

        if gender == 0 :
            firstname = choice(males).partition(' ')[0]
            firstname = damage_term(firstname)
            if random.randint(0,100) > 70 :
                middleone = choice(males).partition(' ')[0]
                middleone = damage_term(middleone)
                if random.randint(0,100) > 80 :
                    middletwo = choice(males).partition(' ')[0]
                    middletwo = damage_term(middletwo)
            gender = 'M'
        else:
            firstname = choice(females).partition(' ')[0]
            firstname = damage_term(firstname)
            gender = 'F'   
            if random.randint(0,100) > 70 :
                middleone = choice(females).partition(' ')[0]
                middleone = damage_term(middleone)
                if random.randint(0,100) > 80 :
                    middletwo = choice(females).partition(' ')[0]
                    middletwo=damage_term(middletwo)
        

              
        # print firstname.title() + "," + lastname.title() + "," + gender
        
        pcline = choice(postcodes)
        parts = pcline.split(',')
        postcode = parts[0]
        lat = parts[1]
        lon = parts[2]
        town = parts[13].partition(' ')[0].replace('"','');
        county = parts[6].replace(' County','').replace('"','');
        
        #print postcode + "," + lat + "," + lon + "," + town + "," + county
        
        street = choice(streets)
        streetno = random.randint(1,200)
        streettype = choice(["Rd.","Road","Ln.","Lane","Crescent","St.","Street"]);
        
   
        
        stdcode = choice(stdcodes)
        phoneshort = random.randint(200000,8900000);
        phoneno =  stdcode  + str(phoneshort)
        
        mobile = random.randint(000000000,999999999);
        mobileno = "07"+str(mobile)
        
        
        address = str(streetno) + " " + street + " " + streettype + ", "+ town + ", " + county
        

        metafirstname = soundslike(firstname)
        metalastname = soundslike(lastname)
        metamiddleone = soundslike(middleone)
        metamiddletwo = soundslike(middletwo)
        
        record = { "gender" : gender,
                   "address" : address,
                   "postcode" : postcode,
                   "phone" : phoneno,
                   "mobile" : mobileno,
                   "location" : [lat,lon],
                   "firstname" : firstname ,
                   "metafirstname": metafirstname,
                   "lastname": lastname,
                   "metalastname": metalastname,
                   "allnames" : [firstname,lastname],
                   "allmetanames" : [metafirstname,metalastname]
                   }
        
        if middleone :
            record["middlenameone"] = middleone
            record["metamiddleone"] = metamiddleone
            record["allnames"].append(middleone)
            record["allmetanames"].append(metamiddleone)
            
        if middletwo :
            record["middlenametwo"] = middletwo
            record["metamiddletwo"] = metamiddletwo
            record["allnames"].append(middletwo)
            record["allmetanames"].append(metamiddletwo)
        
     
        records.append(record)
        
        #Add to  unique name list
        
        if firstname and len(firstname)>2:
            names.append({ "_id" : firstname ,
                       "p" : [firstname[0],firstname[1]] })
        if lastname and len(lastname)>2:
            names.append({ "_id" : lastname ,
                        "p" : [lastname[0],lastname[1]] })   
        if middleone and len(middleone)>2:    
            names.append({ "_id" : middleone ,
                        "p" : [middleone[0],middleone[1]]  }) 
        if middletwo and len(middletwo)>2:
            names.append({ "_id" : middletwo ,
                        "p" : [middletwo[0],middletwo[1]] }) 
            
    
        
            
    database.nominals_v2.insert(records);
    
    try:            
        database.nominals_v2_names.insert(names,continue_on_error=True) # This WILL fail a lot - thats the idea could upsert and count!
    except pymongo.errors.DuplicateKeyError:
        pass
    
   
    
    
    print x * 5000

database.nominals_v2.ensure_index([("firstname",pymongo.ASCENDING)])
database.nominals_v2.ensure_index([("lastname",pymongo.ASCENDING),("firstname",pymongo.ASCENDING)])   
database.nominals_v2.ensure_index([("middlenameone",pymongo.ASCENDING)])
database.nominals_v2.ensure_index([("middlenametwo",pymongo.ASCENDING)])

#Soundex Ones

database.nominals_v2.ensure_index([("metafirstname",pymongo.ASCENDING)])   
database.nominals_v2.ensure_index([("metalastname",pymongo.ASCENDING),("metafirstname",pymongo.ASCENDING)])   
database.nominals_v2.ensure_index([("metamiddleone",pymongo.ASCENDING)])  
database.nominals_v2.ensure_index([("metamiddletwo",pymongo.ASCENDING)])  

database.nominals_v2.ensure_index([("allnames",pymongo.ASCENDING)])   
database.nominals_v2.ensure_index([("allmetanames",pymongo.ASCENDING)])   



#What if we want to extend it out!

                   
                   
        
    






