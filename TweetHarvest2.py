#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 14:05:08 2017

@author: llbos
"""

import pdb
import imp 
import setupSearchStrings
import setupTwitterAccounts
import os 
import csv 
import time as ttime
import sys
from TwitterSearch import *
import sqlite3 as lite
import datetime
import re
def pauseAccount(response):
    if int(response['meta']['x-rate-limit-remaining'])<2:
        if verbosity_level>0:
            print("Parking %s for %i minutes to avoid rate limiting. " % (account[0],(float(response['meta']['x-rate-limit-reset'])+1-ttime.time())/60))
#Write when that account is paused until to the disk for the getTwitterAccountDetails function to look at.                                                                           
        with open(account[0] +'Pausing.csv','w') as f:
                writer = csv.writer(f)
                writer.writerow(['Pause Until',float(response['meta']['x-rate-limit-reset'])+1]) 


def getTwitterAccountDetails(n, tsN):
    
#    """
#    Function to get the twitterAccount details from the setup file in the home directory.  Returns 'accounts,n,tsN;. 'accounts' is a list of all account details, 'n' is the chosen account, tsN is the corresponding list of TwitterSearch Orders for the 'accounts'.  a slit of all acounts 'tsN',
#    """
    
    global timePausing #Used for checking how much time the function is idle for
    try:

        todo=True
        imp.reload(setupTwitterAccounts)#Reload the setup file to allow on the fly addition and subtraction of account
        accounts=setupTwitterAccounts.main()
        firstTimeFlag=True
        AccountPauses=[]
        remaketsN=True
    #Only remake the twitter search orders if necessary
        if len(tsN)==len(accounts):
            remaketsN=False
        else:
            tsN=[]
        while todo:
    #        Search through the accounts until one is found which isn't paused
            n+=1
            if n>len(accounts)-1:
                n=0
            if firstTimeFlag:
                #Remake the List of Twitter Account details and the list of how long the accounts are paused for.
                firstTimeFlag=False
                for account in accounts:
    #                Loop over the accounts found in the setup file and 
                    if remaketsN:
                        if verbosity_level>0:
                            print(["Remaking TwitterSearchAccount: " +account[0]])
                        tsN.append(TwitterSearch(account[1], account[2],account[3],account[4]))
                        
                        
    #                    See if there is a Pausing file for the account.  If there is, fetch when it is paused until.
                    if os.path.isfile(account[0] +'Pausing.csv'):
                        with open(account[0] +'Pausing.csv','r') as f:
                            reader = csv.reader(f)
                            AccountPause=list(reader)
                            AccountPauses.append(float(AccountPause[0][1]))
    #                 Else set the wait until to 1       
                    else:
                        AccountPauses.append(1)
    #            No point looping over the accounts if they are all paused.                    
                minPause=min(AccountPauses)-ttime.time()+1
                if minPause>0:
                    if verbosity_level>0:
                        print("Wating for account to become available")
                    sleepyTime=True
                    while sleepyTime:
                        timepause=min(AccountPauses)-(ttime.time())
                        if verbosity_level>0:
                            print("Wating for account to become available: " + str(round(timepause,0))+'s.')
                        ttime.sleep(1)
                        timePausing+=1
                        if min(AccountPauses)<(ttime.time()+.01):
                            sleepyTime=False
            #If the account at n is available exit the loop                        
            if AccountPauses[n]<ttime.time():
                todo=False
    except Exception as e:
        errstring=('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e )
        with open('errorfile.csv','a') as f:
            writer = csv.writer(f)
            writer.writerow([str(errstring)+' at ' +str(datetime.datetime.now())]) 
        # pdb.set_trace()

        
    return accounts,n,tsN

def getSeacrchStrings(n):
#    """"
#Function searches the setup file for all the strings and returns the chosen string
#    """"
    global timePausing
    global timeSince 
    global efficiency
#    Used to calculate  idle time. 
    if ttime.time()-timeSince>60:
        efficiency=(1-(timePausing/(ttime.time()-timeSince)))*100
        timeSince=ttime.time()
        timePausing=0
    todo=True
#    Get the search strings
    imp.reload(setupSearchStrings)
    searchstrings,BackFill=setupSearchStrings.main()
    attFilt=[0]*len(BackFill)
    attFilt.extend([1]*len(BackFill))
    attFilt.extend([2]*len(BackFill))
    stringspos=[s+'_POS' for s in searchstrings]
    stringsneg=[s+'_NEG' for s in searchstrings]
    searchstrings.extend(stringsneg)
    searchstrings.extend(stringspos)
    BackFill.extend(BackFill)
    BackFill.extend(BackFill)
    firstTimeFlag=True
    while todo:
        #By Default the T0, T1, and T2 are set to 0.  
#        The last 100 tweets are collected between T2 and T1.  Only 100 tweets at  a time can be returned, the oldest being T1 and most recent being T2.  The previous iterations T2 is set to T0.  If there is 100 returned we know there must be a gap inbetween T1 and T0 which needs filling.
        GapFile=[]
        T0=0
        T1=0
        T2=0
        n+=1
        if n>len(searchstrings)-1:
            n=0
       
#        print(n)
        
        if firstTimeFlag:
#            This is basically about not interrogating the API unecceasrrily.  When a search is made, the number of tweets returned over the time difference is used to predict when the next search should be - the searchstring is "paused".
            stringPause=[]
            for eachString in searchstrings:
                if os.path.isfile(eachString +'Pausing.csv'):
                    with open(eachString +'Pausing.csv','r') as f:
                        reader = csv.reader(f)
                        searchPause=list(reader)
                        stringPause.append(float(searchPause[0][1]))
                        
                else:
#                    If there is not Pause file for the string, set the 'pause until' to 1.
                    stringPause.append(1)
#            Select the searchString IF the string is NOT paused!        
        if stringPause[n]<(ttime.time()-.1):
            
            searchstring=searchstrings[n]
            attitudeFilter=attFilt[n]
            todo=False
        else:
#            print('Pausing')
            pass
#        No point looping if ALL strings are paused.  The following loop looks at all the pauses and paused for the minimum available.
        if todo:
            soonest=min(stringPause)
            sleeptime=soonest-ttime.time()+.1
            if sleeptime>0:
                if verbosity_level>0:
                    print("Waiting for search string to become available in %is. (Approx %i%% efficient)" % (sleeptime,efficiency))
                sleepyTime=True
                while sleepyTime:
                    ttime.sleep(1)
                    timePausing+=1
                    if (ttime.time()-1)-soonest>0.1:
                        sleepyTime=False
#    For the chosen search string we need to get the T0, T1, T2 for that searchstring.
#Read in GapFile.  Gapfile has 3 tweet IDs.  T0 is the tweet ID at the start of the gap (the last contiguous slice). T1 is the end of the gap.  T2 is the most recent TweetID.
    if os.path.isfile(searchstring +'GapFile.csv'):
        
        with open(searchstring +'GapFile.csv') as f:
            reader = csv.reader(f)
            GapFile=list(reader)
            try:
                T0=int(GapFile[0][1])
                T1=int(GapFile[1][1])
                T2=int(GapFile[2][1])
            except:
                print("Error in Gapfile for search string: "+searchstring)
            if verbosity_level>1:
                print('Gapfile exists ' + searchstring+' '+ str(T0) +' ' +str(T1) +' '+str(T2))

                    
    else:
        todo = False
    if T1==0:
       GapFilling=False

       if verbosity_level>1:
            print('gapfilling is false')
        
    else:
       GapFilling=True    
        #IF the user has chosen to backfill, set T0 to the first tweet ever 
    firstSearch=False
    if BackFill[n]==1 and not os.path.isfile(searchstring +'GapFile.csv'):  
        GapFilling=True
        firstSearch=True
        if verbosity_level>1:
            print('First TIMER Backfilling')
    if BackFill[n]==0 and not os.path.isfile(searchstring +'GapFile.csv'):
        firstSearch=True
        GapFilling=False
        if verbosity_level>1:
            print('First Time Not Backfilling')

        
#    print(['T2: ' + str(T2)])
    return searchstring,n,T0,T1,T2,GapFilling, attitudeFilter,firstSearch

global verbosity_level   
DefaultWaitTime=60*10  #If no tweets are found the search is paused.  Reduce this if you want fast response to a sudden event people start tweeting about - BE AWARE OF BEING RATE LIMITED THOUGH
DefaultWaitTimeNoTweets=4*60
DefaultWaitTimeNoTweetsAtt=15*60
verbosity_level=1
debugUser='YourUsername'  #If YourUsername is found in the search it will flash up on the screen!

optimalNumberOfTweets=70 #Aim to record this number of tweets each time (not paused if more than this nubmer).

#Random variables
timePausing=0
efficiency=100
timeSince=0
timeSince=ttime.time()
nTwitterAccounts=0
nSearchStrings=0
countResponse=0
tsN=[]
db=True
todo=True


#Loop Over Following
try:
    sys.setdefaultencoding('utf8')
except:
    pass

while todo:
    try:
        countResponse+=1
        #Get Twitter Account Details
#        account,nTwitterAccounts=getTwitterAccountDetails(nTwitterAccounts)
        accounts,nTwitterAccounts,tsN=getTwitterAccountDetails(nTwitterAccounts,tsN)        
        #Get Twitter Search Term
        account=accounts[nTwitterAccounts]
        SearchString,nSearchStrings,T0,T1,T2,GapFilling,attitudeFilter,firstSearch=getSeacrchStrings(nSearchStrings)
        keywords=SearchString
        #Create a search order
        tso = TwitterSearchOrder()
        tso.remove_all_filters()
         # let's define all words we would like to have a look for
        if attitudeFilter==1:
            ###Attitude filter is negative so we need to chop off the Neg part of searchstring
            keywords=SearchString[:-4]
            tso.set_negative_attitude_filter()

        if attitudeFilter==2:
            ###Attitude filter is positive so we need to chop off the Neg part of searchstring
            keywords=SearchString[:-4]
            tso.set_positive_attitude_filter()
            
        tso.set_keywords([keywords])
        
#                         Might want to CHANGE THIS!!!!!!
        tso.set_language('en')
        if not firstSearch:
            if not GapFilling:#Only need to set since_id 
                if type(T2)==int:
                    if T2==0:
                        T2=1
                    tso.set_since_id(T2)
    #                print('since :' + str(T0))

                else:
                    pass    
            elif GapFilling:
                tso.set_max_id(T1)
                if T0==0:
                    if verbosity_level>0:
                        print('Not sure why but T0 is set to 0 and I am trying to search backwards so setting to 1')
                    T0=1
                tso.set_since_id(T0)

#                print("Not Setting a T0")
#            print('since :' + str(T0))
                
#                print('max :' + str(T1))
#                print("Not Setting T1")

      
#Lets get some Tweets
###########################################################################################################################################################        
        response = tsN[nTwitterAccounts].search_tweets(tso)
     
  
        ##########################################################################################################################################################################################       
       
        #Use information from search to park the twitter account for the correct number of seconds.  This is ifthe account is being rate limited.  Usually only a problem if the search term is retrieving LOTS of results.  
        if 'meta' in response:
            if 'x-rate-limit-remaining' in response['meta']:
                pauseAccount(response)
        #==============================================================================
        #==============================================================================
        # CHANGE THE T0 T1 T@ according to how many tweets were obtained
        #==============================================================================
        #==============================================================================                    
        #==============================================================================
        #        If the number of tweets is not 0
        #==============================================================================

                        #==============================================================================
        #===============================FILE TWEETS!!!=================================
        #  If number of tweets returned > 0 File tweets
        #==============================================================================
        #==============================================================================
                
                nTweets=0
                NTweets=0
                nGeo=0
                pattern = '%a %b %d %H:%M:%S %Y'
                if len(response['content']['statuses'])!=0:
                    with open(SearchString+'DATA.csv', 'a',encoding='utf-8') as fdata:                    
                        
                        connection = lite.connect('twit.db')
                        cursor = connection.cursor()
                        if verbosity_level>1:
                            cursor.execute("select count(*) from tweets")  
                            print('tweets before ' + str(list(cursor)))
                        for tweet in response['content']['statuses']:
                            #For Each Tweet, Send it to the DATA file.
                            NTweets+=1
                            if 'retweeted_status' not in tweet:             
                                nTweets+=1
                                sqlstr=''
                                writer = csv.writer(fdata)                       
        #                        geo=''
        #                        if 'geo' not in tweet:
        #                            geo=''
        #                        else:
        #                            try:
        #                                nGeo+=1
        #                                print(tweet['geo'])
        #                            except:
        #                                pass
        #                       
                                t_start=tweet['created_at']
                                t_start=t_start[:20]+t_start[-4:]
                                epochstart = int(ttime.mktime(ttime.strptime(t_start, pattern)))
                                data=[tweet['user']['screen_name'],tweet['id'],tweet['created_at'],tweet['retweet_count'],tweet['user']['location'],tweet['text'],epochstart]

                                writer.writerow(data)
                                p=data
                                

                                
                                sql = "SELECT * FROM tweets \
                                   WHERE id = '%d'" % (p[1])
                                cursor.execute(sql) 
                                results = cursor.fetchall()
                                negfilt=0
                                posfilt=0

                                if attitudeFilter==1:
                                            ###Attitude filter is negative so we need to chop off the Neg part of searchstring
                                    negfilt=1
                                if attitudeFilter==2:
                                            ###Attitude filter is positive so we need to chop off the Neg part of searchstring
                                    posfilt=1
                                # If the tweet already exist it may be that the tweet has now been found using the attitude filtering option so that tweet just needs updating.
                                if len(results)==0:
                                    
                                    #Insert the result into the database
                                    p.append(posfilt)
                                    p.append(negfilt)
        #                            format_str = """INSERT INTO tweets (id, screen_name, text, retweet_count, location,epoch,positive,negative,search) VALUES ({id}, "{screen_name}", "{text}", "{retweet_count}", "{location}","{epoch}",{positive},{negative},"{search}");"""
        #                            sql_command = format_str.format(id=int(p[1]), screen_name=p[0], text=repr(p[5]), retweet_count = int(p[3]),location = p[4], epoch=float(p[6]),positive=p[7],negative=p[8],search=str(keywords))
                                    textt=p[5]
                                    textt=textt.replace("\'"," SL")
                                    textt=textt.replace("\""," SLd")
                                    keywordst=keywords
                                    keywordst.replace("\'"," SL")
                                    keywordst.replace("\""," SLd")
                                    screen_namet=p[0]
                                    screen_namet.replace("\'"," SL")
                                    screen_namet.replace("\""," SLd")
                                    locationt=p[4]
                                    locationt.replace("\'"," SL")
                                    locationt.replace("\""," Sld")
                                    

                                    format_str = 'INSERT INTO tweets (id, screen_name, text, retweet_count, location,epoch,positive,negative,search,harvested_at) VALUES (%i, "%s", "%s", %i, "%s",%f,%i,%i,"%s",%f)'
        #                                                       sql="UPDATE tweets SET positive=%i, negative=%i WHERE id =%i" % (posfilt,negfilt,p[1])
                                    try:
                                        # sqlstr=format_str % (int(p[1]), re.escape(screen_namet), re.escape(textt),  int(p[3]), re.escape(locationt), float(p[6]),p[7],p[8],re.escape(str(keywordst)))
        #                               print(sqlstr)
                                        cursor.execute(format_str % (int(p[1]), re.escape(screen_namet), re.escape(textt),  int(p[3]), re.escape(locationt), float(p[6]),p[7],p[8],re.escape(str(keywordst)),ttime.time()))
                                    except Exception as e:
                                        format_str = 'INSERT INTO tweets (id, epoch,positive,negative,search,harvested_at) VALUES (%i,%f,%i,%i,"%s",%f)'
                                        qlstr=format_str# % (int(p[1]), float(p[6]),p[7],p[8],re.escape(str(keywordst)))
        #                               print(sqlstr)
                                        cursor.execute(qlstr%(int(p[1]), float(p[6]),p[7],p[8],re.escape(str(keywordst)),ttime.time()))
                                        with open('TweetsFailedToSendToDB.csv', 'a',encoding='utf-8') as ff:
                                            writer = csv.writer(ff)
                                            writer.writerow([data,str(datetime.datetime.now())])

                                        errstring=('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e)
                                        with open('errorfile.csv','a') as ferr:
                                            writer = csv.writer(ferr)
                                            writer.writerow([str(errstring)+' at ' +str(datetime.datetime.now())+ ' SQL STRING= ']) 
                                        
                                    if verbosity_level>1:
                                        print('New Tweet Added')

  
         
                                                                           
                                else:
                                    #Modify the entry.
                                    posfilt=posfilt+results[0][6]                            
                                    negfilt=negfilt+results[0][7]                            
                                    if negfilt>1:
                                        negfilt=1
                                    if posfilt>1:
                                        posfilt=1
                                    sql="UPDATE tweets SET positive=%i, negative=%i WHERE id =%i" % (posfilt,negfilt,p[1])
        #                            print(sql)
                                    if int(p[3])>=results[0][3]:
                                        retweetCount=int(p[3])
                                    else:
                                        retweetCount=results[0][3]
                                    cursor.execute("UPDATE tweets SET positive=%i, negative=%i,retweet_count=%i WHERE id =%i" % (posfilt,negfilt,retweetCount,p[1]))
                                    # print('updated')
                                    if verbosity_level>1:
                                        print('New Tweet updated')    
                                
                                #Purely to check that the softwre is finding all the tweets.  Check to see if your username is found
                                if db:                            
                                    if tweet['user']['screen_name']==debugUser:
                                        for NN in range(0,20):
                                            print("SEEN YOUR TWEET: %s" % (tweet['text']))
                            if 'retweeted_status' in tweet:             
                                tweet=tweet['retweeted_status']
                                sqlstr=''
                                geo=''
                                t_start=tweet['created_at']
                                t_start=t_start[:20]+t_start[-4:]
                                epochstart = int(ttime.mktime(ttime.strptime(t_start, pattern)))
                                data=[tweet['user']['screen_name'],tweet['id'],tweet['created_at'],tweet['retweet_count'],tweet['user']['location'],tweet['text'],epochstart]
                                p=data
                                

                                
                                sql = "SELECT * FROM tweets \
                                   WHERE id = '%d'" % (p[1])
                                cursor.execute(sql) 
                                results = cursor.fetchall()
#                                pdb.set_trace()

                                posfilt=0                            
                                negfilt=0
                                if attitudeFilter==1:
                                            ###Attitude filter is negative so we need to chop off the Neg part of searchstring
                                    negfilt=1
                                if attitudeFilter==2:
                                            ###Attitude filter is positive so we need to chop off the Neg part of searchstring
                                    posfilt=1
                                if len(results)==0:
                                    #Insert the result into the database
                                    p.append(posfilt)
                                    p.append(negfilt)
        #                            format_str = """INSERT INTO tweets (id, screen_name, text, retweet_count, location,epoch,positive,negative,search) VALUES ({id}, "{screen_name}", "{text}", "{retweet_count}", "{location}","{epoch}",{positive},{negative},"{search}");"""
        #                            sql_command = format_str.format(id=int(p[1]), screen_name=p[0], text=repr(p[5]), retweet_count = int(p[3]),location = p[4], epoch=float(p[6]),positive=p[7],negative=p[8],search=str(keywords))
                                    textt=p[5]
                                    textt=textt.replace("\'"," SL")
                                    textt=textt.replace("\""," SLd")
                                    keywordst=keywords
                                    keywordst.replace("\'"," SL")
                                    keywordst.replace("\""," SLd")
                                    screen_namet=p[0]
                                    screen_namet.replace("\'"," SL")
                                    screen_namet.replace("\""," SLd")
                                    locationt=p[4]
                                    locationt.replace("\'"," SL")
                                    locationt.replace("\""," Sld")
                                    

                                    format_str = 'INSERT INTO tweets (id, screen_name, text, retweet_count, location,epoch,positive,negative,search,harvested_at) VALUES (%i, "%s", "%s", %i, "%s",%f,%i,%i,"%s",%f)'
        #                                                       sql="UPDATE tweets SET positive=%i, negative=%i WHERE id =%i" % (posfilt,negfilt,p[1])
                                    try:
                                        # sqlstr=format_str % (int(p[1]), re.escape(screen_namet), re.escape(textt),  int(p[3]), re.escape(locationt), float(p[6]),p[7],p[8],re.escape(str(keywordst)))
        #                               print(sqlstr)
                                        cursor.execute(format_str % (int(p[1]), re.escape(screen_namet), re.escape(textt),  int(p[3]), re.escape(locationt), float(p[6]),p[7],p[8],re.escape(str(keywordst)),ttime.time()))
                                    except Exception as e:
                                        format_str = 'INSERT INTO tweets (id, epoch,positive,negative,search,harvested_at) VALUES (%i,%f,%i,%i,"%s",%f)'
                                        qlstr=format_str# % (int(p[1]), float(p[6]),p[7],p[8],re.escape(str(keywordst)))
        #                               print(sqlstr)
                                        cursor.execute(qlstr%(int(p[1]), float(p[6]),p[7],p[8],re.escape(str(keywordst)),ttime.time()))
                                        with open('TweetsFailedToSendToDB.csv', 'a',encoding='utf-8') as ff:
                                            writer = csv.writer(ff)
                                            writer.writerow([data,str(datetime.datetime.now())])

                                        errstring=('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e)
                                        with open('errorfile.csv','a') as ferr:
                                            writer = csv.writer(ferr)
                                            writer.writerow([str(errstring)+' at ' +str(datetime.datetime.now())+ ' SQL STRING= ']) 
                                    if verbosity_level>1:
                                        print('Old Retweet Added')
                                        
                                        
         
                                                                           
                                else:
                                    #Modify the entry.
                                    posfilt=posfilt+results[0][6]                            
                                    negfilt=negfilt+results[0][7]                            
                                    if negfilt>1:
                                        negfilt=1
                                    if posfilt>1:
                                        posfilt=1
                                 
                                    sql="UPDATE tweets SET positive=%i, negative=%i, WHERE id =%i" % (posfilt,negfilt,p[1])
        #                            print(sql)
                                    if int(p[3])>=results[0][3]:
                                        retweetCount=int(p[3])
                                    else:
                                        retweetCount=results[0][3]

                                    cursor.execute("UPDATE tweets SET positive=%i, negative=%i,retweet_count=%i WHERE id =%i" % (posfilt,negfilt,retweetCount,p[1]))
                                    if verbosity_level>1:
                                        print('Old Retweet updated')

                                
                                #Purely to check that the softwre is finding all the tweets.  Check to see if your username is found
                                if db:                            
                                    if tweet['user']['screen_name']==debugUser:
                                        for NN in range(0,20):
                                            print("SEEN YOUR TWEET: %s" % (tweet['text']))

                        if verbosity_level>1:
                            print("committing to DB")
                        connection.commit()
                        if verbosity_level>1:
                            cursor.execute("select count(*) from tweets")  
                            print('tweets after ' + str(list(cursor)))

                        connection.close()

                            
                    percentTweets=100-(nTweets/NTweets*100)
                    percentGeo=(nGeo/NTweets*100)
                    if verbosity_level>1:
                        print("reTweets Discarded: "+str(nTweets-NTweets))
        #            print("Percent Geo: "+str(percentGeo))
                                        
                
        #==============================================================================
        #         Use number of tweets returned to sleep Search Term for correct number of seconds
        #           And send that 'sleep until' number to the disk so other functions can find it.
                    #Pause to ensure max use of calls.  Aiming for ~OptimalNumber tweets a call.                
        #=============================================================================
        #Latest and oldest tweet just obtained.            
                    tweet=response['content']['statuses'][0]  
                    tweet2=response['content']['statuses'][-1]  
                    tweet_id = tweet['id']
                    #Work out how many seconds this time covered: epochend to epochstart
                    pattern = '%a %b %d %H:%M:%S %Y'
                    t_start=tweet['created_at']
                    t_start=t_start[:20]+t_start[-4:]
                    epochstart = int(ttime.mktime(ttime.strptime(t_start, pattern)))
                    t_end=tweet2['created_at']
                    t_end=t_end[:20]+t_end[-4:]
                    epochend = int(ttime.mktime(ttime.strptime(t_end, pattern)))
                    #Work out how long you should wait before searching for this temr again.

                    time_pause=(epochstart-epochend+0.1)/len(response['content']['statuses'])*optimalNumberOfTweets
        #But DON'T Exceed the DefaultWaitTime otherwise can accidentally wait forever.
                    if time_pause>DefaultWaitTime:
                        time_pause=DefaultWaitTime
                    
                    if GapFilling:
                        if verbosity_level>0:
                            #Tell the user that your still gapfilling
                            print("%i tweets seen using %s about %s up to ID %i at %s. GapFilling..." % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14]))   

                    else:
                        if T0==0: 
                            #Which it WON'T BE if this is the last of the GapFilling iterations which means you shouldn't pause but the GapFillilng has already been set to 0.  This is a little inelegant but nexessary.
                          
                            #If < optimalNumberOfTweets obtained we need to pause this string to make the best use of the number of searches allowed. 
                            if len(response['content']['statuses'])<optimalNumberOfTweets:
                                if verbosity_level>0:
                                    print("%i tweets seen using %s about %s up to %i at %s. Pausing for %is " % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14],round(time_pause)))  
                                #And send that 'pause until' to the disk. 
                                with open(SearchString +'Pausing.csv','w') as f:
                                    writer = csv.writer(f)
                                    writer.writerow(['Searching for '+ SearchString+ ' paused until',ttime.time()+time_pause])
                            #Else don't pause but tell the user anyway and send 1 as the 'pause until' to the disk. 
                            else:
                                if verbosity_level>0:
                                    print("%i tweets seen using %s about %s up to with ID %i at %s. Not Pausing..." % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14]))   
                                with open(SearchString +'Pausing.csv','w') as f:
                                    writer = csv.writer(f)
                                    writer.writerow(['Searching for '+ SearchString+ ' paused until',1])
                                    
                #No Tweets obtained so Pause that search for a while
                else:
                    if verbosity_level>1:
                        print('No Tweets to file')
                        ##Commented out as this pausing is done earlier
        #            with open(SearchString +'Pausing.csv','w') as f:
        #                if GapFilling:
        #                    print("Gap Filled.....")   
        #                    writer = csv.writer(f)
        #                    writer.writerow(['Searching for '+ SearchString+ ' paused for some time until ',ttime.time()+0])
        #                
        #                else:
        #                    print("No tweets seen using %s about %s . Pausing for %i mins." % (str(account[0]),SearchString,DefaultWaitTimeNoTweets/60))   
        #                    writer = csv.writer(f)
        #                    writer.writerow(['Searching for '+ SearchString+ ' paused for some time until ',ttime.time()+DefaultWaitTimeNoTweets])
                if len(response['content']['statuses'])!=0:  

        #==========================================================================
        #             If the number of tweets is 100 then there is a gap to be filled between the oldest tweet just returned and the last tweet obtained in the previous search
        #==============================================================================
                    if len(response['content']['statuses'])==100:
                        if GapFilling:#Gap Must still not filled so change the T1 to the oldest tweet just obtained.
                            T1=response['content']['statuses'][99]['id']-1
                    
                                #Alter T1 but not T0 and obviously not T2
                        if not GapFilling:
                                #There is now a Gap!  So the most recent tweet you obtained is set to T2 and the Oldest to T1.  We get the T0 from the last iteration's T2
                            if not firstSearch:
                                GapFilling=True


                            if type(T2)==int:
                                T1=response['content']['statuses'][99]['id']-1
                                T0=T2#The Previous Iteration's T2 is the start of the gap.
                                T2=response['content']['statuses'][0]['id']
                            else:
                                break  
                            
                    else:
                        
        #==============================================================================
        #                 If the number of tweets < 1 then we have obtained all the tweets available
        #==============================================================================
                        if GapFilling:
        #                  Gap is now Filled!!!  We already have the T2, set from a previous iteration of this search so just set T0 and T1 to 0.
                            if len(response['content']['statuses'])!=0:
                                #For some reason there seems to be a bug which results
                                T1=response['content']['statuses'][-1]['id']-1
                                GapFilling=True
                            else:
                                T0=0
                                T1=0
                        else:
                            
                            #Update the T2.
                            T2=response['content']['statuses'][0]['id']
                            T0=0
                            T1=0
                            
                   
                    
                else:
        #No results were obtiined so we need to pause this for a little while
                    if not GapFilling:
                        dwait=DefaultWaitTimeNoTweets
                        if attitudeFilter==1:
                            dwait=DefaultWaitTimeNoTweetsAtt
                        if attitudeFilter==2:
                            dwait=DefaultWaitTimeNoTweetsAtt

                        with open(SearchString +'Pausing.csv','w') as f:
                            writer = csv.writer(f)
                            writer.writerow(['Pause until',ttime.time()+dwait])
                            if verbosity_level>1:
                                print('Pausing ' + SearchString + ' as no tweets seen for ' + str(int(dwait/60)) + ' minutes.')
                    else:
        #                If you were gapfilling and returned 0, this means that the gap is filled so run along.  And write the T0,T1,T2 to a file so the functions can obtain it. 
                        T0=0
                        T1=0                
                 #And write the T0,T1,T2 to a file so the functions can obtain it. 
                if len(response['content']['statuses'])!=0:
                    if firstSearch:
                        if GapFilling:
                            T1=response['content']['statuses'][-1]['id']-1
                            T0=1
                            T2=response['content']['statuses'][0]['id']
                        else:
                            T2=response['content']['statuses'][0]['id']
                            T1=0
                            T0=0
                # pdb.set_trace()

                if type(T2)==int:#It wont be if this is the first time being called and the response has had 0 tweets.  In which case we do NOT want a gap file because it will print 'false' to T2 and we will have an error the next time we check this search string.
                    with open(SearchString +'GapFile.csv','w') as f:
                        writer = csv.writer(f)
                        writer.writerow(['T0',T0])
                        writer.writerow(['T1',T1])
                        writer.writerow(['T2',T2])
                            
                

        else:
            if verbosity_level>0:
                print('for some reason the search was blocked from account ' +str(account[0])+ ' searchterm '+ keywords )
#        
    except Exception as e:
#         print(type(inst))    # the exception instance
#         print(inst.args)      # arguments stored in .args
#         print(inst)
        
        print(e)

        errstring=('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e)
        print(errstring)
        connection.close()
        errstring=('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e)
        with open('errorfile.csv','a') as f:
            writer = csv.writer(f)
            writer.writerow([str(errstring)+' at ' +str(datetime.datetime.now())]) 

        #Exception is usually something to do with rate limiting or connection so just pause that search account for DefaultWaitTime and see what happens.
        sleeptime=ttime.time()+DefaultWaitTime
        try: 
            #If the response has a rate-limiting-number use that to pause the account.
            sleeptime=(float(response['meta']['x-rate-limit-reset'])-ttime.time()+1)
            print("Some unexpected errorrate limit reached.  Parking for %i minutes. " % ((float(response['meta']['x-rate-limit-reset'])+1-ttime.time())/60))
        except:
            print("Unkown error.  Parking for %i minutes. " % (sleeptime/60))
            break
        #And send that to disk.                                                                  
        with open(account[0] +'Pausing.csv','w') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Pause Until:' , str(sleeptime+1)]) 
        response
        # break

        
