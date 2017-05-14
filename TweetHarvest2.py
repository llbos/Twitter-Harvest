#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 17 14:05:08 2017

@author: llbos
"""


import imp 
import setupSearchStrings 
import setupTwitterAccounts
import os 
import csv 
import time as ttime
import sys
from TwitterSearch import *



def getTwitterAccountDetails(n, tsN):
    
#    """
#    Function to get the twitterAccount details from the setup file in the home directory.  Returns 'accounts,n,tsN;. 'accounts' is a list of all account details, 'n' is the chosen account, tsN is the corresponding list of TwitterSearch Orders for the 'accounts'.  a slit of all acounts 'tsN',
#    """
    
    global timePausing #Used for checking how much time the function is idle for
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
                print("Wating for account to become available")
                sleepyTime=True
                while sleepyTime:
                    ttime.sleep(1)
                    timePausing+=1
                    if min(AccountPauses)<(ttime.time()+.01):
                        sleepyTime=False
        #If the account at n is available exit the loop                        
        if AccountPauses[n]<ttime.time():
            todo=False

    
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
            todo=False
        else:
#            print('Pausing')
            pass
#        No point looping if ALL strings are paused.  The following loop looks at all the pauses and paused for the minimum available.
        if todo:
            soonest=min(stringPause)
            sleeptime=soonest-ttime.time()+.1
            if sleeptime>0:
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
            print('Gapfile exists ' + searchstring+' '+ str(T0) +' ' +str(T1) +' '+str(T2))

                    
    else:
        todo = False
    if T1==0:
       GapFilling=False
       print('gapfilling is false')
        
    else:
       GapFilling=True    
     #IF the user has chosen to backfill, set T0 to the first tweet ever 
    if BackFill[n]==1 and not os.path.isfile(searchstring +'GapFile.csv'):
        T2='False'        
        GapFilling=False
        print('FirstTIMER')
#    print(['T2: ' + str(T2)])
    return searchstring,n,T0,T1,T2,GapFilling
    
DefaultWaitTime=60*13  #If no tweets are found the search is paused.  Reduce this if you want fast response to a sudden event people start tweeting about - BE AWARE OF BEING RATE LIMITED THOUGH
DefaultWaitTimeNoTweets=2*60
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
        SearchString,nSearchStrings,T0,T1,T2,GapFilling=getSeacrchStrings(nSearchStrings)

        #Create a search order
        tso = TwitterSearchOrder()
        tso.remove_all_filters()
        tso.set_keywords([SearchString]) # let's define all words we would like to have a look for
        
#                         Might want to CHANGE THIS!!!!!!
        tso.set_language('en')
        if not GapFilling and T2!=0:#Only need to set since_id 
            if type(T2)==int:
                tso.set_since_id(T2)
#                print('since :' + str(T0))

            else:
                pass    
        elif GapFilling:
            tso.set_max_id(T1)
            if T0==0:
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
        if int(response['meta']['x-rate-limit-remaining'])<5:
            print("Parking %s for %i minutes to avoid rate limiting. " % (account[0],(float(response['meta']['x-rate-limit-reset'])+1-ttime.time())/60))
#Write when that account is paused until to the disk for the getTwitterAccountDetails function to look at.                                                                           
            with open(account[0] +'Pausing.csv','w') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Pause Until',float(response['meta']['x-rate-limit-reset'])+1]) 

                    
#==============================================================================
#==============================================================================
# CHANGE THE T0 T1 T@ according to how many tweets were obtained
#==============================================================================
#==============================================================================                    
#==============================================================================
#        If the number of tweets is not 0
#==============================================================================

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
                    GapFilling=True
                    if type(T2)==int:
                        T1=response['content']['statuses'][99]['id']-1
                        T0=T2#The Previous Iteration's T2 is the start of the gap.
                        T2=response['content']['statuses'][0]['id']
                    elif type(T2)==str:
                        T1=response['content']['statuses'][99]['id']-1
                        T0=1#
                        T2=response['content']['statuses'][0]['id']
                    else:
                        break
                    
            else:
                
#==============================================================================
#                 If the number of tweets < 100 then we have obtained all the tweets available
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
                with open(SearchString +'Pausing.csv','w') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Pause until',ttime.time()+DefaultWaitTimeNoTweets])
            else:
#                If you were gapfilling and returned 0, this means that the gap is filled so run along.  And write the T0,T1,T2 to a file so the functions can obtain it. 
                T0=0
                T1=0                
         #And write the T0,T1,T2 to a file so the functions can obtain it. 
        if type(T2)==int:#It wont be if this is the first time being called and the response has had 0 tweets.  In which case we do NOT want a gap file because it will print 'false' to T2 and we will have an error the next time we check this search string.
            with open(SearchString +'GapFile.csv','w') as f:
                writer = csv.writer(f)
                writer.writerow(['T0',T0])
                writer.writerow(['T1',T1])
                writer.writerow(['T2',T2])
                    
        
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
            with open(SearchString+'DATA.csv', 'a',encoding='utf-8') as f:                    
                for tweet in response['content']['statuses']:
                    #For Each Tweet, Send it to the DATA file.
                    NTweets+=1
                    if 'retweeted_status' not in tweet:             
                        nTweets+=1
                        writer = csv.writer(f)                       
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
                        #Purely to check that the softwre is finding all the tweets.  Check to see if your username is found
                        if db:                            
                            if tweet['user']['screen_name']==debugUser:
                                for NN in range(0,20):
                                    print("SEEN YOUR TWEET: %s" % (tweet['text']))
            percentTweets=100-(nTweets/NTweets*100)
            percentGeo=(nGeo/NTweets*100)
            print("reTweets Discarded: "+str(percentTweets))
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
                #Tell the user that your still gapfilling
                       print("%i tweets seen using %s about %s up to ID %i at %s. GapFilling..." % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14]))   

            else:
                if T0==0: 
                    #Which it WON'T BE if this is the last of the GapFilling iterations which means you shouldn't pause but the GapFillilng has already been set to 0.  This is a little inelegant but nexessary.
                  
                    #If < optimalNumberOfTweets obtained we need to pause this string to make the best use of the number of searches allowed. 
                    if len(response['content']['statuses'])<optimalNumberOfTweets:
                        print("%i tweets seen using %s about %s up to %i at %s. Pausing for %is " % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14],round(time_pause)))  
                        #And send that 'pause until' to the disk. 
                        with open(SearchString +'Pausing.csv','w') as f:
                            writer = csv.writer(f)
                            writer.writerow(['Searching for '+ SearchString+ ' paused until',ttime.time()+time_pause])
                    #Else don't pause but tell the user anyway and send 1 as the 'pause until' to the disk. 
                    else:
                        print("%i tweets seen using %s about %s up to with ID %i at %s. Not Pausing..." % (len(response['content']['statuses']),str(account[0]),SearchString,tweet['id'], tweet['created_at'][:-14]))   
                        with open(SearchString +'Pausing.csv','w') as f:
                            writer = csv.writer(f)
                            writer.writerow(['Searching for '+ SearchString+ ' paused until',1])
                            
        #No Tweets obtained so Pause that search for a while
        else:
            with open(SearchString +'Pausing.csv','w') as f:
                if GapFilling:
                    print("Gap Filled.....")   
                    writer = csv.writer(f)
                    writer.writerow(['Searching for '+ SearchString+ ' paused for some time until ',ttime.time()+0])
                
                else:
                    print("No tweets seen using %s about %s . Pausing for %i mins." % (str(account[0]),SearchString,DefaultWaitTimeNoTweets/60))   
                    writer = csv.writer(f)
                    writer.writerow(['Searching for '+ SearchString+ ' paused for some time until ',ttime.time()+DefaultWaitTimeNoTweets])
                
        
    except Exception as e:
#         print(type(inst))    # the exception instance
#         print(inst.args)      # arguments stored in .args
#         print(inst)
        print(e)
        print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e), e)
        
        #Exception is usually something to do with rate limiting or connection so just pause that search account for DefaultWaitTime and see what happens.
        sleeptime=ttime.time()+DefaultWaitTime
        try: 
            #If the response has a rate-limiting-number use that to pause the account.
            sleeptime=(float(response['meta']['x-rate-limit-reset'])-ttime.time()+1)
            print("Some unexpected errorrate limit reached.  Parking for %i minutes. " % ((float(response['meta']['x-rate-limit-reset'])+1-ttime.time())/60))
        except: 
             print("rate limit reached.  Parking for %i minutes. " % (sleeptime/60))
        #And send that to disk.                                                                  
        with open(account[0] +'Pausing.csv','w') as f:
                    writer = csv.writer(f)
                    writer.writerow(['Pause Until:' , str(sleeptime+1)]) 
        