#!/usr/bin/python
#This file simulates the whole system for n_days days and then generates a report for the top 50 domains
import sqlite3, random, string
from MailingAddrDataSource import *
from TestDataGenerator import *
import threading
from CounterBackPort import Counter
from datetime import date, datetime, timedelta

#Function used for multithreading
def countDomainsInList(counterslist, list, t):
    list = [str(addr[0]).split('@')[1] for addr in list]
    counterslist[t]=Counter(list)

#Setup helper modules
mailingAddrDataSource=MailingAddrDataSource()
testDataGen = TestDataGenerator()

#Starting date
year=2014
month=1
day=1
startdate=date(year, month, day)
currentdate=startdate
print currentdate

#Set the total number of days the simulation is to be run for
n_days=75

#Each iteration of the loop is one day
for day in range(0,n_days):
    #For each day add some new data
    testDataGen.addTestData(mailingAddrDataSource);
    
    #Query the mailing address table. 
    c=mailingAddrDataSource.queryAllMailingAddresses()
    numaddresses=len(c.fetchall())
    print "Total number of email addresses on day" + str(day) + " :" + str(numaddresses)
    c.close()
    
    #Determine the number of results and spawn a thread pool to split the work for counting occurrences of each of the domain name
    #Assumption: It is assumed that the problem will always be of a significant size so some cases e.g the mailing table having less than 4 items are not handled
    numthreads=4
    worksize=numaddresses/numthreads
    remainder=numaddresses%numthreads
    counterslist=[]
    threadslist=[]
    
    c=mailingAddrDataSource.queryAllMailingAddresses()
    for t in range(0, numthreads):
        counterslist.append(Counter())
        res_list=[]
        if t < (numthreads-1):
            res_list=c.fetchmany(worksize)
        else: 
            res_list=c.fetchmany(worksize+remainder)
        thread=threading.Thread(target=countDomainsInList, args= (counterslist, res_list, t))
        threadslist.append(thread)
        thread.start()
        
    
    for thread in threadslist:
        thread.join()
    
    #Collapse results from each of the threads
    countingresults=Counter()
    for counter in counterslist:
        countingresults=countingresults+counter
        
    #Update the stats table
    print currentdate
    for e in countingresults.items():
        mailingAddrDataSource.insertDomainStats(e[0], e[1], currentdate)
    c.close()        
    
    currentdate=currentdate+timedelta(days=1)

#Generate Report for Top 50 Domains in the last month
c_latest = mailingAddrDataSource.queryAllStatsByDate(currentdate-timedelta(days=1))
c_30_old = mailingAddrDataSource.queryAllStatsByDate(currentdate-timedelta(days=30))
counter_latest=Counter()
counter_30_old=Counter()
for c in c_latest:
    counter_latest.update({str(c[0]): int(c[1])})
    
for c in c_30_old:
    counter_30_old.update({str(c[0]): int(c[1])})    


mostcommondomains=(counter_latest-counter_30_old).most_common(50) #This gets the top domains by percentage growth in the last 30 days  

#Print the results based on the actual total count of each website
print "\n*Top Domains this month with total hits ever recorded*\n"
for domain in mostcommondomains: #This gets the percentage growth in the last 30 days
    c=mailingAddrDataSource.queryDomainStatsByDate(domain[0], currentdate-timedelta(days=1))
    print c.fetchall()
    
  
#Finalize helper modules
mailingAddrDataSource.closeDB()

