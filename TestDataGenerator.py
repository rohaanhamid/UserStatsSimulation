#!/usr/bin/python
#This file generates dest data and inserts it directly into the mailing table.
#Reads a csv file containing sample domain names from http://moz.com/top500/domains/csv, generates random email addresses based off of it and adds to the mailing list table
import sqlite3
import csv
import random
import string
from MailingAddrDataSource import *

class TestDataGenerator:   
    domain_list=[]
    def __init__(self):
        global domain_list
        domain_list=[]
        with open('top500.domains.01.14.csv', 'rb') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in csvreader:
                domain_list.append((row[1])[1:-2])
    
    #Adds 100-200 new email addresses each time the function is called
    #Takes reference to database from the main file            
    def addTestData(self, mailingAddrDataSource):
        x=random.randint(100, 200) #randomly generated loop bounds 
        for i in range(0,x):
            y=random.randint(0, 499) #Randomly choose one domain name  
            randomText = ''.join(random.sample(string.letters, 15))
            #print randomText + '@' + domain_list[y]
            mailingAddrDataSource.insertMailingAddress(randomText + '@' + domain_list[y])
            
    
    
            
