#!/usr/bin/python
#This file contains the helper class for database operations
import sqlite3

class MailingAddrDataSource:
    conn=sqlite3.Connection
    
    def __init__(self):
        global conn
        conn = sqlite3.connect('casale_test.db')
        c = conn.cursor()
        c.execute('''DROP TABLE if exists mailing''')
        c.execute('''DROP TABLE if exists stats''')
        c.execute('''CREATE TABLE if not exists mailing(addr VARCHAR(255) NOT NULL)''')
        c.execute('''CREATE TABLE if not exists stats(domain VARCHAR(255) NOT NULL, dailycount INT, date DATE)''')
    
    def closeDB(self):
       conn.commit()
       conn.close()
       
    
    def queryAllMailingAddresses(self):
        c = conn.cursor()
        c.execute('''SELECT * FROM mailing''')
        return c;
        
    def insertMailingAddress(self, email):
        c = conn.cursor()
        c.execute('INSERT INTO mailing (addr) values(?)', (email,))
        #conn.commit()
        
    def insertDomainStats(self, domain, totalcount, date):
        c = conn.cursor()
        c.execute('INSERT INTO stats(domain, dailycount, date) values(?,?,?)', (domain, totalcount, date))
        
    def queryAllStatsByDate(self, date):
        c = conn.cursor()
        c.execute('SELECT domain, dailycount FROM stats where date=?', (date,))    
        return c
    
    def queryDomainStatsByDate(self, domain, date):
        c = conn.cursor()
        c.execute('SELECT domain, dailycount FROM stats where domain=? AND date=?', (domain, date))    
        return c
        

        