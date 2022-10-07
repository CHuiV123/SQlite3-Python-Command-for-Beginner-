#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Sep 29 15:36:43 2022
For anyone who wish to perform SQlite3 DDL,DML,DQL using python 
@author: angela
"""

#%%
from sqlite3 import errorcode 
from sqlite3 import Error 
import sqlite3 

#%%

# To Create Database 
con = sqlite3.connect('Sample_DB')

# To Create a cursor to execute command 
c = con.cursor()

# To Create a Table 
c.execute('''CREATE TABLE Customer_TBL (CustomerID INTEGER NOT NULL PRIMARY KEY,
                                     CustomerName VARCHAR NOT NULL,
                                     JobPosition VARCHAR,
                                     CompanyName VARCHAR NOT NULL,
                                     USState VARCHAR NOT NULL,
                                     ContactNo BIGINTEGER NOT NULL )''');
#%%
# To Alter a Table 
c.execute('''ALTER TABLE Customer_TBL ADD CompanyADD VARCHAR''');
#%%
# To Drop existing Table 
c.execute('''DROP TABLE Customer_TBL''');
#%%
# Th Recreate the Table for following usage 
c.execute('''CREATE TABLE Customer_TBL (CustomerID INTEGER NOT NULL,
                                     CustomerName VARCHAR NOT NULL,
                                     JobPosition VARCHAR,
                                     CompanyName VARCHAR NOT NULL,
                                     USState VARCHAR NOT NULL,
                                     ContactNo BIGINTEGER NOT NULL )''');
#%%
# To Insert Record
c.execute('''
          INSERT INTO Customer_TBL (CustomerID,CustomerName,JobPosition,CompanyName,USState,ContactNo) 
          VALUES (1,'Kathy Ale','President','Tile Industrial','TX',3461234567)''');

# For every insert action, you will need to commit it 
con.commit()
#%%
# To Insert Another Record
c.execute('''
          INSERT INTO Customer_TBL 
          VALUES (2,'Kevin Lord','VP','Best Tooling','NY',5181234567)''');
con.commit()

#%% 
# To Insert Multiple Records 
c.execute('''
          INSERT INTO Customer_TBL 
          VALUES
          (3,'Kim ASH','Director','Car World','CA',5101234567),
          (4,'Abby Karr','Manager','West Mart','NV',7751234567)''');
con.commit()

#%%
# To Insert only selected column with job position blank 
# Job position can accept null value becuase when we create table, we do not state NOT NULL 

c.execute('''
          INSERT INTO Customer_TBL (customerID,CustomerName,CompanyName,USState,ContactNo)
          VALUES(5,'Mike Armhs','1 Driving School','NJ',2011234567)''');
con.commit()

#%%
# To Update the Job Position of Mike into the table 
c.execute('''
          UPDATE Customer_TBL
          SET JobPosition ='VP'
          WHERE CustomerName='Mike Armhs'
          ''');
con.commit()

#%% 
# To modify the VP into Vice-President 
c.execute('''
          UPDATE Customer_TBL
          SET JobPosition = 'Vice-President'
          WHERE JobPosition = 'VP'
          ''');
con.commit()

#%% 
# modify all records at a time > changing customer names to uppercase 
c.execute('''
          UPDATE Customer_TBL
          SET CustomerName = UPPER(CustomerName)
          ''')
con.commit()
#%%
# To create backup of SQLite database from Python 
def progress(status, remaining, total): 
    print(f'Copied{total - remaining} of {total} pages...')
try: 
    # existing DB 
    con = sqlite3.connect('Sample_DB') 
    # copy into this DB 
    backcon = sqlite3.connect('Sample_DB_backup')
    with backcon: 
        con.backup(backcon, progress=progress)
    print('backup sucessful')
except sqlite3.Error as error: 
    print('Error while taking backup: ',error)
finally: 
    if backcon: 
        backcon.close()
        con.close()
#%%
# To delete one record from the table 
bc = sqlite3.connect('Sample_DB_backup')
c = bc.cursor 

bc.execute('''
           DELETE FROM Customer_TBL
           WHERE CustomerName = 'KATHY ALE'
           ''');
bc.commit()

#%%
# To delete multiple records from the table 
bc.execute('''
           DELETE FROM Customer_TBL
           WHERE JobPosition = 'Vice-President'
           ''');
bc.commit()

#%%
# To fetch all data from the table by row 
con = sqlite3.connect('Sample_DB')


def fetch_tbl (con): 
    c = con.cursor()
    c.execute('''
              SELECT * FROM Customer_TBL
              ''') 
    rows = c.fetchall()
    for row in rows: 
        print(row)

fetch_tbl(con)
#%% 
# to do query 

def query_tbl (con): 
    c = con.cursor()
    c.execute('''
              SELECT * FROM Customer_TBL 
              WHERE JobPosition = 'Vice-President'
              ''');
    rows = c.fetchall()
    for row in rows: 
        print(row)
query_tbl(con)
#%% 
# ascending / desc ORDER BY a condition 
def sort_tbl (con): 
    c = con.cursor()
    c.execute('''
          SELECT * FROM Customer_TBL
          ORDER BY USState DESC
          ''');
    rows = c.fetchall()
    for row in rows: 
        print (row)
sort_tbl(con)
#%%
# using Group By 
def group_tbl (con): 
    c = con.cursor()
    c.execute('''
              SELECT JobPosition, COUNT(*) AS Number_of_record
              FROM Customer_TBL
              GROUP BY JobPosition
              ''');
    rows = c.fetchall()
    for row in rows: 
        print(row) 
group_tbl(con)
#%%
# using group by and order by 
def group_order (con) : 
    c = con.cursor()
    c.execute('''
              SELECT JobPosition, Count(*) AS number_of_record 
              FROM Customer_TBL
              GROUP BY JobPosition 
              ORDER BY JobPosition DESC
              ''');
    rows = c.fetchall()
    for row in rows: 
        print(row)
group_order(con) 
#%% to insert a new record with rollback 
# note: rollback cannot be use as a standalone command in python sqlite3 

try: 
    # Connecting to database 
    con = sqlite3.connect('Sample_DB')
    c = con.cursor()
    c.execute('''
          INSERT INTO Customer_TBL
          VALUES
          (6, 'JOHN DEPP','President','Rockers Mine Company','TX',3467654321)
          '''); 
    con.commit()
    
    #update sucessful message 
    print('Database Updated Sucessfully!')
    
    
except sqlite3.connector.Error as error: 
    
    # update failed message as an error 
    print('Database Update Failed!: {}'.format())
    
    #reverting changes because of exception 
    con.rollback()
#%%




                   