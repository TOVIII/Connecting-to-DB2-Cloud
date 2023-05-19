#!/usr/bin/env python
# coding: utf-8

# # Connect  to Db2 database on Cloud using Python

#  The objective of the report was to use python programming language to communicate with databases on Cloud by creating ,inserting  , updating and retriving  the data frame table from the  IBM Db2 cloud database.

# In[1]:


import ibm_db
import sqlalchemy


# In[2]:


dsn_hostname = "764264db-9824-4b7c-82df-40d1b13897c2.bs2io90l08kqb1od8lcg.databases.appdomain.cloud"
dsn_uid = "yjl93087"
dsn_pwd = "YPTftIMnGNW1JDrc"
dsn_driver = "{IBM DB2 ODBC DRIVER }"
dsn_database = "BLUDB"
dsn_port = "32536"
dsn_protocol = "TCPIP"
dsn_security = "SSL"


# In[3]:


# creating the dsn  connection string

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver , dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid , dsn_pwd, dsn_security)

print(dsn)


# In[4]:


try:
    conn = ibm_db.connect(dsn, "", "")
    print("Connected to database: ",dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

except:
    print("Unable to connect: ",ibm_db.conn_errormsg())


# In[5]:


#Retrieve Metadata for the Database Server
server= ibm_db.server_info(conn)

print("DBMS_NAME: ", server.DBMS_NAME)
print("DBMS_VER:  ",server.DBMS_VER)
print("DB_NAME:   ",server.DB_NAME)


# # Creating  a table in the database

# In[6]:


#  I will drop the table INSTRUCTOR in case it exists  from a previious attempt
dropQuery = "drop table INSTRUCTOR"

dropStmt = ibm_db.exec_immediate(conn,dropQuery)


# In[7]:


# The error will  occour .It just implies that the INSTRUCTOR table already exist.
createQuery =" create table INSTRUCTOR(ID INTEGER PRIMARY KEY NOT NULL ,FNAME VARCHAR (20) , LNAME VARCHAR(20), CITY VARCHAR(20),CCCODE CHAR(2))"

createStmt = ibm_db.exec_immediate(conn,createQuery)


# In[8]:


# DDL staement to insert values on the table

insertQuery = "Insert into  INSTRUCTOR values(1 , 'Rav' ,'Ahuja','TORONTO','CA')"

insertStmt= ibm_db.exec_immediate(conn,insertQuery)


# In[9]:


# Adding moreraws to the table
insertQuery2 = "Insert into INSTRUCTOR values(2 , 'Raul' , 'Chong', 'Markham' , 'CA'), (3,'Hima','Vasudevan','Chicago','US')"

insertSt2 = ibm_db.exec_immediate(conn,insertQuery2)


# # Querying data in the table
# 

# In[10]:


# Retrieving  all rows  from the INSTRUCTOR
selectQuery = "select * from INSTRUCTOR "

selectStmt = ibm_db.exec_immediate(conn,selectQuery)


# In[11]:


while ibm_db.fetch_row(selectStmt) !=False:
    print("ID:", ibm_db.result(selectStmt,0), "FNAME:", ibm_db.result(selectStmt, "FNAME"))


# In[12]:


updateQuery = "update INSTRUCTOR  set CITY = 'MOOSETOWN' where ID = '1'"

updateStmt = ibm_db.exec_immediate(conn,updateQuery)


# # Retrieving   data  into  Pandas

# In[13]:


# Libaries
import pandas 
import ibm_db_dbi


# In[14]:


# connecting to pandas
pconn = ibm_db_dbi.Connection(conn)


# In[15]:


# Retrieving  all rows in the INSTRUCTOR table

selectQuery = "select*From INSTRUCTOR"

pdf =pandas.read_sql(selectQuery,pconn)

pdf


# In[16]:


# The shape method to see how many rows and columns  are in the data frame
pdf.shape


# In[17]:


#Closing  the  connections so that we can avoid unused  connections taking up resources
ibm_db.close(conn)


# In[ ]:




