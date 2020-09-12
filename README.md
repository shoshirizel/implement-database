# DataBase Exercise

Constructing a fully operational self-made database.
Differing relational and non-relational functionalities
within the same generic API and pre-written test cases using Python.
Involving run-time and I/O bound optimizations.


## Description

This is an implemention of a non relation DataBase,
The database supports creating and deleting tables.
Inserting deleting,updating and selecting records from an existing table.
also retrieve data from a table according to some criterions. (by query)
and creating an index to a specific field.

## Getting Started

### Installing
* python 3.8
* In requirements.txt file you can see what to install.
* You need to create a db_files directory to store your data.

### Executing program

In a python file you need to import db and create a DataBase by db.DataBase().
You can look at the db_api file to see the functions you can use.
```
db = db.DataBase()
table = db.create_table('Students', STUDENT_FIELDS, 'ID')
```



