# DataBase Exercise

Constructing a fully operational self-made database.
Differing relational and non-relational functionalities
within the same generic API and pre-written test cases using Python.
Involving run-time and I/O bound optimizations.


## Description

This is an implemention of a non relation DataBase,
You can create a collection and do some actions on it.
Also you can create a hash index ro improve the run time.

## Getting Started



### Dependencies

* Describe any prerequisites, libraries, OS version, etc., needed before installing program.
* ex. Windows 10

### Installing

* In requirements.txt file you can see what to install.
* You need to create a db_files directory to store your data.

### Executing program

In a python file you need to import db and create a DataBase by db.DataBase().
You can look at the db_api file to see the functions you can use.
```
db = db.DataBase()
table = db.create_table('Students', STUDENT_FIELDS, 'ID')
```



