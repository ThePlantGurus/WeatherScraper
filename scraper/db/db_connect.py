import pyodbc

def db_connect_f(username, password):
    connection = pyodbc.connect(Driver='{SQL SERVER}',
                                Server='CORTS-SERVER\GURU',
                                Database='SM',
                                Trusted_connection='no',
                                UID=username,
                                PWD=password)
    connection.timeout = 86400
    connection = connection.cursor()
    return connection