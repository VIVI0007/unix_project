import threading

import subprocess

import mysql.connector

from time import sleep



#Enter the details of MySQL connection

pool_config = {

    "pool_name": "my_pool",

    "pool_size": 6,

    "host": "192.168.0.4",

    "user": "backup",

    "password": "Backup#123",

    "database": "backup_data",

}



# Create a connection pool

connection_pool = mysql.connector.pooling.MySQLConnectionPool(**pool_config)



# Thread synchronization using a semaphore

connection_semaphore = threading.Semaphore(pool_config["pool_size"])

backup_restore_lock = threading.Lock()

restore_lock = threading.Lock()



#Method to backup each table to a specific file using mysqldump shell script command

def backup_table(table):

        try:

                subprocess.run(f"mysqldump -h 192.168.0.4 -u backup -pBackup#123 backup_data {table} > backup_{table}.sql", stdout = subprocess.PIPE, shell=True, universal_newlines=True)

                print(f"Table {table} backed up successfully.")



        except subprocess.CalledProcessError as e:

                print(f"Error backing up table {table}: {e}")

        sleep(1)



def backup():

        while(1):

                connection_semaphore.acquire()

                #Obtain a connection from the connection pool

                connection = connection_pool.get_connection()

                cursor = connection.cursor()



                #Get the number of tables available in the database

                table_count_query = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE()"

                cursor.execute(table_count_query)

                table_count = cursor.fetchall()[0][0]



                #Get the names of the tables available in the database

                table_name_query = "SHOW TABLES"

                cursor.execute(table_name_query)

                table_name = list(map(lambda x:x[0], cursor.fetchall()))



                #Create a list to store all the threads

                threads = list()

                backup_restore_lock.acquire()

                for table in table_name:

                        #Create a thread to backup each table respectfully

                        thread = threading.Thread(target=backup_table, args=(table,))

                        threads.append(thread)

                        thread.start()



                for thread in threads:

                        #Join the created threads to the Primary thread

                        thread.join()



                #Close the cursor and connection after use

                cursor.close()

                connection.close()

                print("All Tables backed up!");

                connection_semaphore.release()

                backup_restore_lock.release()

                sleep(3)



def restore_table(table, sql_file):

        connection_semaphore.acquire()

        #obtain a connection from connection pool

        connection = connection_pool.get_connection()

        cursor = connection.cursor()



        try:

                with open(sql_file, 'r') as sql_file:

                        queries = sql_file.read().split(';')

                        for query in queries:

                                if query.strip():

                                        cursor.execute(query)



                connection.commit()

                print(f"Table {table} restored successfully.")



        except Exception as e:

                print(f"Error restoring table {table}: {str(e)}")



        cursor.close()

        connection.close()

        connection_semaphore.release()

        restore_lock.release()

        sleep(1)



def restore():

        while(1):

                # Run the 'ls' command with 'grep' to list .sql files in the current directory

                command = "ls | grep '.sql$'"

                result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, text=True)



                # Get the output of the command and split it into a list of filenames

                sql_files = result.stdout.strip().split('\n')



                #thread list to store thread ids

                threads = []

                backup_restore_lock.acquire()

                for sql_file in sql_files:

                        restore_lock.acquire()

                        #obtain the table name from the name of the file

                        table_name = sql_file.split('.')[0]

                        #Create a thread to restore each table

                        thread = threading.Thread(target=restore_table, args=(table_name, sql_file))

                        threads.append(thread)

                        thread.start()



                for thread in threads:

                        thread.join()



                print("All tables restored.")

                backup_restore_lock.release()

                sleep(10)



def main():



        backup_thread = threading.Thread(target=backup)

        restore_thread = threading.Thread(target=restore)



        backup_thread.start()

        restore_thread.start()



        backup_thread.join()

        restore_thread.join()



if __name__ == "__main__":

        main()

