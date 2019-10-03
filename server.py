
import selectors
import socket
import psycopg2 as psql
import threading
import types
import time
import logging as log
import traceback
log.basicConfig(level=log.DEBUG)


class DB:
    def __init__(self, host: str, db_name: str, 
                 user: str, password: str, db_tables: tuple, port=5432):
        """ DB class 
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.db_tables = db_tables
        self.user = user
        self.password = password

    def connect(self):
        self.connection = psql.connect(host=self.host, port=self.port, 
                                       database=self.db_name, user=self.user,
                                       password=self.password)
        self.cursor = self.connection.cursor()
        return self.connection

    def _is_db_online(self):
        self.cursor.execute("select 1")

    def _is_table_exists(self, table_name):
        self.cursor().execute(f"SELECT * FROM information_schema.tables WHERE table_name={table_name};")
        self.cursor().commit()
        if self.cursor.fetchone()[0] == 1:
            return True
        return False

    def _execute_sql(self, sql: str):
        try:
            self.cursor.execute(sql)
            self.connection.commit()
            return self.cursor.fetchone()
        except(Exception, psql.DatabaseError) as Error:
            log.debug(f"DB execution Error...\n{Error}")
            return None

    def read_value(self, var_name, table_name):
        sql = f'SELECT * FROM {table_name};'
        return self._execute_sql(sql)

    def insert_value(self, table_name: str, var_name, value):
        """table_name = str - name of the table
          *var_to_insert_value = str - name of the var
          *value = ? - value
          *out_var_name = - value which would be returned (id or uni_code)
        """
        if table_name in self.db_tables:
            
            sql = f"""INSERT INTO {table_name}({var_name}) VALUES ({value});"""
            return self._execute_sql(sql)

    def fill_row(self):
        return

    def read_row(self):
        return

    def delete_table(self, table_name):
        self._execute_sql(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;')
        return True

    def create_table(self, table_name: str, vars: dict):
        """create table
            table_name : str
            vars : dict - {"id" : "INT PRIMARY KEY     NOT NULL", "NAME" : "TEXT", AGE - INT}
        """
        if table_name in self.db_tables:
            sql = f'CREATE TABLE IF NOT EXISTS {table_name} ('
            current_key = 1
            for key in vars:
                sql += f"{key} {vars[key]}"
                if current_key < len(vars):
                    sql += ","
                elif current_key == len(vars):
                    sql += ");"
                current_key += 1
            self._execute_sql(sql)
            return True
        else:
            return False


def db_init(data_base: DB):
    
    data_base.connect()
    try:
        for table in data_base.db_tables:
            data_base.delete_table(table)

        data_base.create_table('organization', { 
                        'id': "SERIAL PRIMARY KEY",
                        'name': "TEXT NOT NULL",
                        'uni_code': "INT NOT NULL",
                        'department_uni_codes': "integer[]"})   
        data_base.create_table('department', {
                        'id': "SERIAL PRIMARY KEY",    
                        'name': "TEXT NOT NULL",
                        'uni_code': "INT NOT NULL",
                        'persons_uni_codes': "integer[]"})
        data_base.create_table('person', {
                        'id': "SERIAL PRIMARY KEY",
                        'name': "TEXT NOT NULL",
                        'birth_day': "DATE NOT NULL",
                        'salary_month_USD': "INT",
                        'uni_code': "INT NOT NULL"})
        data_base.insert_value(table_name='organization', var_name=('name, uni_code, department_uni_codes'), value=("'Corp', 12345, ARRAY[12345]"))
        value = data_base.read_value(table_name='organization', var_name=('name'))
        return True
    except(Exception, psql.DatabaseError) as Error:
            log.debug(f"DB Init Error...\n{Error}")
            return False


class Server:
    def __init__(self, host: str, port: int, data_base: DB, max_clients: int):
        self.db = data_base

        self.main_socket = socket.socket()
        self.main_socket.bind((host, port))
        self.main_socket.listen(100)
        self.main_socket.setblocking(False)

        self.selector = selectors.DefaultSelector()
        self.selector.register(fileobj=self.main_socket,
                               events=selectors.EVENT_READ,
                               data=self.on_accept)
        
        self.current_peers = {}

    def on_accept(self, sock, mask):
        try:
            conn, addr = self.main_socket.accept()
            log.info(f'accepted connection from {addr}')
            conn.setblocking(False)
            self.current_peers[conn.fileno()] = conn.getpeername()
            self.selector.register(fileobj=conn, events=selectors.EVENT_READ,
                                data=self.on_read)
        except KeyError as Error:
            log.debug(Error)

    def close_connection(self, conn):

        peername = self.current_peers[conn.fileno()]
        log.info(f'closing connection to {peername}')
        del self.current_peers[conn.fileno()]
        conn.close()

    def on_read(self, conn, mask):

        try:
            data = conn.recv(1000)
            if data:
                peername = conn.getpeername()
                log.info(f'got data from {peername}: {data}')
                conn.send(data)
            else:
                self.close_connection(conn)
        except ConnectionResetError:
            self.close_connection(conn)

    def server_loop(self):
        last_report_time = time.time()
        while True:
            events = self.selector.select(timeout=0.2)

            for key, mask in events:
                handler = key.data
                handler(key.fileobj, mask)
            cur_time = time.time()
            if cur_time - last_report_time > 1:
                log.info('Running report...')
                log.info(f'Num active peers = {len(self.current_peers)}')
                last_report_time = cur_time


def main():
    db_tables = ('organization', 'department', 'person')
    data_base = DB(host="172.17.0.2", db_name="postgres",
                   user="postgres", password="secret", db_tables=db_tables)    
    db_init(data_base)
    #server = Server(host="127.0.0.1", port=8321, data_base=data_base, max_clients=10)
    #print(server)
    #server.server_loop()

if __name__ == "__main__":
    main()
