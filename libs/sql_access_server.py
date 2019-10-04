"""
  *lib for DB read & write access from clients via tcp/selectors
  *
"""

#libs for DB server side 
import psycopg2 as psql
import logging as log
import sys


class DB:
    def __init__(self, host: str, db_name: str, 
                 user: str, password: str, db_tables: tuple, port=5432):
        """ DB read & write class 
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.db_tables = db_tables
        self.user = user
        self.password = password

    def connect(self):
        """start connection with the sql server
        """
        self.connection = psql.connect(host=self.host, port=self.port, 
                                       database=self.db_name, user=self.user,
                                       password=self.password)
        self.cursor = self.connection.cursor()
        return self.connection

    def _is_db_online(self):
        """Nothing yet
        """
        self.cursor.execute("select 1")

    def _is_table_exists(self, table_name):
        """check if table exists in db
        """
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

    def read_values(self, var_name, table_name):
        sql = f'SELECT {var_name} FROM {table_name};'
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
        value = data_base.read_values(table_name='organization', var_name=('name'))
        return data_base
    except(Exception, psql.DatabaseError) as Error:
            log.debug(f"DB Init Error...\n{Error}")
            return None



# libs for Server side
import io
import json
import types
import struct
import socket
import blinker
import selectors


on_request = blinker.signal('on_request')


class Server:
    def __init__(self, selector, sock, addr, data_base: DB):
        self.selector = selector
        self.sock = sock
        self.addr = addr
        self._recv_buffer = b""
        self._send_buffer = b""
        self._jsonheader_len = None
        self.jsonheader = None
        self.request = None
        self.response_created = False
        self.data_base = data_base
        if self.data_base is None:
            raise Exception('No data_base provided')

    def _set_selector_events_mask(self, mode):
        """Set selector to listen for events: mode is 'r', 'w', or 'rw'."""
        if mode == "r":
            events = selectors.EVENT_READ
        elif mode == "w":
            events = selectors.EVENT_WRITE
        elif mode == "rw":
            events = selectors.EVENT_READ | selectors.EVENT_WRITE
        else:
            raise ValueError(f"Invalid events mask mode {repr(mode)}.")
        self.selector.modify(self.sock, events, data=self)

    def _read(self):
        try:
            data = self.sock.recv(4096)
        except BlockingIOError:
            # Resource temporarily unavailable
            pass
        else:
            if data:
                self._recv_buffer += data
            else:
                raise RuntimeError("Peer closed.")

    def _write(self):
        if self._send_buffer:
            print("sending", repr(self._send_buffer), "to", self.addr)
            try:
                # Should be ready to write
                sent = self.sock.send(self._send_buffer)
            except BlockingIOError:
                # Resource temporarily unavailable (errno EWOULDBLOCK)
                pass
            else:
                self._send_buffer = self._send_buffer[sent:]
                # Close when the buffer is drained. The response has been sent.
                if sent and not self._send_buffer:
                    self.close()

    def _json_encode(self, obj, encoding):
        return json.dumps(obj, ensure_ascii=False).encode(encoding)

    def _json_decode(self, json_bytes, encoding):
        tiow = io.TextIOWrapper(
            io.BytesIO(json_bytes), encoding=encoding, newline=""
        )
        obj = json.load(tiow)
        tiow.close()
        return obj

    def _create_message(
        self, *, content_bytes, content_type, content_encoding
    ):
        jsonheader = {
            "byteorder": sys.byteorder,
            "content-type": content_type,
            "content-encoding": content_encoding,
            "content-length": len(content_bytes),
        }
        jsonheader_bytes = self._json_encode(jsonheader, "utf-8")
        message_hdr = struct.pack(">H", len(jsonheader_bytes))
        message = message_hdr + jsonheader_bytes + content_bytes
        return message


    def _create_response_binary_content(self):
        response = {
            "content_bytes": b"First 10 bytes of request: "
            + self.request[:10],
            "content_type": "binary/custom-server-binary-type",
            "content_encoding": "binary",
        }
        return response

    def process_events(self, mask):
        if mask & selectors.EVENT_READ:
            self.read()
        if mask & selectors.EVENT_WRITE:
            self.write()

    def read(self):
        self._read()

        if self._jsonheader_len is None:
            self.process_protoheader()

        if self._jsonheader_len is not None:
            if self.jsonheader is None:
                self.process_jsonheader()

        if self.jsonheader:
            if self.request is None:
                self.process_request()

    def write(self):
        response = None
        if self.request:
            response = on_request.send(self, request=self.request)
        else:
            response = self._create_response_binary_content()
        self._send_buffer += self._create_message(**response[0][1])
        self.response_created = True
        self._write()

    def close(self):
        """Close connection to the client
        """
        print("closing connection to", self.addr)
        try:
            self.selector.unregister(self.sock)
        except Exception as e:
            print(
                f"error: selector.unregister() exception for",
                f"{self.addr}: {repr(e)}",
            )

        try:
            self.sock.close()
        except OSError as e:
            print(
                f"error: socket.close() exception for",
                f"{self.addr}: {repr(e)}",
            )
        finally:
            # Delete reference to socket object for garbage collection
            self.sock = None

    def process_protoheader(self):
        hdrlen = 2
        if len(self._recv_buffer) >= hdrlen:
            self._jsonheader_len = struct.unpack(
                ">H", self._recv_buffer[:hdrlen]
            )[0]
            self._recv_buffer = self._recv_buffer[hdrlen:]

    def process_jsonheader(self):
        hdrlen = self._jsonheader_len
        if len(self._recv_buffer) >= hdrlen:
            self.jsonheader = self._json_decode(
                self._recv_buffer[:hdrlen], "utf-8"
            )
            self._recv_buffer = self._recv_buffer[hdrlen:]
            for reqhdr in (
                "byteorder",
                "content-length",
                "content-type",
                "content-encoding",
            ):
                if reqhdr not in self.jsonheader:
                    raise ValueError(f'Missing required header "{reqhdr}".')

    def process_request(self):
        content_len = self.jsonheader["content-length"]
        if not len(self._recv_buffer) >= content_len:
            return
        data = self._recv_buffer[:content_len]
        self._recv_buffer = self._recv_buffer[content_len:]
        if self.jsonheader["content-type"] == "text/json":
            encoding = self.jsonheader["content-encoding"]
            self.request = self._json_decode(data, encoding)
            print("received request", repr(self.request), "from", self.addr)
        else:
            # Binary or unknown content-type
            self.request = data
            print(
                f'received {self.jsonheader["content-type"]} request from',
                self.addr,
            )
        # Set selector to listen for write events, we're done reading.
        self._set_selector_events_mask("w")


