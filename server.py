import psycopg2 as psql
import asyncio


class DB:
    def __init__(self, host: str, db_name: str, 
                 user: str, password: str, port=5432):
        """ DB class 
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password

    def connect(self):
        self.connection = psql.connect(host=self.host, port=self.port, 
                                       database=self.db_name, user=self.user,
                                       password=self.password)
        self.cursor = self.connection.cursor()
        return self.connection
    
    def _is_table_exists(self, table_name):
        self.cursor.execute("select exists(select * from information_schema.tables where table_name=%s)", (table_name,))
        return(bool(self.cursor.rowcount))

    def create_table(self, table_name: str, vars: dict):
        """create table
            table_name : str
            vars : dict - {"id" : "INT PRIMARY KEY     NOT NULL", "NAME" : "TEXT", AGE - INT}
        """
        if not self._is_table_exists(table_name):
            str2exec = f'CREATE TABLE {table_name} ('
            current_key = 1
            for key in vars:
                str2exec += f"{key} {vars[key]}"
                if current_key < len(vars):
                    str2exec += ","
                elif current_key == len(vars):
                    str2exec += ");"
                current_key += 1
            self.connection.cursor().execute(str2exec)
            self.connection.commit()
            return True
        else:
            return False


def main():
    p_db = DB("172.17.0.2", "postgres",
              "postgres", "secret")
    cursor = p_db.connect().cursor()
    cursor.execute("select 1")
    p_db.create_table('organization', {'id': "INT PRIMARY KEY NOT NULL",
                               'name': "TEXT NOT NULL",
                               'uni_code': "INT NOT NULL"})
    
    print("i'm in:")
    return

if __name__ == "__main__":
    main()
