"""
  *Server instance code
"""

from libs.sql_access_server import *
import logging as log
import asyncio
import traceback
import selectors
import struct

log.basicConfig(level=log.DEBUG)
sel = selectors.DefaultSelector()


class ServerApp:
    def __init__(self, addr, data_base: DB):
        self.addr = addr
        self.data_base = db_init(data_base)
        if self.data_base is None:
            raise Exception(f'DB init error')
        self.response = None
        self.server = None

    async def accept_wrapper(self, sock):
        conn, addr = sock.accept()
        print("accepted connection from", addr)
        conn.setblocking(False)
        message = Server(sel, conn, addr, self.data_base)
        sel.register(conn, selectors.EVENT_READ, data=message)

    async def run_server(self):
        lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        lsock.bind(self.addr)
        lsock.listen()
        log.debug(f"listening on {self.addr}")
        lsock.setblocking(False)
        sel.register(lsock, selectors.EVENT_READ, data=None)
        try:
            while True:
                events = sel.select(timeout=None)
                for key, mask in events:
                    if key.data is None:
                        await self.accept_wrapper(key.fileobj)
                    else:
                        message = key.data
                        try:
                            message.process_events(mask)
                        except Exception as Error:
                            print(
                                "main: error: exception for",
                                f"{message.addr}:\n{Error}",
                            )
                            message.close()
        except Exception as Error:
            log.error(Error)
        finally:
            sel.close()

@on_request.connect
def on_request(sender, **kw):
    response = None
    if kw['request']['action'] == 'read_table':
        answer = sender.data_base.read_values(table_name='organization', var_name=('*'))
        response = {"result": answer}
    else:
        response = {"result": f'Error: invalid action "{action}".'}
    content_encoding = "utf-8"
    response = {
        "content_bytes": sender._json_encode(response, content_encoding),
        "content_type": "text/json",
        "content_encoding": content_encoding,
    }
    return response


def main():
    db_tables = ('organization', 'department', 'person')
    data_base = DB(host="172.17.0.2", db_name="postgres",
                   user="postgres", password="secret", db_tables=db_tables)
    server_addr = ('127.0.0.1', 8321)
    loop = asyncio.get_event_loop()
    server = ServerApp(server_addr, data_base)
    loop.create_task(server.run_server())
    loop.run_forever()
    loop.close()

    #server = Server(host="127.0.0.1", port=8321, data_base=data_base, max_clients=10)
    #print(server)
    #server.server_loop()

if __name__ == "__main__":
    main()
