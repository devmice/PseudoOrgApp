import selectors
import socket
from threading import Thread
import asyncio
import libs.messenger_client as msg_client


import logging as log

log.basicConfig(level=log.DEBUG)
sel = selectors.DefaultSelector()


class ClientApp:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def _start_read_loop(self):
        try:
            while True:
                events = sel.select(timeout=1)
                for key, mask in events:
                    message = key.data
                    try:
                        message.process_events(mask)
                    except Exception as Error:
                        print("Socket Error: exception for\n"+
                              f"{message.addr}:{Error}")
                        message.close()
                # Check for a socket being monitored to continue.
                if not sel.get_map():
                    break
        except Exception as Error:
            log.debug(f"Unknown exception, exiting Thread: {Error}")
        finally:
            sel.close()

    def create_request(self, action, value):
        if action == "read_table":
            return dict(
                type="text/json",
                encoding="utf-8",
                content=dict(action=action, value=value),
            )
        else:
            return dict(
                type="binary/custom-client-binary-type",
                encoding="binary",
                content=bytes(action + value, encoding="utf-8"),
            )

    def send_msg(self, request):
        """send message to the server
          *request = create_request(action, value)
        """
        self.addr = (self.host, self.port)
        log.debug(f"starting connection to {self.addr}")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(False)
        self.sock.connect_ex(self.addr)
        self.events = selectors.EVENT_READ | selectors.EVENT_WRITE
        message = msg_client.Message(sel, self.sock, self.addr, request)
        sel.register(self.sock, self.events, data=message)
        read_thread = Thread(target=self._start_read_loop(), 
                                       daemon=True)
        read_thread.start()

    async def on_message(message):
        return

def syntetic_load(client):
    print(type(client))
    request = client.create_request('read_table', "{'table' : 'organization'}")
    client.send_msg(request)
    print('something')


def main():
    client = ClientApp(host="127.0.0.1", port=8321 )
    threads = [Thread(target=syntetic_load, args=[client], daemon=True) for _ in range(1)]
    request = client.create_request('read_table', "{'table' : 'organization'}")
    client.send_msg(request) 
    #for thread in threads:
    #    thread.start()
    #while threads[-1].is_alive():
    #    1 -1 

if __name__ == "__main__":
    main()
