#coding=utf-8

import socket
import threading
import sqlite3
import time
import re

BAD_REQUEST_RESP = bytes('HTTP/1.1 400 Bad Request\r\n\r\n', encoding='utf-8')
NOT_FOUND_RESP = bytes('HTTP/1.1 404 Not Found\r\n\r\n', encoding='utf-8')
BAD_GATEWAY_RESP = bytes('HTTP/1.1 502 Bad Gateway\r\n\r\n', encoding='utf-8')
NOT_IMPL_RESP = bytes('HTTP/1.1 501 Not Implemented\r\n\r\n', encoding='utf-8')
LISTEN_ADDRESS = ('127.0.0.1', 5678)
# LISTEN_ADDRESS = ('0.0.0.0', 5678)


class fmdb:
    @staticmethod
    def init_db():
        conn = sqlite3.connect('fm.db')
        cur = conn.cursor()
        cur.execute('create table fm(id integer primary key autoincrement not null, uri text not null,  name text not null, type text not null, country text not null, bits integer not null, favorite integer not null)')
        conn.commit()
        conn.close()

    @staticmethod
    def __get_connection():
        return sqlite3.connect('fm.db')


    @staticmethod
    def __mark_favorite(rid: int, favorite: bool):
        conn = fmdb.__get_connection()
        cur = conn.cursor()
        cur.execute('UPDATE fm SET favorite=? WHERE id=?', (1 if favorite else 0, rid))
        conn.commit()
        conn.close()

    @staticmethod
    def add_radio(url: str, name: str, type_text: str, country: str, bits: int, favorite: bool):
        conn = fmdb.__get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT INTO fm (uri, name, type, country, bits, favorite) VALUES (?,?,?,?,?,?)', (url, name, type_text, country, bits, 1 if favorite else 0))
        conn.commit()
        conn.close()


    @staticmethod
    def del_radio(rid: int):
        conn = fmdb.__get_connection()
        cursor = conn.cursor()
        cursor.execute('DELETE FROM fm WHERE id=?', (rid, ))
        conn.commit()
        conn.close()


    @staticmethod
    def update_radio(rid: int, url: str, name: str, type_text: str, country: str, bits: int, favorite: bool):
        conn = fmdb.__get_connection()
        cursor = conn.cursor()
        cursor.execute('UPDATE fm SET uri=?, name=?, type=?, country=?, bits=?, favorite=? WHERE id=?', (url, name, type_text, country, bits, 1 if favorite else 0, rid))
        conn.commit()
        conn.close()

    @staticmethod
    def radio_list():
        ret_list = []
        conn = fmdb.__get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM fm')
        result = cursor.fetchall()
        for row in result:
            ret_list.append(row)
        conn.close()
        return ret_list

    @staticmethod
    def get_radio(rid: int):
        conn = fmdb.__get_connection()
        cursor = conn.cursor()
        result = cursor.execute('SELECT * FROM fm WHERE id=?', (rid, )).fetchall()
        conn.close()
        return result
    
    @staticmethod
    def mark_favorite(rid):
        fmdb.__mark_favorite(rid, True)
    @staticmethod
    def mark_unfavorite(rid):
        fmdb.__mark_favorite(rid, False)



class proxy_session:
    def __init__(self, client_socket: socket.socket):
        self.so_cli = client_socket
        self.so_svr = None
        self.radio_id = 0
        self.radio = None

    def __del__(self):
        print("Session has been closed !")

    def __thread__(self):
        print("ProxySession running .... ", self.so_cli.getpeername())
        try:
            self.__main()
        except Exception as e:
            print(e)
            pass
    def __main(self):
        # receiving http request ...
        if not self.__wait_request():
            print("Bad request !", self.so_cli.getpeername())
            self.so_cli.send(BAD_REQUEST_RESP)
            return 
        # get radio information
        self.radio = fmdb.get_radio(self.radio_id)
        if len(self.radio) == 0:
            print("Radio is not exist !", self.so_cli.getpeername())
            self.so_cli.send(NOT_FOUND_RESP)
            return 
        self.radio = self.radio[0]
        # connect to radio server
        if not self.__do_connect_server():
            print("Couldn't connect to radio server ... !")
            self.so_cli.send(BAD_GATEWAY_RESP)
            return 
        # send request
        self.__send_request_to_server()
        # forwarding
        self.__forward()

    def __wait_request(self):
        try:
            buf = str(self.so_cli.recv(4096), encoding='utf-8')
            if buf.find("\r\n\r\n") == -1:
                return False
            req_line = str(buf[0: buf.find("\r\n")])
            req_sp = req_line.split(' ')
            method = req_sp[0]
            uri = req_sp[1]
            if method != 'GET':
                return False
            if uri[0] != '/':
                return False
            self.radio_id = int(uri[1:])
            return True
        except:
            return False
        
    def __do_connect_server(self):
        self.so_svr = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # fetch domain and port
        url = self.radio[1]
        match_obj = re.match(r'^(http|https)://(.*?)/.*$', url)
        if None == match_obj:
            return False
        host = match_obj.group(2)
        port = 80
        if host.find(':') != -1:
            match_obj = re.match(r'^(.*?):(\d+)', host)
            if None == match_obj:
                return False
            host = match_obj.group(1)
            port = int(match_obj.group(2))
        try:
            self.host = host
            self.so_svr.connect((host, port))
            return True
        except:
            return False

    
    def __send_request_to_server(self):
        url = self.radio[1]
        mObj = re.match(r'^(http|https)://.*?(/.*)$', url)
        if None == mObj:
            return False
        uri = mObj.group(2)
        req = 'GET {} HTTP/1.1\r\nHost: {}\r\nConnection: keep-alive\r\nDNT: 1\r\nAccept-Encoding: identity\r\nUser-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36\r\nAccept: */*\r\nReferer: {}\r\nRange: bytes=0-\r\n\r\n'.format(uri, self.host, self.radio[1])
        req_bytes = bytes(req, encoding='utf-8')
        self.so_svr.send(req_bytes)
        return True
    
    def __forward(self):
        buf = self.so_svr.recv(4096)
        cache = bytearray(0)

        # check response header complete !
        tmp = str(buf)
        pos_hdr = tmp.find("\\r\\n\\r\\n")
        if pos_hdr == -1:
            # Bad response
            self.so_cli.send(NOT_IMPL_RESP)
            return

        # searching Transfer-Encoding
        pos = tmp.find("Transfer-Encoding: chunked")
        if pos == -1:
            self.so_cli.send(buf)
            self.__bypass()
            return 
        
        # make response
        resp = bytes('HTTP/1.1 200 OK\r\nContent-Type: audio/mpeg\r\nicy-br:{}\r\nicy-charset:UTF-8\r\nicy-description:(C)2019 ETS2 FM Proxy\r\nicy-name:{}\r\nicy-public:0\r\nServer: Limecast 2.0.0\r\n\r\n'.format(self.radio[2], self.radio[5]), encoding='utf-8')
        self.so_cli.send(resp)
        # decode and bypass
        cache = bytearray(0)
        buf = buf[pos_hdr + 4:]
        cache += buf

        '''
        0: chunked_size
        1: chunked_size_n
        2: body
        3. end_n
        '''
        state = 0
        chunked_size_str = ""
        chunked_size = 0
        chunked_transfered = 0
        body = bytearray(0)
        cache = bytearray(0)

        while True:
            while len(cache) > 0:
                if state == 0:
                    ch = chr(cache.pop(0))
                    if ch == '\r':
                        state = 1
                        chunked_size = int(chunked_size_str, 16)
                        chunked_transfered = 0
                        chunked_size_str = ""
                    else:
                        chunked_size_str += ch
                elif state == 1:
                    ch = chr(cache.pop(0))
                    if ch != '\n':
                        return 
                    state = 2
                    body = bytearray(0)
                elif state == 2:
                    if chunked_transfered < chunked_size:
                        body.append(cache.pop(0))
                        chunked_transfered+=1
                    elif chunked_transfered == chunked_size:
                        ch = chr(cache.pop(0))
                        if ch != '\r':
                            return
                        else:
                            state = 3
                    else:
                        return
                elif state == 3:
                    ch = chr(cache.pop(0))
                    if ch != '\n':
                        return 
                    # forward message
                    self.so_cli.send(body)
                    # reset
                    state = 0
                    chunked_size_str = ""
                    chunked_size = 0
                    chunked_transfered = 0
                    body = bytearray(0)
                else:
                    return 
            buf = self.so_svr.recv(4096)
            if buf == None or len(buf) == 0:
                return
            cache += buf



    def __bypass(self):
        while True:
            self.so_cli.send(self.so_svr.recv(4096))
        
    
    def run(self):
        self.thread = threading.Thread(target=self.__thread__)    
        self.thread.start()





print("ETS2 FM proxy server starting ... ")
proxy_server_so = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
proxy_server_so.bind(LISTEN_ADDRESS)
proxy_server_so.listen(socket.SOMAXCONN)
while True:
    cli_socket, cli_addr = proxy_server_so.accept()
    print('Client connected ... address = ', cli_socket.getpeername())
    proxy_session(cli_socket).run()