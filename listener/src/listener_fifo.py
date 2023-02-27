#!/usr/bin/env python3

import sys
import os
import pyarrow as pa
import pyarrow.ipc

fifo_path=sys.argv[1]
print("Fifo path ",fifo_path)

os.mkfifo(fifo_path, mode=0o666)

fd=open(fifo_path,"r")
while True:
    line = fd.readline()
    if line !="":
        print(line)

#with open(fifo_path) as fifo:
#    while True:
#        select.select([fifo],[],[fifo])
#        data = fifo.read()
#        print(data)

#with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
#    sock.bind((listen, port))
#    sock.listen()
#    print(f"Listening on {listen} on port {port}")
#    conn, _ = sock.accept()
#    with conn:
#        conn_file = conn.makefile(mode="b")
#        reader = pyarrow.ipc.RecordBatchStreamReader(conn_file)
#        table = reader.read_all()
#        #table = reader.read_next_batch()
#        print(table)
#        print(table.to_pandas())
