import sys
import time

print("Hello candle!")
sys.stdout.flush()

import torch

print("Candle got torch")
sys.stdout.flush()

import socket
import os
import struct

print("Loading model")
sys.stdout.flush()

model = torch.jit.load("./models/model.pt")

print("Loaded model")
sys.stdout.flush()

sockpath = sys.argv[1]

# procport = int(os.getenv("CANDLE_PORT", "5678"))

# servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# servsock.bind(("", procport))

# servsock.listen(5)
# print("Candle listening on port", procport)
# sys.stdout.flush()

sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
sock.connect(sockpath)

print(f"Connected to {sockpath}", flush=True)

with torch.no_grad():
    while True:
        # sock, addr = servsock.accept()
        # print("Accepted from addr")
        # sys.stdout.flush()
        stream = sock.makefile("rwb")
        print("Started")
        sys.stdout.flush()
        while True:
            print("CANDLE Reading line", flush=True)
            ln = stream.readline()
            print("candle Size line: ", ln, flush=True)
            sizes = [int(x) for x in ln.decode().strip().split(" ")]
            print("candle sizes: ", sizes, flush=True)
            count = 1
            for s in sizes:
                count *= s
            print("CANDLE Reading values", flush=True)
            buf = []
            rd_st = time.time()
            while len(buf) < 4*count:
                tmp = stream.read(4*count - len(buf))
                buf.extend(tmp)
            rd_end = time.time()
            print("candle read time (ms): ", (rd_end - rd_st)*1e3, flush=True)
            inpbuf = [x for x in struct.iter_unpack("f", bytearray(buf))]
            inp = torch.tensor(inpbuf)
            print("CANDLE read values", flush=True)
            inp = inp.reshape(sizes)
            print("CANDLE running model", flush=True)
            op = model(inp)
            print("CANDLE result: ", op, flush=True)
            val1 = op[0][0].item()
            val2 = op[0][1].item()
            stream.write(f"{val1} {val2}\n".encode())
            stream.flush()
            print("CANDLE wrote results")
            print("CANDLE op: ", op, flush=True)
        print("Stopped")
