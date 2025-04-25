import socket
import time
import os
import multiprocessing

SOCKET_PATH = "/tmp/unix_socket_benchmark"
PACKET_SIZE = 1 * 3 * 16 * 112 * 112 * 4  # Compute the packet size
NUM_ITERATIONS = 5  # Number of messages to send

def server():
    if os.path.exists(SOCKET_PATH):
        os.remove(SOCKET_PATH)
    
    server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server_socket.bind(SOCKET_PATH)
    server_socket.listen(1)
    
    conn, _ = server_socket.accept()
    while True:
        buf = []
        while len(buf) < PACKET_SIZE:
            data = conn.recv(PACKET_SIZE - len(buf))
            buf.extend(data)
        print("recd with len ", len(buf))
        if not buf:
            break
        conn.sendall(bytes(buf))  # Echo back
    
    conn.close()
    server_socket.close()
    os.remove(SOCKET_PATH)

def client():
    time.sleep(1)  # Give server some time to start
    client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_socket.connect(SOCKET_PATH)
    
    data = b"0" * PACKET_SIZE  # Create a dummy payload
    
    # Benchmark latency
    start_time = time.time()
    for i in range(NUM_ITERATIONS):
        client_socket.sendall(data)
        buf = []
        while len(buf) < PACKET_SIZE:
            tmp = client_socket.recv(PACKET_SIZE - len(buf))
            buf.extend(tmp)
        print(f"Done {i+1}")
    end_time = time.time()
    
    latency = (end_time - start_time) / NUM_ITERATIONS
    throughput = (PACKET_SIZE * NUM_ITERATIONS) / (end_time - start_time) / (1024 * 1024)  # MB/s
    
    print(f"Average Latency: {latency * 1e6:.2f} µs")
    print(f"Throughput: {throughput:.2f} MB/s")
    
    client_socket.close()

def main():
    server_process = multiprocessing.Process(target=server)
    server_process.start()
    
    client()
    
    server_process.terminate()
    server_process.join()

if __name__ == "__main__":
    main()

