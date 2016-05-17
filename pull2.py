import zmq
import time

context = zmq.Context(1)
worker = context.socket(zmq.DEALER)
worker.setsockopt(zmq.IDENTITY, b'2')
worker.connect("tcp://localhost:5559")
worker.send(b"READY")
while True:
    msg = worker.recv_multipart()
    print("Let's talk about %s." % msg)
    if msg[0] == b"END":
        time.sleep(0.5)
        break


