import zmq


class Broker:
    ''' calss defining a broker carcterised by :
    - idFrontEnd : port number for the back-end socket
    - idBackEnd :  port number for the front-end socket
    - frontEnd : front-end socket
    - backEnd : back-end socket
    - poller : A stateful poll object
    - MessageList : a list containing the messages recieved from the clients other than the Ready message '''
    def __init__(self, idFrontEnd, idBackEnd):
        self.idFrontEnd = idFrontEnd
        self.idBackEnd = idBackEnd
        # Prepare context and sockets
        context = zmq.Context()
        self.frontEnd = context.socket(zmq.ROUTER)
        self.backEnd = context.socket(zmq.ROUTER)
        self.frontEnd.bind("tcp://*:" + self.idFrontEnd)
        self.backEnd.bind("tcp://*:" + self.idBackEnd)
        # Initialize poll set
        self.poller = zmq.Poller()
        self.poller.register(self.frontEnd, zmq.POLLIN)
        self.poller.register(self.backEnd, zmq.POLLIN)
        self.messageList = []

    def start(self):

        while True:
            socks = dict(self.poller.poll())
            if socks.get(self.frontEnd) == zmq.POLLIN:
                message = self.frontEnd.recv_multipart()
                print(message[1])
                if message[1] == b"READY":
                    for k in range(len(self.messageList)):
                        if self.messageList[k][0] == message[0]:
                            self.frontEnd.send_multipart(self.messageList[k])
                            self.messageList[k][0] = b"sent"
                    self.frontEnd.send_multipart([message[0], b"END"])

                else:
                    self.messageList.append([message[1], message[2]])

            if socks.get(self.backEnd) == zmq.POLLIN:
                message = self.backEnd.recv_multipart()
                print(message)

                if message[1] == b"READY":
                    for k in range(len(self.messageList)):
                        if self.messageList[k][0] == message[0]:
                            self.backEnd.send_multipart(self.messageList[k])
                    self.backEnd.send_multipart([message[0], b"END"])
                else:
                    self.messageList.append([message[1], message[2]])

broker1 = Broker("5559", "5560")
broker1.start()


