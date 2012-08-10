import zo_api

worker = zo_api.ZOSerivce("tcp://localhost:5555", "echo", True)

reply = None

def echo(s):
    return s


while True:
    request = worker.recv(reply)
    if request is None:
        break
    reply = echo(request) # do some work