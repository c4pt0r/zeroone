import zo_api
import time


worker = zo_api.ZOClient("tcp://localhost:5555", True)

while True:
	time.sleep(1)
	worker.send('echo', 'hello')
	pass