import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	hostport = args.hostport
	import random
	random.seed(123)
	print(random.randint(1,100))
	print(random.randint(1, 100))

	try:
		import time
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8080")
		client.surfstore.updatefile("Test.txt", 0, [0])
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8081")
		client.surfstore.crash()
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8082")
		client.surfstore.crash()
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8080")
		client.surfstore.updatefile("Test.txt", 1, [0])

		client = xmlrpc.client.ServerProxy('http://' + "localhost:8083")
		client.surfstore.crash()
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8080")
		client.surfstore.crash()
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8084")
		client.surfstore.crash()

		client = xmlrpc.client.ServerProxy('http://' + "localhost:8081")
		client.surfstore.restore()
		#time.sleep(2)
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8082")
		client.surfstore.restore()
		#time.sleep(2)
		client = xmlrpc.client.ServerProxy('http://' + "localhost:8084")
		client.surfstore.restore()



	except Exception as e:
		print("Client: " + str(e))
