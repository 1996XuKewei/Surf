import argparse
import xmlrpc.client

if __name__ == "__main__":

	parser = argparse.ArgumentParser(description="SurfStore client")
	parser.add_argument('hostport', help='host:port of the server')
	args = parser.parse_args()

	hostport = args.hostport

	try:
		client = xmlrpc.client.ServerProxy('http://' + hostport)
		# Test ping
		print(client.surfstore.isLeader())
		print(client.surfstore.isCrashed())
		client.surfstore.restore()
		#client.surfstore.updatefile("Test.txt", 3, [1, 2, 3])
		#print(client.surfstore.tester_getversion("Test.txt"))
		print(client.surfstore.tester_getversion("Test.txt"))

	except Exception as e:
		print("Client: " + str(e))
	print(123)
