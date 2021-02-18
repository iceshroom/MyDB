.PHONY : main
main :
	g++ DBServer.cpp -o server -lpthread -g
	g++ DBClient.cpp -o client -lpthread -g
	mv server ../bin/server
	mv client ../bin/client
	cp Server.conf ../bin/server