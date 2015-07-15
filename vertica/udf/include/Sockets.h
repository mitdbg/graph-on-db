/*
 * Sockets.h
 *
 *  Created on: Nov 6, 2014
 *      Author: alekh
 */

#ifndef SOCKETS_H_
#define SOCKETS_H_

#include "Print.h"
#include <mutex>

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <thread>
#include <pthread.h>
#include <set>
#include <map>

#include <errno.h>
#include <stdlib.h>

#ifndef NULL
#define NULL   ((void *) 0)
#endif


#define PORT "3490"  // the port users will be connecting to
#define BACKLOG 10	 // how many pending connections queue will hold

int EOM = -1;		// end of message
int SYNC = -2;		// sync
int MSG_COUNT = -3;	// message count


//using namespace std;


int get_node_id(const char *node_name){
	int i,t_pos=-1;
	char t[10];
	for(i=0; node_name[i]!='\0'; i++){
		if(node_name[i]=='.')
			break;
		if(isdigit(node_name[i]))
			t[++t_pos] = node_name[i];
		else
			t_pos=-1;
	}
	t[++t_pos] = '\0';
	return atoi(t);
}

int get_node_id(){
	int node_id=0;
	struct addrinfo hints, *info, *p;

	char hostname[1024];
	hostname[1023] = '\0';
	gethostname(hostname, 1023);

	memset(&hints, 0, sizeof hints);
	hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_CANONNAME;

	getaddrinfo(hostname, "http", &hints, &info);
	for(p = info; p != NULL; p = p->ai_next) {
		node_id = get_node_id(p->ai_canonname);
//		int i,t_pos=-1;
//		char t[10];
//		for(i=0; p->ai_canonname[i]!='\0'; i++){
//			if(p->ai_canonname[i]=='.')
//				break;
//			if(isdigit(p->ai_canonname[i]))
//				t[++t_pos] = p->ai_canonname[i];
//			else
//				t_pos=-1;
//		}
//		t[++t_pos] = '\0';
//		node_id = atoi(t);
	}
	freeaddrinfo(info);

	return node_id;
}


// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa){
	if (sa->sa_family == AF_INET)
		return &(((struct sockaddr_in*)sa)->sin_addr);
	return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

void sigchld_handler(int s){
	while(waitpid(-1, (int*)NULL, WNOHANG) > 0);
}

/**
 * Socket server to receive messages from other nodes.
 * Accepts connections from all nodes except itself.
 * There is a single server instance across all partitions.
 *
 */
class SockServer{
private:
	MessageStore **messageStores;
	set<int> localPartitionIds;

	int sockfd;  // listen on sock_fd, new connection on new_fd
	int *fd;


	char s[INET6_ADDRSTRLEN];
	int rv;

	std::thread *serverThreads;
	std::thread acceptThread;
	//pthread_t *serverThreads;
	//pthread_t acceptThread;

public:

	mutex sync_mutex;
	int global_sync_count;
	vector<int> perNodeMessageCounts;

	void setup(){
		struct addrinfo hints, *servinfo, *p;
		struct sigaction sa;
		int yes=1;
		global_sync_count = 0;

		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_PASSIVE; // use my IP

//		int port = atoi(PORT)+get_node_id();
//		char buff[10];
//		sprintf(buff, "%d", port);
//		debug(INFO,"starting server on port %d",port);

		if ((rv = getaddrinfo((char*)NULL, PORT, &hints, &servinfo)) != 0) {
			fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
			return;
		}

		// loop through all the results and bind to the first we can
		for(p = servinfo; p != NULL; p = p->ai_next) {
			if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
				perror("server: socket");
				continue;
			}

			if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
					sizeof(int)) == -1) {
				perror("setsockopt");
				exit(1);
			}

			if (::bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
				close(sockfd);
				perror("server: bind");
				continue;
			}

			break;
		}

		if (p == NULL)  {
			fprintf(stderr, "server: failed to bind\n");
			return;
		}

		freeaddrinfo(servinfo); // all done with this structure

		if (listen(sockfd, BACKLOG) == -1) {
			perror("listen");
			exit(1);
		}

		sa.sa_handler = sigchld_handler; // reap all dead processes
		sigemptyset(&sa.sa_mask);
		sa.sa_flags = SA_RESTART;
		if (sigaction(SIGCHLD, &sa, 0) == -1) {
			perror("sigaction");
			exit(1);
		}

		//print("finished setup of socket server");
	}

	void do_accept(int numHosts, set<int> localPartitionIds, MessageStore **messageStores){
		this->messageStores = messageStores;
		this->localPartitionIds = localPartitionIds;
		serverThreads = new thread[numHosts];
		//serverThreads = new pthread_t[numHosts];
		fd = new int[numHosts];

		acceptThread = std::thread(&SockServer::accept_thread, this, numHosts);
		//pthread_create(&acceptThread, NULL, SockServer::accept_thread, &numHosts);
	}

//	void *accept_thread(void *arg){
//		int *numHosts = (int*)arg;
//		int fd_count;
//
//		struct sockaddr_storage their_addr; // connector's address information
//		socklen_t sin_size;
//
//		//print("going to accept connections ...");
//		for(fd_count=0; fd_count < (*numHosts-1)*max_num_stores; fd_count++){	// main accept() loop; do not accept connection from itself.
//			sin_size = sizeof their_addr;
//			int new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
//			if (new_fd == -1) {
//				perror("accept");
//				continue;
//			}
//
//			inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr), s, sizeof s);
//			debug(INFO,"server: got connection from %s", s); flush();
//
//			fd[fd_count] = new_fd;
//			//serverThreads[fd_count] = std::thread(&SockServer::recvMessages, this, new_fd);
//			pthread_create(&serverThreads[fd_count], NULL, SockServer::recvMessages, &new_fd);
//		}
//		close(sockfd);
//
//		for(fd_count=0; fd_count < (num_data_nodes-1)*max_num_stores; fd_count++)
//			//serverThreads[fd_count].join();
//			pthread_join(serverThreads[fd_count], NULL);
//
//		return NULL;
//	}

	void accept_thread(int numHosts){
		int fd_count;

		struct sockaddr_storage their_addr; // connector's address information
		socklen_t sin_size;

		//print("going to accept connections ...");
		for(fd_count=0; fd_count < (numHosts-1)*max_num_stores; fd_count++){	// main accept() loop; do not accept connection from itself.
			sin_size = sizeof their_addr;
			int new_fd = accept(sockfd, (struct sockaddr *)&their_addr, &sin_size);
			if (new_fd == -1) {
				perror("accept");
				continue;
			}

			inet_ntop(their_addr.ss_family, get_in_addr((struct sockaddr *)&their_addr), s, sizeof s);
			debug(INFO,"server: got connection from %s", s); //flush();

			fd[fd_count] = new_fd;
			serverThreads[fd_count] = std::thread(&SockServer::recvMessages, this, new_fd);
		}
		close(sockfd);

		for(fd_count=0; fd_count < (num_data_nodes-1)*max_num_stores; fd_count++)
			serverThreads[fd_count].join();
	}

	void shutdown(){
		acceptThread.join();
		//pthread_join(acceptThread, NULL);
	}

	void sendPartitionIds(int sock, set<int> localPartitionIds){
		// send current node's partition ids to each accepted connection
		//print("sending partition ids");

		set<int>::iterator it;
		int c=0;
		for(it=localPartitionIds.begin(); it!=localPartitionIds.end(); ++it){
			int partitionId = *it;
			send(sock, &partitionId, sizeof(partitionId), 0);
			c++;
		}
		send(sock, &EOM, sizeof(EOM), 0);	// send -1 to indicate the end of partition ids

		//debug(INFO,"finished sending %d partition ids",c);
	}

//	void *recvMessages(void *arg){
//		int *sock = (int *)arg;
//
//		sendPartitionIds(*sock, localPartitionIds);
//
//		int destination, sourcePartitionId; double message;
//		while(true){
//			int numbytes; errno=0;
//			if((numbytes=recv(*sock, &destination, sizeof(destination), 0)) < 0){	;	// read destination vertex
//				perror("server: recv");
//				debug(INFO,"received %d bytes, error: %s",numbytes,strerror(errno));
//			}
//			else{
//				if(numbytes==0 || destination==EOM)
//					break;
//				else if(destination==SYNC){
//					int remoteMsgCount;
//					recv(*sock, &remoteMsgCount, sizeof(remoteMsgCount), 0);
//					addSync(remoteMsgCount);
//				}
//				else{
//					recv(*sock, &sourcePartitionId, sizeof(sourcePartitionId), 0);	// read source partition ID
//					recv(*sock, &message, sizeof(message), 0);						// read the message
//					sendMessage(sourcePartitionId, destination, message);
//				}
//			}
//		}
//		close(*sock);
//
//		return NULL;
//	}

	void recvMessages(int sock){
		sendPartitionIds(sock, localPartitionIds);

		int destination, sourcePartitionId; double message;
		while(true){
			int numbytes; errno=0;
			if((numbytes=recv(sock, &destination, sizeof(destination), 0)) < 0){	;	// read destination vertex
				perror("server: recv");
				debug(INFO,"received %d bytes, error: %s",numbytes,strerror(errno));
			}
			else{
				if(numbytes==0 || destination==EOM)
					break;
				else if(destination==SYNC){
					int remoteMsgCount;
					recv(sock, &remoteMsgCount, sizeof(remoteMsgCount), 0);
					print("adding one remote sync id");
					addSync(remoteMsgCount);
				}
				else{
					recv(sock, &sourcePartitionId, sizeof(sourcePartitionId), 0);	// read source partition ID
					recv(sock, &message, sizeof(message), 0);						// read the message
					sendMessage(sourcePartitionId, destination, message);
				}
			}
		}
		close(sock);
	}

	void addSync(int remoteMsgCount){
		sync_mutex.lock();
		global_sync_count++;
		//print("adding one sync id");
		perNodeMessageCounts.push_back(remoteMsgCount);
		sync_mutex.unlock();
	}

	int getGlobalMsgCount(int sync_id){
		int c=0,i;
		for(i=(sync_id-1)*max_num_stores*num_data_nodes; i<(sync_id*max_num_stores*num_data_nodes); i++){
			c += perNodeMessageCounts[i];
		}
		//debug(INFO,"ooo global message count value = %d",(c/max_num_stores));
		//debug(INFO,"entries in perNodeMessageCounts: %d",perNodeMessageCounts.size());
		return c/max_num_stores;
	}




	/**
	 * These two functions are duplicated from PregelCompute
	 */

	inline int getPartitionId(long id){
		return (int)(id % (max_num_stores*num_data_nodes));	// random salt
	}

	void sendMessage(int sourcePartitionId, int destination, double message){
		//print("received remote message"); //flush();
		int partitionId = getPartitionId(destination);
		//debug(INFO, "remote message: destPartitionId=%d, sourcePartitionId=%d",partitionId,sourcePartitionId);
		MessageStore ms = messageStores[partitionId][sourcePartitionId];
#ifdef SORT_BASED
		int writeMsgIdx = (*ms.woff) + (*ms.num_wmessages);
		ms.messages[writeMsgIdx].key = (vint)destination;
		ms.messages[writeMsgIdx].value = (vfloat)message;
		*ms.num_wmessages = *ms.num_wmessages + 1;
#elif defined(HASH_BASED)
		ms.wmessages->insert(pair<vint,vfloat>(destination,message));
#endif
		//print("remote message updated"); //flush();
	}

//	long messageCount(MessageStore **messageStores){
//		long count=0; int i;
//		for(i=0;i<max_num_stores*num_data_nodes;i++){
//			if(messageStores[i]==NULL)
//				continue;
//			int j;
//			for(j=0;j<max_num_stores*num_data_nodes;j++)
//	#ifdef SORT_BASED
//				count += *messageStores[i][j].num_rmessages;
//	#elif defined(HASH_BASED)
//				count += messageStores[i][j].rmessages->size();
//	#endif
//		}
//		return count;
//	}

};



/**
 * Socket client to send messages to other nodes.
 * The client keeps connections to all other nodes active.
 * All partitions use the same client to send messages.
 *
 */
class SockClient {
private:
	//int MAXDATASIZE=100;
	//int numbytes;
	//char buf[MAXDATASIZE];

	int *fd; int num_fd;
	std::map<int,int> *partitionIdMap;

	MessageStore **messageStores;
	set<int> localPartitionIds;

	std::thread sender_thread;
	bool runSender;

	bool doSync,doReset;
	int localMsgCount;

	mutex sync_mutex;

public:

	bool is_localhost(const char *name){
		struct addrinfo hints, *info, *p;
		char hostname[1024];
		hostname[1023] = '\0';
		gethostname(hostname, 1023);

		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
		hints.ai_socktype = SOCK_STREAM;
		hints.ai_flags = AI_CANONNAME;

		getaddrinfo(hostname, "http", &hints, &info);
		bool flag = true;
		for(p = info; p != NULL; p = p->ai_next) {
			int i;
			for(i=0; p->ai_canonname[i]!='\0' && name[i]!='\0'; i++){
				if(p->ai_canonname[i] != name[i])
					flag = false;
			}
		}
		freeaddrinfo(info);

		return flag;
	}


	void setup(const char *hosts[], int numHosts, set<int> localPartitionIds, MessageStore **messageStores){
		struct addrinfo hints, *servinfo, *p;
		char s[INET6_ADDRSTRLEN];
		int rv; int sockfd;
		int i;

		this->messageStores = messageStores;
		this->localPartitionIds = localPartitionIds;
		this->runSender = true;
		this->doSync = false;
		this->doReset = false;
		//this->syncInProgress = false;

		memset(&hints, 0, sizeof hints);
		hints.ai_family = AF_UNSPEC;
		hints.ai_socktype = SOCK_STREAM;

		fd = new int[numHosts];
		for(i=0;i<numHosts;i++)
			fd[i] = -1;
		num_fd = numHosts;

		//print("trying to connect to other nodes ...");


		for(i=0;i<numHosts;i++){
			//print("checking whether host is localhost or not ..");
			if(is_localhost(hosts[i]))
				continue;

			//print("host is not a localhost");

//			int port = atoi(PORT)+get_node_id(hosts[i]);
//			char buff[10];
//			sprintf(buff, "%d", port);
//			debug(INFO,"connecting on port %d",port);

			if ((rv = getaddrinfo(hosts[i], PORT, &hints, &servinfo)) != 0) {
				debug(ERROR, "getaddrinfo: %s", gai_strerror(rv)); //flush();
				return;
			}

			do{
				sleep(1);
				// loop through all the results and connect to the first we can
				for(p = servinfo; p != NULL; p = p->ai_next) {
					//errno = 0;
					if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
						perror("client: socket");
						//debug(INFO,"socket error: %s",strerror(errno));
						continue;
					}
					int c_flag; errno = 0;
					print("client: going to connect"); //flush();
					if ((c_flag=connect(sockfd, p->ai_addr, p->ai_addrlen)) == -1) {
						close(sockfd);
						perror("client: connect");
						debug(INFO,"connect error: %s, sockfd=%d, sockLen_t=%d",strerror(errno),sockfd,p->ai_addrlen); //flush();
						continue;
					}
					break;
				}
			} while(p==NULL);

//			if (p == NULL) {
//				print("client: failed to connect\n"); flush();
//				return;
//			}

			inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr), s, sizeof s);
			debug(INFO,"client: connecting to %s", s);

			fd[i] = sockfd;

			// receive partition ids from the server
			int partitionId;
			partitionIdMap = new map<int,int>();
			while(true){
				int numbytes;
				if ((numbytes = recv(sockfd, &partitionId, sizeof(partitionId), 0)) == -1){
					perror("recv");
				}

				if(partitionId==-1)
					break;
				//debug(INFO,"received partition id %d",partitionId);
				//partitionIdMap.insert(std::pair<int,int>(partitionId,i));
				(*partitionIdMap)[partitionId] = i;
			}
			//print("client: finished receiving partition IDs"); //flush();
		}

		freeaddrinfo(servinfo); // all done with this structure
		sender_thread = std::thread(&SockClient::senderThread, this);
	}

	int currRMsgIdx[max_num_stores*num_data_nodes][max_num_stores*num_data_nodes];
	multimap<vint,vfloat>::iterator it[max_num_stores*num_data_nodes][max_num_stores*num_data_nodes];


	void senderThread(){
		resetSender();
		while(runSender)
			flushMessages();
	}

//	void senderThread(){
//		int i;
//		resetSender();
//		while(runSender){
//			if(doSync){
//				//syncInProgress = true;
//				print("sync started");
//				flushMessages();
//				print("messages flushed"); flush();
//
//				for(i=0;i<num_fd;i++){
//					debug(INFO,"sending the sync to fd %d",fd[i]);flush();
//					if(fd[i]<0){
//
//						continue;
//					}
//
//					send(fd[i], &SYNC, sizeof(SYNC), 0);	// send SYNC
//					print("sent the sync flag");
//
//					send(fd[i], &localMsgCount, sizeof(localMsgCount), 0);	// send local message count
//					print("sent the local message count"); flush();
//				}
//				print("switching the doSync flag"); flush();
//
//				doSync = false;
//				//syncInProgress = false;
//				print("sync ended"); flush();flush();
//			}
//			else if(doReset){
//				resetSender();
//				doReset = false;
//				print("doReset"); flush();
//			}
//			else{
//				flushMessages();
////				j++;
////				if(j%1000==0){
////					print("flushMessages");
////					flush();
////				}
//			}
//		}
//	}

//	void setResetFlag(){
//		doReset = true;
//	}

	void resetSender(){
		sync_mutex.lock();

		int i,j;
#ifdef SORT_BASED
		//currRMsgIdx = new int[max_num_stores*num_data_nodes][max_num_stores*num_data_nodes];
		for(i=0;i<max_num_stores*num_data_nodes;i++)
			for(j=0;j<max_num_stores*num_data_nodes;j++)
				if(localPartitionIds.find(i) == localPartitionIds.end())
					currRMsgIdx[i][j] = 0;
#elif defined(HASH_BASED)
		//it = new multimap<vint,vfloat>::iterator[max_num_stores*num_data_nodes][max_num_stores*num_data_nodes];
		for(i=0;i<max_num_stores*num_data_nodes;i++)
			for(j=0;j<max_num_stores*num_data_nodes;j++)
				if(localPartitionIds.find(i) == localPartitionIds.end())
					it[i][j] = messageStores[i][j].wmessages->begin();
#endif

		sync_mutex.unlock();
	}

	void flushMessages(){
		sync_mutex.lock();

		int i,j;
		// iterate over all stores
		for(i=0;i<max_num_stores*num_data_nodes;i++){
			//   if the store is not of a local partition then scan for messages
			if(messageStores[i]==0 || localPartitionIds.find(i) != localPartitionIds.end())
				continue;

			for(j=0;j<max_num_stores*num_data_nodes;j++){
				MessageStore ms = messageStores[i][j];
			#ifdef SORT_BASED
				for(;currRMsgIdx[i][j] < *ms.num_wmessages; currRMsgIdx[i][j]++){
					sendMessage(j,
								i,
								ms.messages[*ms.woff + currRMsgIdx[i][j]].key,
								ms.messages[*ms.woff + currRMsgIdx[i][j]].value
							);
					//print("flushing remote messages");
				}
			#elif defined(HASH_BASED)
				//TODO: need to double check whether the iterators work in the presence of updates!
				for(;*ms.it!=ms.wmessages->end() && (*ms.it)->first==minId; (*ms.it)++)
					sendMessage(j,
								i,
								nodeMessages.push_back((*ms.it)->first,
								nodeMessages.push_back((*ms.it)->second
							);
			#endif
			}
		}

		sync_mutex.unlock();
		//print("flush done");
	}


	inline void sendMessage(int sourcePartitionId, int destPartitionId, int destination, double message){
		send(fd[(*partitionIdMap)[destPartitionId]], &destination, sizeof(destination), 0);
		send(fd[(*partitionIdMap)[destPartitionId]], &sourcePartitionId, sizeof(sourcePartitionId), 0);
		send(fd[(*partitionIdMap)[destPartitionId]], &message, sizeof(message), 0);
	}

	void sendSync(int localMsgCount){

//		print("waiting for other sync to finish"); flush();
//
//		while(doSync);
//
//		print("done waiting .. going to set the sync flag"); flush();
//
//		//this->syncInProgress = true;
//		this->localMsgCount = localMsgCount;
//		this->doSync = true;

		//print("in sendSync");
		flushMessages();
		sync_mutex.lock();

		int i;
		//print("sending sync ..");
		for(i=0;i<num_fd;i++){
			if(fd[i]<0)
				continue;
			send(fd[i], &SYNC, sizeof(SYNC), 0);	// send SYNC
			send(fd[i], &localMsgCount, sizeof(localMsgCount), 0);	// send local message count
		}

		sync_mutex.unlock();
	}


//	int getMessageCount(){
//		int i,localMsgCount,msgCount=0;
//		//print("sending sync ..");
//		for(i=0;i<num_fd;i++){
//			send(fd[i], &MSG_COUNT, sizeof(MSG_COUNT), 0);	// send MSG_COUNT request
//			recv(fd[i], &localMsgCount, sizeof(localMsgCount), 0);
//			msgCount += localMsgCount;
//		}
//		return msgCount;
//	}

	void closeAll(){
		// shutdown and sync the other thread
		runSender = false;
		sender_thread.join();

		int i;
		for(i=0;i<num_fd;i++){
			if(fd[i]<0)
				continue;
			send(fd[i], &EOM, sizeof(EOM), 0);		// send -1 to indicate the end of messages
			close(fd[i]);
		}
	}
};









#endif /* SOCKETS_H_ */
