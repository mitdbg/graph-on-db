/*
 * VertexUDF.h
 *
 *  Created on: Oct 6, 2014
 *      Author: alekh
 */

#ifndef VERTEXUDF_H_
#define VERTEXUDF_H_


#include "Constants.h"
#include "Messaging.h"
#include "Timer.h"
#include "Print.h"
#include "Sockets.h"
#include "Signals.h"

#include "Vertica.h"

#include <sys/file.h>
#include <mutex>
#include <thread>
//#include <unistd>

//#define DISTRIBUTED


using namespace Vertica;
using namespace std;




class VerticaUDF : public TransformFunction {

private:
	int i;
	int sync_id;

	MessageStore *instanceMessageStores;
	int *currRMsgIdx;

	Timer t;

protected:
	int instance_id;
	int iteration;

public:
	static vector<int> sync_ids;
	static mutex sync_mutex;
	static MessageStore **messageStores;
	static int instanceCounter;
	static set<int> partitionIds;
	//static MessageStore **remoteMessageStores;

#ifdef DISTRIBUTED
	static SockServer *sockServer;
	static SockClient *sockClient;
	static bool socketSetup, socketStart;
	int global_sync_id;
#endif

	virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
		//print("started UDF");
		handle_sigsegv();

		// read params from user
		readParams(srvInterface.getParamReader());

		// initialize
		sync_id = 0;
		iteration = 0;

		// initialize data
		setupData(srvInterface);

		// initialize this instance's message store
		//createMessageStores(srvInterface, instanceMessageStores, max_num_messages);

		// point to all other messages stores as well; set instance id
		sync_mutex.lock();
		int i,j;
		if(messageStores==0){
			messageStores = (MessageStore**)srvInterface.allocator->alloc(max_num_stores*num_data_nodes*sizeof(MessageStore*));
			for(i=0;i<max_num_stores*num_data_nodes;i++){
				//messageStores[i] = NULL;	// initialize all stores to be null
				messageStores[i] = (MessageStore*)srvInterface.allocator->alloc(max_num_stores*num_data_nodes*sizeof(MessageStore));
				for(j=0;j<max_num_stores*num_data_nodes;j++){
		#ifdef SORT_BASED
					messageStores[i][j].messages = (Message*)srvInterface.allocator->alloc(2*max_num_messages*sizeof(Message));
					messageStores[i][j].roff = (int *)srvInterface.allocator->alloc(sizeof(int));
					messageStores[i][j].woff = (int *)srvInterface.allocator->alloc(sizeof(int));
					messageStores[i][j].num_rmessages = (int *)srvInterface.allocator->alloc(sizeof(int));
					messageStores[i][j].num_wmessages = (int *)srvInterface.allocator->alloc(sizeof(int));
					*messageStores[i][j].roff = max_num_messages;
					*messageStores[i][j].woff = 0;
					*messageStores[i][j].num_rmessages = 0;
					*messageStores[i][j].num_wmessages = 0;
		#elif defined(HASH_BASED)
					messageStores[i][j].rmessages = (multimap<vint,vfloat> *)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>));
					new (messageStores[i][j].rmessages) multimap<vint,vfloat>();
					messageStores[i][j].wmessages = (multimap<vint,vfloat> *)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>));
					new (messageStores[i][j].wmessages) multimap<vint,vfloat>();

					messageStores[i][j].it = (multimap<vint,vfloat>::iterator*)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>::iterator));
		#endif
				}
			}
		}
		//instance_id = instanceCounter;
		//messageStores[instance_id] = instanceMessageStores;
		//instanceCounter++;
#ifdef DISTRIBUTED
		if(!socketSetup){
			sockServer = (SockServer*)srvInterface.allocator->alloc(sizeof(SockServer));
			sockClient = (SockClient*)srvInterface.allocator->alloc(sizeof(SockClient));
			socketSetup = true;
//			remoteMessageStores = (MessageStore**)srvInterface.allocator->alloc(max_num_stores*(num_data_nodes-1)*sizeof(MessageStore*));
//			for(i=0;i<max_num_stores*(num_data_nodes-1);i++)
//				createMessageStores(srvInterface, remoteMessageStores[i], max_num_messages);	// set to the size of remote message store
		}
		global_sync_id=0;
#endif
		sync_mutex.unlock();



		//debug(INFO,"finished UDF setup on instance %d",instance_id);
	}

	virtual void createMessageStores(ServerInterface &srvInterface, MessageStore *messageStores, size_t messagesPerStore){
		messageStores = (MessageStore*)srvInterface.allocator->alloc(max_num_stores*num_data_nodes*sizeof(MessageStore));
		for(i=0;i<max_num_stores*num_data_nodes;i++){
#ifdef SORT_BASED
			messageStores[i].messages = (Message*)srvInterface.allocator->alloc(2*messagesPerStore*sizeof(Message));
			messageStores[i].roff = (int *)srvInterface.allocator->alloc(sizeof(int));
			messageStores[i].woff = (int *)srvInterface.allocator->alloc(sizeof(int));
			messageStores[i].num_rmessages = (int *)srvInterface.allocator->alloc(sizeof(int));
			messageStores[i].num_wmessages = (int *)srvInterface.allocator->alloc(sizeof(int));
			*messageStores[i].roff = messagesPerStore;
			*messageStores[i].woff = 0;
			*messageStores[i].num_rmessages = 0;
			*messageStores[i].num_wmessages = 0;
#elif defined(HASH_BASED)
			messageStores[i].rmessages = (multimap<vint,vfloat> *)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>));
			new (messageStores[i].rmessages) multimap<vint,vfloat>();
			messageStores[i].wmessages = (multimap<vint,vfloat> *)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>));
			new (messageStores[i].wmessages) multimap<vint,vfloat>();

			messageStores[i].it = (multimap<vint,vfloat>::iterator*)srvInterface.allocator->alloc(sizeof(multimap<vint,vfloat>::iterator));
#endif
		}
	}

	virtual void setUpInstance(int partitionId, ServerInterface &srvInterface){
		sync_mutex.lock();
		instance_id = partitionId;
		//messageStores[instance_id] = instanceMessageStores;
		partitionIds.insert(partitionId);
		sync_mutex.unlock();

		synchronize();	// make sure that all instances are set up

#ifdef DISTRIBUTED
		// initialize the instances for remote stores
//		sync_mutex.lock();
//		int i=0;
//		for(i=0;i<(max_num_stores*num_data_nodes);i++){
//			if(messageStores[i]==NULL && partitionIds.find(i) == partitionIds.end()){
//				createMessageStores(srvInterface, messageStores[i], max_num_messages);	// set number of remote messages
//				debug(INFO,"setting remote message store %d", i);
//			}
//		}
//		sync_mutex.unlock();
//
//		for(int i=0;i<max_num_stores*num_data_nodes;i++){
//			if(messageStores[i]==0)
//				debug(INFO,"message store %d is still null!",i);
//		}
//		flush();
#endif
	}

	virtual void startSockets(int partitionId){
#ifdef DISTRIBUTED
//		sync_mutex.lock();
//
//		sync_mutex.unlock();
//		debug(INFO,"synchronize before starting the sockets on instance %d",instance_id);
//		synchronize();
//		print("ready to start the sockets");

		sync_mutex.lock();
		if(!socketStart){
			//debug(INFO,"going to setup socket server on instance %d",instance_id);
			sockServer->setup();
			sockServer->do_accept(num_data_nodes, partitionIds, messageStores);
			//flush();
			//sleep(5);

			//debug(INFO,"going to setup socket client on instance %d",instance_id);
			sockClient->setup(host_names, num_data_nodes, partitionIds, messageStores);
			//flush();

			socketStart= true;
			debug(INFO,"socket started using instance %d",instance_id);
			flush();
		}
		sync_mutex.unlock();

#endif
	}

	virtual void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
#ifdef DISTRIBUTED
		sync_mutex.lock();
		if(socketSetup){
			sockClient->closeAll();
			sockServer->shutdown();
			socketSetup = false;
		}
		sync_mutex.unlock();
#endif
	}

	virtual void readParams(ParamReader paramReader) = 0;
	virtual void setupData(ServerInterface &srvInterface) = 0;
	virtual void readData(PartitionReader &inputReader, ServerInterface &srvInterface) = 0;
	virtual bool prepareCompute(int *currRMsgIdx) = 0;
	virtual void doCompute() = 0;
	virtual void resetData() = 0;
	virtual void writeData(PartitionWriter &outputWriter) = 0;


	virtual void processPartition(ServerInterface &srvInterface,
	                                  PartitionReader &inputReader,
	                                  PartitionWriter &outputWriter){

		// make sure that all instances have allocated memory!
		//synchronize();
		//debug(INFO, "starting iteration = %d on instance = %d",iteration, instance_id);

		currRMsgIdx = (int*)srvInterface.allocator->alloc(max_num_stores*num_data_nodes*sizeof(int));
		for(i=0;i<max_num_stores*num_data_nodes;i++)
			currRMsgIdx[i] = 0;

		readData(inputReader,srvInterface);

		t.on();
		int msgCount = prepareMessages(srvInterface);
		//flush();

		// loop as long as there are any new messages for this partition
		while(msgCount > 0){
			iteration++;
			debug(INFO, "starting iteration = %d, messages = %ld",iteration,msgCount);
			//flush();

			// compute
			while(prepareCompute(currRMsgIdx))		// probably we do not need the first condition!
				doCompute();
			debug(INFO, "iteration = %d, finished computation", iteration);
			flush();

			// prepare next messages
			msgCount = prepareMessages(srvInterface);
			//flush();
		}
		t.off();

		writeData(outputWriter);

		debug(INFO, "instance=%d, time=%f\n", instance_id, t.time());
		flush();
	}

	virtual int prepareMessages(ServerInterface &srvInterface){
		print("going to prepare messages");
		int msgCount = synchronize_global();
		//print("done global sync"); //flush();

		sync_mutex.lock();
		swapMessageStores(messageStores, instance_id);
#ifdef DISTRIBUTED
		sockClient->resetSender();
#endif
		sync_mutex.unlock();
		//debug(INFO,"global message count = %d",msgCount); //flush();
		//print("message stores swapped");

		synchronize_global();

		sortMessages(messageStores, instance_id);		// sort the messages for this instance
		//print("message stores sorted");

		// reset
		resetData();
		for(i=0;i<max_num_stores*num_data_nodes;i++)
			currRMsgIdx[i] = 0;

		//print("finished message prepare");

		return msgCount;
	}

	virtual void synchronize(){
		sync_mutex.lock();
		sync_ids.push_back(sync_id);
		sync_mutex.unlock();
		sync_id++;

		int expectedLines = sync_id*max_num_stores;
		while(sync_ids.size() < (size_t)expectedLines){
			usleep(1*100);	// 1 millisecond wait
			//debug(INFO,"sync count: %d",sync_ids.size());
		}
	}

	virtual int synchronize_global(){
		//print("sync global: starting local sync");
		synchronize();	// sync locally first
		//print("sync global: local sync done");
		int msgCount = messageCountBeforeSwap(messageStores, partitionIds);

#ifdef DISTRIBUTED
		print("going to send remote sync");
		//sync_mutex.lock();
		sockServer->addSync(msgCount);
		print("addSync done");
		sockClient->sendSync(msgCount);
		//sync_mutex.unlock();

		print("sendSync done");

		flush();

		global_sync_id++;

		int expectedLines = global_sync_id*max_num_stores*num_data_nodes;
		while(sockServer->global_sync_count < expectedLines){
			usleep(1*100);	// 1 millisecond wait
			//debug(INFO,"global sync count: %d",sockServer->global_sync_count);
			//flush();
		}
		print("global sync done"); flush();
		return sockServer->getGlobalMsgCount(global_sync_id);
#else
		return msgCount;
#endif
	}

	inline MessageStore getMessageStore(int partition_id, int instance_id){
		return messageStores[partition_id][instance_id];
	}








};
vector<int> VerticaUDF::sync_ids;
mutex VerticaUDF::sync_mutex;
MessageStore **VerticaUDF::messageStores = 0;
int VerticaUDF::instanceCounter = 0;
set<int> VerticaUDF::partitionIds;
//MessageStore **VerticaUDF::remoteMessageStores = 0;

#ifdef DISTRIBUTED
SockServer *VerticaUDF::sockServer = 0;
SockClient *VerticaUDF::sockClient = 0;
bool VerticaUDF::socketSetup = false;
bool VerticaUDF::socketStart = false;
#endif

#endif /* VERTEXUDF_H_ */
