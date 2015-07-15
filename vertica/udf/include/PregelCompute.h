/*
 * PregelCompute.cpp
 *
 *  Created on: Sep 25, 2013
 *      Author: alekh
 */

#include "VerticaUDF.h"

#include <climits>

struct node{
	vint id;
	vfloat value;
	vfloat state;
	vector<vint> edges;
};
typedef struct node Node;

class PregelCompute : public VerticaUDF {
private:
	Node *nodes;
	int num_nodes;
	int currNodeIdx;

	vector<vfloat> nodeMessages;

public:

	virtual void setupData(ServerInterface &srvInterface){
		// initialize nodes
		nodes = (Node*)srvInterface.allocator->alloc(max_num_nodes*sizeof(Node));
		currNodeIdx = 0; num_nodes = INT_MAX; nodes[0].id = -1;
	}

	virtual void readData(PartitionReader &inputReader, ServerInterface &srvInterface){
		const vint dst0 = inputReader.getIntRef(0);
		int partitionId = getPartitionId(dst0);
		setUpInstance(partitionId,srvInterface);	// look at the partition ID and setup the instance
		debug(INFO,"processing partition id=%d on instance=%d",partitionId,instance_id);
#ifdef DISTRIBUTED
		startSockets(partitionId);	// look at the partition ID and start sockets
		//flush();
#endif

		int nodeCount = 0;
		do{
			const vint dst = inputReader.getIntRef(0);
			const vint dst_edge = inputReader.getIntRef(1);
			const vfloat dst_value = inputReader.getFloatRef(2);
			// check if it is a new vertex
			if(nodes[0].id!=-1 && nodes[currNodeIdx].id!=dst){
				doCompute();	// need to compute the previous vertex
				nodes[currNodeIdx].id = dst;
				nodeCount++;
			}
			// set vertex value
			if(dst_edge==vint_null){
				nodes[currNodeIdx].id = dst;
				nodes[currNodeIdx].value = dst_value;
			}
			// collect all 'edge' for any one 'from' node
			if(vfloatIsNull(dst_value))
				nodes[currNodeIdx].edges.push_back(dst_edge);
		}while(inputReader.next());

		num_nodes = nodeCount+1;
		doCompute();

		debug(INFO,"finished reading data (#nodes=%d) on instance %d", num_nodes,instance_id); flush();
	}



	virtual bool prepareCompute(int *currRMsgIdx){
		int i; vint minId=LONG_MAX;
		for(i=0;i<max_num_stores*num_data_nodes;i++){
			MessageStore ms = getMessageStore(instance_id,i); //[instance_id][i]
#ifdef SORT_BASED
			if(currRMsgIdx[i] < *(ms.num_rmessages) && ms.messages[*ms.roff + currRMsgIdx[i]].key < minId)
				minId = ms.messages[*ms.roff + currRMsgIdx[i]].key;
#elif defined(HASH_BASED)
			if(*ms.it!=ms.rmessages->end() && (*ms.it)->first < minId)
				minId = (*ms.it)->first;
#endif
		}

		if(minId==LONG_MAX)
			return false;

		for(;currNodeIdx<num_nodes && nodes[currNodeIdx].id<minId;currNodeIdx++);	// skip all nodes not having any message

		for(i=0;i<max_num_stores*num_data_nodes;i++){
			MessageStore ms = getMessageStore(instance_id,i); //[instance_id][i];
#ifdef SORT_BASED
			for(;currRMsgIdx[i] < *ms.num_rmessages && ms.messages[*ms.roff + currRMsgIdx[i]].key==minId; currRMsgIdx[i]++)
				nodeMessages.push_back(ms.messages[*ms.roff + currRMsgIdx[i]].value);
#elif defined(HASH_BASED)
			for(;*ms.it!=ms.rmessages->end() && (*ms.it)->first==minId; (*ms.it)++)
				nodeMessages.push_back((*ms.it)->second);
#endif
		}
		return true;
	}

	virtual void doCompute(){
		compute(getMessages());
		nodeMessages.clear();
		currNodeIdx++;
	}

	virtual void resetData(){
		currNodeIdx = 0;
	}

	virtual void writeData(PartitionWriter &outputWriter){
		// output the new vertex values (TODO: could be optimized to output ONLY the modified vertices)
		int i;
		for(i=0;i<num_nodes;i++){
			outputWriter.setInt(0, nodes[i].id);
			outputWriter.setFloat(1,nodes[i].value);
			outputWriter.setInt(2,(vint)(getPartitionId(nodes[i].id)));
			outputWriter.next();
		}
	}


//	set<int> getLocalPartitionIds(){
//		return partitionIds;
//	}



	/**
	 *
	 * Pregel API
	 *
	 */

	virtual void compute(vector<vfloat> messages) = 0;
	//virtual int getPartitionId(long id) = 0;

	inline int getPartitionId(long id){
		return (int)(id % (max_num_stores*num_data_nodes));	// random salt
	}

	inline vint getVertexId(){
		return nodes[currNodeIdx].id;
	}
	inline vfloat getVertexValue(){
		return nodes[currNodeIdx].value;
	}
	inline vector<vfloat> getMessages(){
		return nodeMessages;
	}
	inline vector<vint> getOutEdges(){
		return nodes[currNodeIdx].edges;
	}
	inline void modifyVertexValue(double value){
		nodes[currNodeIdx].value = value;
	}
	inline void sendMessage(int destination, double message){
		int partitionId = getPartitionId(destination);
#ifdef DISTRIBUTED
		// no different treatment for remote messages needed
//		if(partitionIds.find(partitionId)==partitionIds.end()){		// need to send to other nodes
//			//print("got a remote message"); //flush();
//			//sync_mutex.lock();
//			sockClient->sendMessage(instance_id, partitionId, destination, message);
//			//sync_mutex.unlock();
//			return;
//		}
#endif
		//print("sending messages");
		MessageStore ms = getMessageStore(partitionId,instance_id);
		//print("fetched the message store");
#ifdef SORT_BASED
		//if(partitionIds.find(partitionId)==partitionIds.end())		// need to send to other nodes
			//print("got a remote message");

		int writeMsgIdx = (*ms.woff) + (*ms.num_wmessages);
		ms.messages[writeMsgIdx].key = (vint)destination;
		ms.messages[writeMsgIdx].value = (vfloat)message;
		*ms.num_wmessages = *ms.num_wmessages + 1;

		//if(partitionIds.find(partitionId)==partitionIds.end())		// need to send to other nodes
		//	print("sent a remote message");
#elif defined(HASH_BASED)
		ms.wmessages->insert(pair<vint,vfloat>(destination,message));
#endif
	}
	inline void voteToHalt(){
		nodes[currNodeIdx].state = 0;
	}
};



class VertexFactory : public TransformFunctionFactory{

    // Tell Vertica that we take in a row with 7 attributes, and return a row with 7 attributes
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType){
        argTypes.addInt();		// dst
        argTypes.addInt();		// dst_edge
        argTypes.addFloat();	// dst_value

        returnType.addInt();	// id
        returnType.addFloat();	// value
        returnType.addInt();	// grp
    }

    // string length
    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes){

        // Error out if we're called with anything but 4 argument
        if (inputTypes.getColumnCount() != 3)
            vt_report_error(0, "Function only accepts 4 arguments, but %zu provided", inputTypes.getColumnCount());

        outputTypes.addInt("id");
        outputTypes.addFloat("value");
        outputTypes.addInt("state");
    }

    virtual void getPerInstanceResources(ServerInterface &srvInterface, VResources &res){
    	vint m1 = 100*1024*1024;
    	res.scratchMemory = m1*10;
		//res.scratchMemory = 100*1024*1024;
		//res.scratchMemory = 2147483648;
    	res.nFileHandles = max_num_stores * num_data_nodes;
	}
};

