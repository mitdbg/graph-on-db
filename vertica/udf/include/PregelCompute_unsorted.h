/*
 * PregelCompute.cpp
 *
 *  Created on: Sep 25, 2013
 *      Author: alekh
 */

#include "VerticaUDF.h"

#include <climits>

struct node{
	vfloat value;
	vfloat state;
	vector<vint> edges;
};
typedef struct node Node;

class PregelCompute : public VerticaUDF {
private:
	map<vint,Node> *nodes;
	int num_nodes;
	map<vint,Node>::iterator currNode;

	vector<vfloat> nodeMessages;

public:

	virtual void setupData(ServerInterface &srvInterface){
		// initialize nodes
		nodes = (map<vint,Node>*)srvInterface.allocator->alloc(sizeof(map<vint,Node>));
		new (nodes) map<vint,Node>();
	}

	virtual void readData(PartitionReader &inputReader){
		do{
			const vint dst = inputReader.getIntRef(0);
			const vint dst_edge = inputReader.getIntRef(1);
			const vfloat dst_value = inputReader.getFloatRef(2);

			if(nodes->find(dst) == nodes->end()){	// node does not exist
				Node n;
				nodes->insert(pair<vint,Node>(dst,n));
			}
			if(dst_edge==vint_null)
				nodes->at(dst).value = dst_value;
			else
				nodes->at(dst).edges.push_back(dst_edge);
		}while(inputReader.next());

		num_nodes = nodes->size();
		debug(INFO,"number of nodes = %d", num_nodes);

		currNode = nodes->begin();
		while(currNode != nodes->end())
			doCompute();
		resetData();
		//debug(INFO, "messages = %ld",iteration,messageCount(messageStores));
		//flush();
	}

	virtual bool prepareCompute(int *currRMsgIdx){
		int i; vint minId=LONG_MAX;
		for(i=0;i<max_num_stores;i++){
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

		for(;currNode!=nodes->end() && currNode->first<minId;currNode++);	// skip all nodes not having any message

		for(i=0;i<max_num_stores;i++){
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
		currNode++;
	}

	virtual void resetData(){
		currNode = nodes->begin();
	}

	virtual void writeData(PartitionWriter &outputWriter){
		// output the new vertex values (TODO: could be optimized to output ONLY the modified vertices)
		for(currNode=nodes->begin(); currNode!=nodes->end(); currNode++) {
			outputWriter.setInt(0, currNode->first);
			outputWriter.setFloat(1,currNode->second.value);
			outputWriter.setInt(2,(vint)(getPartitionId(currNode->first)));
			outputWriter.next();
		}
	}





	/**
	 *
	 * Pregel API
	 *
	 */

	virtual void compute(vector<vfloat> messages) = 0;

	virtual int getPartitionId(long id){
		return (int)(id % max_num_stores);	// random salt
	}

	virtual vint getVertexId(){
		return currNode->first;
	}
	virtual vfloat getVertexValue(){
		return currNode->second.value;
	}
	virtual vector<vfloat> getMessages(){
		return nodeMessages;
	}
	virtual vector<vint> getOutEdges(){
		return currNode->second.edges;
	}
	virtual void modifyVertexValue(double value){
		currNode->second.value = value;
	}
	virtual void sendMessage(int destination, double message){
		int partitionId = getPartitionId(destination);
		MessageStore ms = getMessageStore(partitionId,instance_id);
#ifdef SORT_BASED
		int writeMsgIdx = (*ms.woff) + (*ms.num_wmessages);
		ms.messages[writeMsgIdx].key = (vint)destination;
		ms.messages[writeMsgIdx].value = (vfloat)message;
		*ms.num_wmessages = *ms.num_wmessages + 1;
#elif defined(HASH_BASED)
		ms.wmessages->insert(pair<vint,vfloat>(destination,message));
#endif
	}
	virtual void voteToHalt(){
		currNode->second.state = 0;
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
	}
};

