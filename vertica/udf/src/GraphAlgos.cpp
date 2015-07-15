/*
 * Algos.cpp
 *
 *  Created on: Sep 27, 2014
 *      Author: alekh
 */


#include <cfloat>
#include <set>


#ifdef UNSORT_INITIAL
	#include "PregelCompute_unsorted.h"
#elif defined(SORT_INITIAL)
	#include "PregelCompute.h"
#else
	#include "PregelCompute.h"
#endif


/**
 *
 *  Algorithm 1: PageRank
 *
 */
class AlgoPageRank : public PregelCompute{
protected:
	int NUM_NODES, NUM_ITERATIONS;
public:
	// read the params neded by PageRank
	virtual void readParams(ParamReader paramReader){
		const vint numNodes = paramReader.getIntRef("numNodes");
		const vint numIterations = paramReader.getIntRef("numIterations");
		NUM_NODES = numNodes;
		NUM_ITERATIONS = numIterations;
	}
	// the actual compute function for PageRank
	virtual void compute(vector<vfloat> messages){
		vfloat value;
		if(iteration >= 1){
			vfloat sum = 0;
			for(vector<vfloat>::iterator it = messages.begin(); it != messages.end(); ++it)
				sum += *it;
			value = 0.15/NUM_NODES + 0.85*sum;
		}
		else
			value = 1.0/NUM_NODES;
		modifyVertexValue(value);
		if(iteration < NUM_ITERATIONS){
			vector<vint> edges = getOutEdges();
			for(vector<vint>::iterator it = edges.begin(); it != edges.end(); ++it){
				sendMessage(*it, value/edges.size());
			}
		}
		voteToHalt();
	}
};
class PregelPageRankFactory : public VertexFactory{
	virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes){
		parameterTypes.addInt("numNodes");
		parameterTypes.addInt("numIterations");
	}
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, AlgoPageRank); }
};
RegisterFactory(PregelPageRankFactory);





/**
 *
 *  Algorithm 2: Shortest Paths
 *
 */
class PregelShortestPath : public PregelCompute{
protected:
	vint START_NODE, END_NODE;
public:
	virtual void readParams(ParamReader paramReader){
		const vint startNode = paramReader.getIntRef("startNode");
		const vint endNode = paramReader.getIntRef("endNode");
		START_NODE = startNode;
		END_NODE = endNode;
	}
	virtual void compute(vector<vfloat> messages){
		vfloat mindist = getVertexId()==START_NODE ? 0 : DBL_MAX;
		for(vector<vfloat>::iterator it = messages.begin(); it != messages.end(); ++it)
			mindist = min(mindist,(vfloat)*it);
		vfloat vvalue = getVertexValue();
		if(mindist < vvalue){
			modifyVertexValue(mindist);
			vector<vint> edges = getOutEdges();
			for(vector<vint>::iterator it = edges.begin(); it != edges.end(); ++it)
				sendMessage((vint)*it, mindist+1);
		}
		voteToHalt();
	}
};
class PregelShortestPathFactory : public VertexFactory{
	virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes){
		parameterTypes.addInt("startNode");
		parameterTypes.addInt("endNode");
	}
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelShortestPath);}
};
RegisterFactory(PregelShortestPathFactory);




/**
 *
 * Algorithm 3: Connected Componenets
 *
 */
class PregelConnectedComponent : public PregelCompute{
public:
	virtual void readParams(ParamReader paramReader){
	}
	virtual void compute(vector<vfloat> messages){
		vfloat componentId = getVertexValue();
		if(iteration==0){		// look only at the vertex ids for the first iteration
			componentId = getVertexId();
			vector<vint> edges = getOutEdges();
			for(vector<vint>::iterator it = edges.begin(); it != edges.end(); ++it)
				if(*it < componentId)
					componentId = *it;
		}
		else{		// find the minimum message
			for(vector<vfloat>::iterator it = messages.begin(); it != messages.end(); ++it)
				if(*it < componentId)
					componentId = *it;
		}
		if(componentId != getVertexValue()){	// send new value to all neighbors
			modifyVertexValue(componentId);
			vector<vint> edges = getOutEdges();
			for(vector<vint>::iterator it = edges.begin(); it != edges.end(); ++it)
				sendMessage((vint)*it, componentId);
		}
		voteToHalt();
	}
};
class PregelConnectedComponentFactory : public VertexFactory{
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelConnectedComponent); }
};
RegisterFactory(PregelConnectedComponentFactory);





/**
 *
 * Algorithm 4: Triangle Counting (each triangle is counted on one of the nodes)
 *  			... global count can be obtained by summing the values of the output vertices
 *
 */
class PregelTriangleCounting : public PregelCompute{
public:
	virtual void readParams(ParamReader paramReader){
	}
	virtual void compute(vector<vfloat> messages){
		if(iteration==0)	// send neighbors as messages
			sendNeighbors();
		else
			doCount(messages);
		voteToHalt();
	}
	virtual void sendNeighbors(){
		vint id = getVertexId();
		vector<vint> edges = getOutEdges();
		for(vector<vint>::iterator it1 = edges.begin(); it1 != edges.end(); ++it1)
			for(vector<vint>::iterator it2 = edges.begin(); it2 != edges.end(); ++it2)
				if(id < *it1 && *it2 < *it1)
					sendMessage((vint)*it1, (double)*it2);
	}
	virtual void doCount(vector<vfloat> messages){
		multiset<vfloat> ids;
		for(vector<vfloat>::iterator it = messages.begin(); it != messages.end(); ++it)
			ids.insert((vfloat)*it);
		vint triangleCount = 0;
		vector<vint> edges = getOutEdges();
		for(vector<vint>::iterator it = edges.begin(); it != edges.end(); ++it)
			triangleCount += ids.count((vfloat)*it);
		modifyVertexValue(triangleCount);	//TODO: the counts are not propagated to other vertices of the triangle
	}
};
class PregelTriangleCountingFactory : public VertexFactory{
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelTriangleCounting); }
};
RegisterFactory(PregelTriangleCountingFactory);




/**
 *
 * Algorithm 5: Pernode Triangle Counting
 *
 */
class PregelPernodeTriangleCounting : public PregelTriangleCounting{
public:
	virtual void sendNeighbors(){
		vector<vint> edges = getOutEdges();
		for(vector<vint>::iterator it1 = edges.begin(); it1 != edges.end(); ++it1)
			for(vector<vint>::iterator it2 = edges.begin(); it2 != edges.end(); ++it2)
				sendMessage((vint)*it1, (double)*it2);
	}
};
class PregelPernodeTriangleCountingFactory : public PregelTriangleCountingFactory{
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelPernodeTriangleCounting); }
};
RegisterFactory(PregelPernodeTriangleCountingFactory);




/**
 *
 * Algorithm 6: Weak Ties
 *
 */
class PregelWeakTies : public PregelPernodeTriangleCounting{
protected:
	int THRESHOLD;
public:
	virtual void readParams(ParamReader paramReader){
		const vint threshold = paramReader.getIntRef("threshold");
		THRESHOLD = threshold;
	}
	virtual void doCount(vector<vfloat> messages){
		PregelTriangleCounting::doCount(messages);
		vint numWeakTies = getOutEdges().size()*(getOutEdges().size()-1)/2 - getVertexValue();	// n(n-1)/2  - #triangles
		if(checkThreshold(numWeakTies))
			modifyVertexValue(numWeakTies);
		else
			modifyVertexValue(0);
	}
	virtual bool checkThreshold(vint value){
		return value >= THRESHOLD;
	}
};
class PregelWeakTiesFactory : public PregelTriangleCountingFactory{
	virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes){
		parameterTypes.addInt("threshold");
	}
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelWeakTies); }
};
RegisterFactory(PregelWeakTiesFactory);




/**
 *
 * Algorithm 7: Strong Overlap
 *
 */
class PregelStrongOverlap : public PregelWeakTies{
public:
	virtual void sendNeighbors(){
		vector<vint> edges = getOutEdges();
		for(vector<vint>::iterator it1 = edges.begin(); it1 != edges.end(); ++it1)
			for(vector<vint>::iterator it2 = edges.begin(); it2 != edges.end(); ++it2)
				if(*it2 < *it1)
					sendMessage((vint)*it1, (double)*it2);
	}
	virtual void doCount(vector<vfloat> messages){
		multiset<vfloat> ids;
		for(vector<vfloat>::iterator it = messages.begin(); it != messages.end(); ++it)
			ids.insert((vint)*it);

		vint overlapCount = 0;
		for(multiset<vfloat>::iterator it = ids.begin(); it != ids.end(); ++it)
			if(checkThreshold((vint)*it))
				overlapCount++;

		modifyVertexValue(overlapCount);
	}
};
class PregelStrongOverlapFactory : public PregelWeakTiesFactory{
	virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
	{ return vt_createFuncObj(srvInterface.allocator, PregelStrongOverlap); }
};
RegisterFactory(PregelStrongOverlapFactory);



// Maximal CLiques



