/*
 * Aggregates.cpp
 *
 *  Created on: May 26, 2014
 *      Author: alekh
 */

#include "Vertica.h"

using namespace Vertica;


class Product_agg : public AggregateFunction {
	virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs){
	    vint &prd = aggs.getIntRef(0);
	    prd = 1;
	}
	void aggregate(ServerInterface &srvInterface, BlockReader &arg_reader, IntermediateAggs &aggs){
		vint &prd = aggs.getIntRef(0);
		do{
			const vint &input = arg_reader.getIntRef(0);
			prd *= input;
		}while(arg_reader.next());
	}
	virtual void combine(ServerInterface &srvInterface, IntermediateAggs &aggs,MultipleIntermediateAggs &aggs_other){
		vint &myPrd = aggs.getIntRef(0);
		// Combine all the other intermediate aggregates
		do{
			const vint &otherPrd = aggs_other.getIntRef(0);
			myPrd *= otherPrd;
		}while(aggs_other.next());
	}
	virtual void terminate(ServerInterface &srvInterface, BlockWriter &res_writer,IntermediateAggs &aggs){
		const vint prd = aggs.getIntRef(0);
		res_writer.setInt(prd);
	}
	InlineAggregate()
};


class ProductFactory : public AggregateFunctionFactory {
	virtual void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &input_types, SizedColumnTypes &intermediateTypeMetaData){
		intermediateTypeMetaData.addInt("prd");
	}
	virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType){
		argTypes.addInt();
		returnType.addInt();
	}
	virtual void getReturnType(ServerInterface &srvfloaterface, const SizedColumnTypes &input_types, SizedColumnTypes &output_types){
		output_types.addInt("prd");
	}
	virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
	{ return vt_createFuncObj(srvfloaterface.allocator, Product_agg); }
};

RegisterFactory(ProductFactory);





class GCD_agg : public AggregateFunction {
	virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs){
	    vint &gcd = aggs.getIntRef(0);
	    gcd = 0;
	}
	void aggregate(ServerInterface &srvInterface, BlockReader &arg_reader, IntermediateAggs &aggs){
		vint &gcd = aggs.getIntRef(0);
		do{
			const vint &input = arg_reader.getIntRef(0);
			if(gcd==0)
				gcd = input;
			else
				gcd = find_gcd(gcd,input);
		}while(arg_reader.next());
	}
	virtual void combine(ServerInterface &srvInterface, IntermediateAggs &aggs,MultipleIntermediateAggs &aggs_other){
		vint &myGcd = aggs.getIntRef(0);
		// Combine all the other intermediate aggregates
		do{
			const vint &otherGcd = aggs_other.getIntRef(0);
			myGcd = find_gcd(myGcd,otherGcd);
		}while(aggs_other.next());
	}
	virtual vint find_gcd(vint a, vint b){
		vint dividend = a>b ? a : b;
		vint divisor = a>b ? b : a;
		vint quotient, remainder;
		while(true){
			quotient = dividend / divisor;
			remainder = dividend % divisor;
			if(remainder==0)
				break;
			dividend = divisor;
			divisor = remainder;
		}
		return divisor;
	}
	virtual void terminate(ServerInterface &srvInterface, BlockWriter &res_writer,IntermediateAggs &aggs){
		const vint gcd = aggs.getIntRef(0);
		res_writer.setInt(gcd);
	}
	InlineAggregate()
};

class GCDFactory : public AggregateFunctionFactory {
	virtual void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &input_types, SizedColumnTypes &intermediateTypeMetaData){
		intermediateTypeMetaData.addInt("gcd");
	}
	virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType){
		argTypes.addInt();
		returnType.addInt();
	}
	virtual void getReturnType(ServerInterface &srvfloaterface, const SizedColumnTypes &input_types, SizedColumnTypes &output_types){
		output_types.addInt("gcd");
	}
	virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
	{ return vt_createFuncObj(srvfloaterface.allocator, GCD_agg); }
};

RegisterFactory(GCDFactory);







class GCD_scalar : public ScalarFunction{
public:
	virtual void processBlock(ServerInterface &srvInterface, BlockReader &arg_reader, BlockWriter &res_writer){
		do{
			const vint a = arg_reader.getIntRef(0);
			const vint b = arg_reader.getIntRef(1);
			res_writer.setInt(find_gcd(a,b));
			res_writer.next();
		}
		while (arg_reader.next());
	}
	virtual vint find_gcd(vint a, vint b){
		vint dividend = a>b ? a : b;
		vint divisor = a>b ? b : a;
		vint quotient, remainder;
		while(true){
			quotient = dividend / divisor;
			remainder = dividend % divisor;
			if(remainder==0)
				break;
			dividend = divisor;
			divisor = remainder;
		}
		return divisor;
	}
};
class GCDFactory_scalar : public ScalarFunctionFactory{
	virtual void getPrototype(ServerInterface &interface, ColumnTypes &argTypes, ColumnTypes &returnType){
		argTypes.addInt();
		argTypes.addInt();
		returnType.addInt();
	}
	virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
	{ return vt_createFuncObj(interface.allocator, GCD_scalar); }
};

RegisterFactory(GCDFactory_scalar);
