package core.utils;

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;

public class Metadata {

	public static enum Type {INT,LONG,FLOAT,DOUBLE,STRING}; 
	private List<Object> elements;
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");
	
	public Metadata(String record, Type[] types){
		elements = Lists.newArrayList();
		String[] tokens = SEPARATOR.split(record.toString());
		if(tokens.length!=types.length)
			throw new RuntimeException("Number of attributes expectected: "+types.length+", FOUND: "+tokens.length);
		for(int i=0; i<tokens.length; i++){
			switch(types[i]){
			case INT:		elements.add(Integer.valueOf(tokens[i])); break;
			case LONG:		elements.add(Long.valueOf(tokens[i])); break;
			case FLOAT:		elements.add(Float.valueOf(tokens[i])); break;
			case DOUBLE:	elements.add(Double.valueOf(tokens[i])); break;
			case STRING:	elements.add(tokens[i]); break;
			}
		}
	}
	public Object get(int index){
		return elements.get(index);
	}
	
	public Integer getInteger(int index){
		return (Integer)get(index);
	}
	
	public Long getLong(int index){
		return (Long)get(index);
	}
	
	public Float getFloat(int index){
		return (Float)get(index);
	}
	
	public Double getDouble(int index){
		return (Double)get(index);
	}
	
	public String getString(int index){
		return (String)get(index);
	}
}
