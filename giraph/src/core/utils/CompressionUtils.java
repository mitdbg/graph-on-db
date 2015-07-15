package core.utils;

import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;

public class CompressionUtils {

	public static byte[] compressGZIP(byte[] bytes){
	     byte[] output = new byte[bytes.length];
	     Deflater compresser = new Deflater(Deflater.BEST_SPEED);
	     compresser.setInput(bytes);
	     compresser.finish();
	     int compressedLength = compresser.deflate(output);
	     compresser.end();
	     return BinaryUtils.resize(output, compressedLength);
	}
	
	public static byte[] decompressGZIP(byte[] bytes){
		byte[] output = new byte[bytes.length*20];
		
//		if(bytes.length > 50)
//			output = new byte[bytes.length*50];		// compression ratio up to 50.
//		else
//			output = new byte[bytes.length*500];	// anticipating very high compression ratios
		
		Inflater decompresser = new Inflater();
		decompresser.setInput(bytes, 0, bytes.length);
		try {
			int decompressedLength = decompresser.inflate(output);
			decompresser.end();
			return BinaryUtils.resize(output, decompressedLength);
		} catch (DataFormatException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to decompress: "+e.getMessage());
		}
	}

	public static byte[] compressHadoop(byte[] bytes){
		byte[] output = new byte[bytes.length];
		DefaultCodec codec = new DefaultCodec();
		Compressor compressor = codec.createCompressor();
		compressor.setInput(bytes, 0, bytes.length);
		compressor.finish();
		try {
			int compressedLength = compressor.compress(output, 0, output.length);
			compressor.end();
			return BinaryUtils.resize(output, compressedLength);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to compress: "+e.getMessage());
		}
	}
	
	public static byte[] decompressHadoop(byte[] bytes){
		byte[] output;
		if(bytes.length > 50)
			output = new byte[bytes.length*50];		// compression ratio up to 50.
		else
			output = new byte[bytes.length*500];	// anticipating very high compression ratios
		
		DefaultCodec codec = new DefaultCodec();
		Decompressor decompressor = codec.createDecompressor();
		decompressor.setInput(bytes, 0, bytes.length);
		try {
			int decompressedLength = decompressor.decompress(output, 0, output.length);
			decompressor.end();
			return BinaryUtils.resize(output, decompressedLength);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to decompress: "+e.getMessage());
		}
	}
}
