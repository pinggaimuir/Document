package cn.tarena.utils;

import static java.lang.System.out;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;


public class MergeFileUtil {
	
	public static final int BUFSIZE = 1024 * 8;
	
	public static void mergeFiles(String outFile, String[] files) {
		FileChannel outChannel = null;
		out.println("Merge " + Arrays.toString(files) + " into " + outFile);
		try {
			outChannel = new FileOutputStream(outFile).getChannel();
			for(String f : files){
				FileChannel fc = new FileInputStream(f).getChannel(); 
				ByteBuffer bb = ByteBuffer.allocate(BUFSIZE);
				while(fc.read(bb) != -1){
					bb.flip();
					outChannel.write(bb);
					bb.clear();
				}
				fc.close();
			}
			out.println("Merged!! ");
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally {
			try {if (outChannel != null) {outChannel.close();}} catch (IOException ignore) {}
		}
	}
	
	public static void main(String[] args) {
		String filenames = "";	//"c:/Tulip.txt,c:/Tulip.txt,c:/Tulip.txt";
		for(int i=0;i<1000;i++){
			filenames +="c:/Tulip.txt,";
		}
		filenames = filenames.substring(0,filenames.length()-1);
		
		String[] files = filenames.split(","); 
		mergeFiles("c:/output.txt", files);
		System.out.println("finish.");
	}
}
