package io.otdd.ddl.mysql.common;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;

/*
 * https://dev.mysql.com/doc/internals/en/mysql-packet.html
 */
public class PacketHeader {
	public int payload_length = 0;
	public byte sequence_id = 0;
	
	//http://www.darksleep.com/player/JavaAndUnsignedTypes.html
	public static PacketHeader fromBytes(byte[] bytes){
		if(bytes==null||bytes.length<4){
			return null;
		}
		PacketHeader ret = new PacketHeader();
		int firstByte = 0;
        int secondByte = 0;
        int thirdByte = 0;
        
        firstByte = (0x000000FF & ((int)bytes[0]));
        secondByte = (0x000000FF & ((int)bytes[1]));
        thirdByte = (0x000000FF & ((int)bytes[2]));
        
		ret.payload_length = ((int)(thirdByte<<16|secondByte<<8|firstByte<<0))&0xFFFF;
		ret.sequence_id = bytes[3];
		return ret;
	}
	
	public byte[] getBytes() {
		ByteArrayOutputStream byteArray = new ByteArrayOutputStream();
		ProtocolUtil.g_string_append_c(byteArray, (byte)((payload_length >> 0) & 0xff));
		ProtocolUtil.g_string_append_c(byteArray, (byte)((payload_length >> 8) & 0xff));
		ProtocolUtil.g_string_append_c(byteArray, (byte)((payload_length >> 16) & 0xff));
		
		ProtocolUtil.g_string_append_c(byteArray, sequence_id);
		return byteArray.toByteArray();
	}
	
	public static void main(String args[]){
		PacketHeader header = new PacketHeader();
		header.payload_length = 14345;
//		header.payload_length = 7;
		header.sequence_id = 8;
		System.out.println(Arrays.toString(header.getBytes()));
		System.out.println("payload_length:"+header.payload_length);
		System.out.println("sequence_id:"+(short)header.sequence_id);

		
		PacketHeader newHeader = PacketHeader.fromBytes(header.getBytes());
		System.out.println(Arrays.toString(newHeader.getBytes()));
		System.out.println("payload_length:"+newHeader.payload_length);
		System.out.println("sequence_id:"+(short)newHeader.sequence_id);

		
//		byte[] bytes = new byte[]{
//			-34, 0, 0, 1, -113, -94, 62, 0, -1, -1, -1, 0, 33, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 114, 111, 111, 116, 0, 20, 22, 116, 50, 95, -109, 90, -74, -10, -91, -28, -105, -13, 36, 76, 29, -7, 69, 35, 112, -58, 111, 116, 100, 100, 0, 109, 121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 0, -120, 16, 95, 114, 117, 110, 116, 105, 109, 101, 95, 118, 101, 114, 115, 105, 111, 110, 9, 49, 46, 56, 46, 48, 95, 49, 55, 49, 15, 95, 99, 108, 105, 101, 110, 116, 95, 118, 101, 114, 115, 105, 111, 110, 6, 56, 46, 48, 46, 49, 49, 12, 95, 99, 108, 105, 101, 110, 116, 95, 110, 97, 109, 101, 17, 77, 121, 83, 81, 76, 32, 67, 111, 110, 110, 101, 99, 116, 111, 114, 47, 74, 15, 95, 99, 108, 105, 101, 110, 116, 95, 108, 105, 99, 101, 110, 115, 101, 3, 71, 80, 76, 15, 95, 114, 117, 110, 116, 105, 109, 101, 95, 118, 101, 110, 100, 111, 114, 18, 79, 114, 97, 99, 108, 101, 32, 67, 111, 114, 112, 111, 114, 97, 116, 105, 111, 110
//		};
		
		byte[] bytes = new byte[]{
				1, 0, 0, 1, 4, 38, 0, 0, 2, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0, 42, 0, 0, 3, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 110, 97, 109, 101, 4, 110, 97, 109, 101, 12, 33, 0, -121, 0, 0, 0, -3, 0, 0, 0, 0, 0, 42, 0, 0, 4, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 116, 121, 112, 101, 4, 116, 121, 112, 101, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 56, 0, 0, 5, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 12, 63, 0, 19, 0, 0, 0, 12, -128, 0, 0, 0, 0, 30, 0, 0, 6, 1, 49, 3, 97, 97, 97, 3, 50, 50, 50, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 50, 49, 30, 0, 0, 7, 1, 50, 3, 98, 98, 98, 3, 51, 51, 51, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 51, 51, 7, 0, 0, 8, -2, 0, 0, 34, 0, 0, 0
		};
		
		PacketHeader byteHeader = PacketHeader.fromBytes(bytes);
		System.out.println(Arrays.toString(byteHeader.getBytes()));
		System.out.println("payload_length:"+byteHeader.payload_length);
		System.out.println("sequence_id:"+(short)byteHeader.sequence_id);
		
		System.out.println(ProtocolUtil.validMysqlPacket(bytes,0,bytes.length));

		System.out.println(bytes.length);

	}
}
