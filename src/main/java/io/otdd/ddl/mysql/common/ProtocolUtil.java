package io.otdd.ddl.mysql.common;

import java.io.ByteArrayOutputStream;

//refer to network-mysqld-proto.c
public class ProtocolUtil {

	private static void network_mysqld_proto_append_int_len(ByteArrayOutputStream byteArray, long num, int size) {
		int i = 0;
		for (i = 0; i < size; i++) {
			g_string_append_c(byteArray, (byte)(num & 0xff));
			num >>= 8;
		}
	}

	public static void g_string_append_c(ByteArrayOutputStream byteArray, byte c) {
		byteArray.write(c);
	}

	public static void network_mysqld_proto_append_int32(ByteArrayOutputStream byteArray,int i){
		network_mysqld_proto_append_int_len(byteArray, i, 4);
	}

	public static void network_mysqld_proto_append_int8(ByteArrayOutputStream byteArray,char i){
		network_mysqld_proto_append_int_len(byteArray, (long)i, 1);
	}

	public static void network_mysqld_proto_append_lenenc_int(ByteArrayOutputStream packet, long length) {
		if (length < 251) {
			g_string_append_c(packet, (byte)length);
		} else if (length < 65536) {
			g_string_append_c(packet, (byte)252);
			g_string_append_c(packet, (byte)((length >> 0) & 0xff));
			g_string_append_c(packet, (byte)((length >> 8) & 0xff));
		} else if (length < 16777216) {
			g_string_append_c(packet, (byte)253);
			g_string_append_c(packet, (byte)((length >> 0) & 0xff));
			g_string_append_c(packet, (byte)((length >> 8) & 0xff));
			g_string_append_c(packet, (byte)((length >> 16) & 0xff));
		} else {
			g_string_append_c(packet, (byte)254);

			g_string_append_c(packet, (byte)((length >> 0) & 0xff));
			g_string_append_c(packet, (byte)((length >> 8) & 0xff));
			g_string_append_c(packet, (byte)((length >> 16) & 0xff));
			g_string_append_c(packet, (byte)((length >> 24) & 0xff));

			g_string_append_c(packet, (byte)((length >> 32) & 0xff));
			g_string_append_c(packet, (byte)((length >> 40) & 0xff));
			g_string_append_c(packet, (byte)((length >> 48) & 0xff));
			g_string_append_c(packet, (byte)((length >> 56) & 0xff));
		}
	}

	public static void network_mysqld_proto_append_int16(ByteArrayOutputStream byteArray,short i) {
		network_mysqld_proto_append_int_len(byteArray, i, 2);
	}

	public static boolean validMysqlPacket(byte[] req,int leftIndex,int rightIndex) {
		if(rightIndex-leftIndex<4){
			return false;
		}
		byte[] headerBytes = new byte[4];
		System.arraycopy(req, leftIndex, headerBytes, 0, 4);
		PacketHeader header = PacketHeader.fromBytes(headerBytes);
		int expectedLength = rightIndex - leftIndex - 4;
		if(header.payload_length==expectedLength){
			return true;
		}
		else if(header.payload_length>expectedLength){
			return false;
		}
		else{//there are multiple packets.
			return validMysqlPacket(req,leftIndex+header.payload_length+4,rightIndex);
		}
	}

	public static byte getSeqId(byte[] req) {
		if(req.length>4){
			return (byte)req[3];
		}
		return 0;
	}

	public static void main(String args[]){
		byte[] bytes = new byte[]{
				1, 0, 0, 1, 4, 38, 0, 0, 2, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 2, 105, 100, 2, 105, 100, 12, 63, 0, 11, 0, 0, 0, 3, 3, 80, 0, 0, 0, 42, 0, 0, 3, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 110, 97, 109, 101, 4, 110, 97, 109, 101, 12, 33, 0, -121, 0, 0, 0, -3, 0, 0, 0, 0, 0, 42, 0, 0, 4, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 4, 116, 121, 112, 101, 4, 116, 121, 112, 101, 12, 63, 0, 11, 0, 0, 0, 3, 0, 0, 0, 0, 0, 56, 0, 0, 5, 3, 100, 101, 102, 4, 111, 116, 100, 100, 4, 116, 101, 115, 116, 4, 116, 101, 115, 116, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 11, 105, 110, 115, 101, 114, 116, 95, 116, 105, 109, 101, 12, 63, 0, 19, 0, 0, 0, 12, -128, 0, 0, 0, 0, 30, 0, 0, 6, 1, 49, 3, 97, 97, 97, 3, 50, 50, 50, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 50, 49, 30, 0, 0, 7, 1, 50, 3, 98, 98, 98, 3, 51, 51, 51, 19, 50, 48, 49, 56, 45, 48, 55, 45, 49, 57, 32, 49, 56, 58, 49, 50, 58, 51, 51, 7, 0, 0, 8, -2, 0, 0, 34, 0, 0, 0
		};
		System.out.println(ProtocolUtil.validMysqlPacket(bytes,0,bytes.length));
	}

	public static int getNumOfMysqlPacketNum(byte[] req,int leftIndex,int rightIndex) {
		if(rightIndex-leftIndex<4){
			return 0;
		}
		byte[] headerBytes = new byte[4];
		System.arraycopy(req, leftIndex, headerBytes, 0, 4);
		PacketHeader header = PacketHeader.fromBytes(headerBytes);
		int expectedLength = rightIndex - leftIndex - 4;
		if(header.payload_length==expectedLength){
			return 1;
		}
		else if(header.payload_length>expectedLength){
			return 0;
		}
		else{//there are multiple packets.
			return 1+getNumOfMysqlPacketNum(req,leftIndex+header.payload_length+4,rightIndex);
		}
	}
}
