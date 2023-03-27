package com.demo.db.bachend.utils;

import java.nio.ByteBuffer;

public class Parser {
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }
    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }
}
