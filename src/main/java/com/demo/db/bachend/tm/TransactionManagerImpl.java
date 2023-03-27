package com.demo.db.bachend.tm;

import com.demo.db.bachend.err.Error;
import com.demo.db.bachend.utils.Panic;
import com.demo.db.bachend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager{
    //存储事务信息头文件，代表事务的数量,占用8B
    static final int LEN_TID_HEADER_LENGTH = 8;
    // 每个事务的占用长度, 1B
    private static final int TID_FIELD_SIZE = 1;
    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED  = 2;
    // 超级事务，永远为committed状态
    public static final long SUPER_TID = 0;
    static final String XID_SUFFIX = ".tid";

    private FileChannel fc;
    private long tidCounter;
    private Lock counterLock;
    private RandomAccessFile file;

    TransactionManagerImpl(RandomAccessFile f, FileChannel fc) {
        this.file = f;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkTIDCounter();
    }
    /*
    检测tid文件是否合法
     */
    private void checkTIDCounter(){
        long fileLen = 0;
        try{
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadTIDFileException);
        }
        if(fileLen < LEN_TID_HEADER_LENGTH) {
            Panic.panic(Error.BadTIDFileException);
        }
        ByteBuffer buf = ByteBuffer.allocate(LEN_TID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.tidCounter = Parser.parseLong(buf.array());
        long end = getTidPosition(tidCounter);
        if(end != fileLen) {
            Panic.panic(Error.BadTIDFileException);
        }
    }
    //根据事务tid取得在文件中的位置
    private long getTidPosition(long tid) {
        return LEN_TID_HEADER_LENGTH + (tid - 1) * TID_FIELD_SIZE;
    }
    //将TID加1， 更新TIDHeader
    private void incrTIDCounter() {
        tidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(tidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false); //强迫写入需要更新的数据
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    //更新事务的状态
    private void updateTID(long tid, byte status) {
        long offset = getTidPosition(tid);
        byte[] tmp = new byte[TID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    //开启一个事务
    @Override
    public long begin() {
        counterLock.lock();
        try {
            long tid = tidCounter + 1;
            updateTID(tid, FIELD_TRAN_ACTIVE);
            incrTIDCounter();
            return tid;
        } finally {
            counterLock.unlock();
        }
    }
    //提交事务
    @Override
    public void commit(long tid) {
        updateTID(tid, FIELD_TRAN_COMMITTED);
    }
    //回滚事务
    @Override
    public void abort(long tid) {
        updateTID(tid, FIELD_TRAN_ABORTED);
    }

    private boolean checkTID(long tid, byte status) {
        long offset = getTidPosition(tid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[TID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long tid) {
        if(tid == SUPER_TID) return false;
        return checkTID(tid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long tid) {
        if(tid == SUPER_TID) return false;
        return checkTID(tid, FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long tid) {
        if(tid == SUPER_TID) return false;
        return checkTID(tid, FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
