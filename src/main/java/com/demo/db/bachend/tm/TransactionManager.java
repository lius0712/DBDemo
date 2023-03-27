package com.demo.db.bachend.tm;

import com.demo.db.bachend.err.Error;
import com.demo.db.bachend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin(); //开启事务
    void commit(long tid); //提交一个事务
    void abort(long tid); //撤销一个事务
    boolean isActive(long tid); //查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long tid);//查询一个事务的状态是否是已提交
    boolean isAborted(long tid);//查询一个事务的状态是否是已取消
    void close();// 关闭TM

    public static TransactionManagerImpl create(String path) {
        File f = new File(path + TransactionManagerImpl.TID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_TID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }

    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.TID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf, fc);
    }
}
