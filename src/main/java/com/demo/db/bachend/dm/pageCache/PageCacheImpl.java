package com.demo.db.bachend.dm.pageCache;

import com.demo.db.bachend.common.AbstractCache;
import com.demo.db.bachend.dm.page.Page;
import com.demo.db.bachend.dm.page.PageImpl;
import com.demo.db.bachend.err.Error;
import com.demo.db.bachend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNums;

    public PageCacheImpl(RandomAccessFile file, FileChannel fc, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNums = new AtomicInteger((int)length / PAGE_SIZE);
    }

    public void flush(Page pg) {
        int pgNum = pg.getPageNumber();
        long offset = pageOffSet(pgNum);
        fileLock.lock();
        try{
            ByteBuffer buf = ByteBuffer.wrap(pg.getPageData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNum = pageNums.incrementAndGet();
        Page page = new PageImpl(pageNum, initData, null);
        flush(page);
        return pageNum;
    }

    @Override
    public Page getPage(int pageNum) throws Exception {
        return get((long)pageNum);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByPgNum(int maxPgNum) {
        long size = maxPgNum + 1;
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNums.set(maxPgNum);
    }

    @Override
    public int getPageNum() {
        return pageNums.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
       int pgNum = (int) key;
       long offset = PageCacheImpl.pageOffSet(pgNum);
       ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
       fileLock.lock();
       try {
           fc.position(offset);
           fc.read(buf);
       } catch (IOException e) {
           Panic.panic(e);
       }
       fileLock.unlock();
       return new PageImpl(pgNum, buf.array(), this);
    }

    @Override
    protected void releaseKeyForCache(Page obj) {
        if(obj.isDirty()) {
            flush(obj);
            obj.setDirty(false);
        }
    }

    public static long pageOffSet(int pageNum) {
        return (long) (pageNum - 1) * PAGE_SIZE;
    }
}
