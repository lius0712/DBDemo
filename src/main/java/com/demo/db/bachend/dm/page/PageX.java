package com.demo.db.bachend.dm.page;

import com.demo.db.bachend.dm.pageCache.PageCache;
import com.demo.db.bachend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置偏移量
 */
public class PageX {
    private static final short OF_START = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_START, OF_DATA);
    }
    public static short getFSO(Page pg) {
        return getFSO(pg.getPageData());
    }
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }
    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getPageData());
    }
    //将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getPageData());
        System.arraycopy(raw, 0, pg.getPageData(), offset, raw.length);
        setFSO(pg.getPageData(), (short)( offset + raw.length));
        return offset;
    }
    //用于在数据库崩溃后重新打开时，恢复例程直接插入数据
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getPageData(), offset, raw.length);
        short rawFSO = getFSO(pg.getPageData());
        //数据库崩溃后插入的位置大于原本的数据长度
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getPageData(), (short)(offset+raw.length));
        }
    }
    //用于在数据库崩溃后重新打开时，恢复例程更新数据
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getPageData(), offset, raw.length);
    }
}
