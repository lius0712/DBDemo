package com.demo.db.bachend.dm.page;

public interface Page {
    void lock();
    void unLock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getPageData();
}
