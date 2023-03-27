package com.demo.db.bachend.tm;

public interface TransactionManager {
    long begin(); //开启事务
    void commit(long tid); //提交一个事务
    void abort(long tid); //撤销一个事务
    boolean isActive(long tid); //查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long tid);//查询一个事务的状态是否是已提交
    boolean isAborted(long tid);//查询一个事务的状态是否是已取消
    void close();// 关闭TM

}
