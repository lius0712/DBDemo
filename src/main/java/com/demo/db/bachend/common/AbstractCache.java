package com.demo.db.bachend.common;

import com.demo.db.bachend.err.Error;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache实现一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache; //缓存的数据
    private HashMap<Long, Integer> refers; //资源引用的个数
    private HashMap<Long, Boolean> getting; //正在获取某资源的线程

    private int maxResource; //缓存最大资源数量
    private int count;  //缓存元素个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        refers = new HashMap<>();
        getting = new HashMap<>();
        count = 0;
        lock = new ReentrantLock();
    }
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            // 请求的资源正在被其他线程获取
            if(getting.get(key) != null) {
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }
            // 资源已经在缓存中
            if(cache.get(key) != null) {
                T obj = cache.get(key);
                refers.put(key, refers.get(key) + 1);
                lock.unlock();
                return obj;
            }
            //当前资源已满
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            //若未能命中缓存
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw new RuntimeException(e);
        }
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        refers.put(key, 1);
        lock.unlock();
        return obj;
    }
    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseKeyForCache(T obj);
}
