package com.aliware.tianchi.comm;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 * @desc: <服务端负载信息>
 * @author: KANBO  
 * @date:2019年6月22日 下午1:37:53
 */
public class ServerLoadInfo {
    
    private String quota = null;
    // 等于服务端线程数量
    private int providerThread = 0;
    // 权重
    private volatile int weight = 0;
    // 请求总数(上一秒)
    private AtomicLong reqCount = new AtomicLong(0);
    // 当前任务数量
    private AtomicLong activeCount = new AtomicLong(0);
    // 总耗时(上一秒)
    private AtomicLong spendTimeTotal = new AtomicLong(0);
    private volatile int avgSpendTime;
    private volatile int avgCountPerThread;
    
    public ServerLoadInfo(){
        
    }
    public ServerLoadInfo(String quota,int providerThread,int cpuCount){
        
        this.quota = quota;
        this.providerThread = (int) (providerThread * 0.9);
        //this.providerThread = providerThread;
        this.weight = cpuCount;
    }
    
    public String getQuota() {
    
        return quota;
    }

    public void setQuota(String quota) {
    
        this.quota = quota;
    }

    public int getWeight() {
    
        return weight;
    }
    
    public void setWeight(int weight) {
    
        this.weight = weight;
    }
    
    public AtomicLong getReqCount() {
    
        return reqCount;
    }
    
    public void setReqCount(AtomicLong reqCount) {
    
        this.reqCount = reqCount;
    }
    
    public AtomicLong getActiveCount() {
    
        return activeCount;
    }
    
    public void setActiveCount(AtomicLong activeCount) {
    
        this.activeCount = activeCount;
    }
    
    public AtomicLong getSpendTimeTotal() {
    
        return spendTimeTotal;
    }
    
    public void setSpendTimeTotal(AtomicLong spendTimeTotal) {
    
        this.spendTimeTotal = spendTimeTotal;
    }
    
    public int getAvgSpendTime() {
    
        return avgSpendTime;
    }

    public void setAvgSpendTime(int avgSpendTime) {
    
        this.avgSpendTime = avgSpendTime;
    }
    
    public int getProviderThread() {
    
        return providerThread;
    }
    
    public void setProviderThread(int providerThread) {
    
        this.providerThread = providerThread;
    }
    
    public int getAvgCountPerThread() {
    
        return avgCountPerThread;
    }
    
    public void setAvgCountPerThread(int avgCountPerThread) {
    
        this.avgCountPerThread = avgCountPerThread;
    }
    
}
