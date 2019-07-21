package com.aliware.tianchi;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.LoadBalance;

import com.aliware.tianchi.comm.ServerLoadInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daofeng.xjf
 *
 * 负载均衡扩展接口
 * 必选接口，核心接口
 * 此类可以修改实现，不可以移动类或者修改包名
 * 选手需要基于此类实现自己的负载均衡算法
 */
public class UserLoadBalance implements LoadBalance{
    
    @Override
    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
        
        int size = invokers.size();
        int selectIndex = -1;
        int maxWeight = 0;
        // 首先获取invoker对应的服务端耗时最大的索引
        for(int index=0;index<size;index++){
            Invoker<T> invoker = invokers.get(index);
            ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
            AtomicInteger limiter = UserLoadBalanceService.getAtomicInteger(invoker);
            if(serverLoadInfo != null){
                int permits = limiter.get();
                int weight = serverLoadInfo.getWeight();
                if(permits > 0 && permits>=weight){
                    // 单个CPU可处理的线程数量,值越大说明处理越快
                    int realWeight = permits/weight;
                    if(realWeight > maxWeight){
                        selectIndex = index;
                        maxWeight = realWeight;
                    }
                }
            }
        }
        
        
        if(selectIndex == -1){
            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
        }
        
        return invokers.get(selectIndex);
    }
    
//    @Override
//    public <T> Invoker<T> select(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {
//        
//        int size = invokers.size();
//        // 总权重
//        int totalWeight = 0;
//        List<Integer> hasPermitArr = new ArrayList<>();
//        List<Integer> weightArr = new ArrayList<>();
//        // 首先获取invoker对应的服务端耗时最大的索引
//        for(int index=0;index<size;index++){
//            Invoker<T> invoker = invokers.get(index);
//            ServerLoadInfo serverLoadInfo = UserLoadBalanceService.getServerLoadInfo(invokers.get(index));
//            AtomicInteger limiter = UserLoadBalanceService.getAtomicInteger(invoker);
//            if(serverLoadInfo != null){
//                int permits = limiter.get();
//                int weight = serverLoadInfo.getWeight();
//                if(permits > 0){
//                    int avgSpentTime = serverLoadInfo.getAvgSpendTime();
//                    if(avgSpentTime == 0){
//                        continue;
//                    }
//                    // CPU数量*(1秒内处理个数)
//                    int realWeight = weight * (1000/avgSpentTime);
//                    hasPermitArr.add(index);
//                    weightArr.add(realWeight);
//                    totalWeight = totalWeight+realWeight;
//                }
//            }
//        }
//        
//        
//        if(hasPermitArr.size() == 0){
//            return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//        }
//        // 根据服务端配置和平均耗时计算权重
//        int offsetWeight = ThreadLocalRandom.current().nextInt(totalWeight);
//        
//        for(int i=0;i<hasPermitArr.size();i++){
//            int index = hasPermitArr.get(i);
//            int currentWeight = weightArr.get(i);
//            offsetWeight  = offsetWeight - currentWeight;
//            if (offsetWeight < 0) {
//                return invokers.get(index);
//            }
//        }
//        
//        return invokers.get(ThreadLocalRandom.current().nextInt(invokers.size()));
//    }
    
}
