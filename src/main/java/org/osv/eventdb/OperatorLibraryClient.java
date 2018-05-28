package org.osv.eventdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryRequest;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryResponse;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryService;

/**
 * @author DZR
 * 说明：hbase协处理器endpoint的客户端代码
 * 功能：从服务端获取对hbase表指定列的数据的求和结果
 */

public class OperatorLibraryClient {
	//Gamma函数
	public static List<Double> Gamma(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn) throws Throwable{
	     // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
        // 设置请求对象
        final OperatorLibraryRequest request;
        request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();   
        // 获得返回值
        Map<byte[], List<Double>> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
                new Batch.Call<OperatorLibraryService, List<Double>>() {

                    @Override
                    public List<Double> call(OperatorLibraryService service) throws IOException {
                        BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                        service.getGamma(null, request, rpcCallback);
                        OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                        return response.getResultList();
                    }
        });
        // 将返回值进行迭代合并
        List<Double>  GammaResult=new ArrayList<>();
        for (List<Double> v : result.values()) {
        	if(v.size()==1&&v.get(0)==Double.MIN_VALUE)
        	{
        		GammaResult.add(Double.MIN_VALUE);break;
        	}
        	else
        	{
        		GammaResult.addAll(v);
        	}      	
        }
        // 关闭资源
        table.close();
        return GammaResult;
	}
	//Erf函数
	public static List<Double> Erf(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn) throws Throwable{
	     // 获取表
       HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
       // 设置请求对象;
       final OperatorLibraryRequest request;
       request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();    
       // 获得返回值
       Map<byte[], List<Double>> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
               new Batch.Call<OperatorLibraryService, List<Double>>() {

                   @Override
                   public List<Double> call(OperatorLibraryService service) throws IOException {
                       BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                       service.getErf(null, request, rpcCallback);
                       OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                       return response.getResultList();
                   }
       });
       // 将返回值进行迭代合并
       List<Double>  ErfResult=new ArrayList<>();
       for (List<Double> v : result.values()) {
    	   if(v.size()==1&&v.get(0)==Double.MIN_VALUE)
    	   {
    		   ErfResult.add(Double.MIN_VALUE);break;
    	   }
    	   else
    	   {
    		   ErfResult.addAll(v);
    	   }  	   
       }
       // 关闭资源
       table.close();
       return ErfResult;
	}
	//Sum函数
	public static Double Sum(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn)throws Throwable{
	     // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
        // 设置请求对象
        final OperatorLibraryRequest request;
        request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();
        // 获得返回值
		Map<byte[], List<Double>> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
                new Batch.Call<OperatorLibraryService, List<Double>>() {
                    @Override
                    public List<Double> call(OperatorLibraryService service) throws IOException {
                        BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                        service.getSum(null, request, rpcCallback);
                        OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                        return response.getResultList() ;
                    }
        });
        // 将返回值进行迭代相加
		double sumResult=0.0;
        for (List<Double> v1 : result.values()) {
        	if(v1.size()==1&&v1.get(0)==Double.MIN_VALUE)
        	{
        		sumResult=Double.MIN_VALUE;break;
        	}
        	else
        	{
        		for(Double v2:v1)
             	{
             		sumResult=sumResult+v2;
             	}
        	}        	
        }
        // 关闭资源
        table.close();
        return sumResult;		
	}
	//Average函数
	public static Double Ave(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn)throws Throwable{
	     // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
        // 设置请求对象
        final OperatorLibraryRequest request;
        request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();       
        // 获得返回值
		Map<byte[], List<Double>> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
                new Batch.Call<OperatorLibraryService, List<Double>>() {
                    @Override
                    public List<Double> call(OperatorLibraryService service) throws IOException {
                        BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                        service.getAve(null, request, rpcCallback);
                        OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                        return response.getResultList();
                    }
        });
        // 将返回值进行迭代相加
		Double sum=0.0;
		Double ave=0.0;
        for (List<Double> v1 : result.values()) {
        	for(Double v2:v1)
            {
             	sum=sum+v2;
            }       		
        }      
        if(sum==Double.MIN_VALUE*result.size())
        {
        	ave=Double.MIN_VALUE;
        }
        else
        {
        	ave=sum/result.size();
        }       
        // 关闭资源
        table.close();
        return ave;		
	}
	//Max函数
	public static Double Max(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn)throws Throwable{
	     // 获取表
        HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
        // 设置请求对象
        final OperatorLibraryRequest request;
        request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();        
        // 获得返回值
		Map<byte[], Double> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
                new Batch.Call<OperatorLibraryService, Double>() {
                    @Override
                    public Double call(OperatorLibraryService service) throws IOException {
                        BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                        service.getMax(null, request, rpcCallback);
                        OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                        return response.getResult(0) ;
                    }
        });
        // 将返回值进行迭代相加
		List<Double> MaxList =new ArrayList<Double>();
        for (Double v : result.values()) {
        		MaxList.add(v);        	     	
        }
        // 关闭资源
        table.close();
        return Collections.max(MaxList);		
	}
	//Min函数
	public static Double Min(String table_name, String runID,String property,String startvalue,String endvalue,Connection conn)throws Throwable{
	     // 获取表
       HTable table = (HTable) conn.getTable(TableName.valueOf(table_name));
       // 设置请求对象
       final OperatorLibraryRequest request;
       request=OperatorLibraryRequest.newBuilder().setTablename(table_name).setRunID(runID).setProperty(property).setStartvalue(startvalue).setEndvalue(endvalue).build();
       // 获得返回值
		Map<byte[], Double> result = table.coprocessorService(OperatorLibraryService.class, null, null, 
               new Batch.Call<OperatorLibraryService, Double>() {
                   @Override
                   public Double call(OperatorLibraryService service) throws IOException {
                       BlockingRpcCallback<OperatorLibraryResponse> rpcCallback = new BlockingRpcCallback<OperatorLibraryResponse>();
                       service.getMin(null, request, rpcCallback);
                       OperatorLibraryResponse response = (OperatorLibraryResponse) rpcCallback.get();
                       return response.getResult(0) ;
                   }
       });
       // 将返回值进行迭代相加
	   List<Double> MinList =new ArrayList<Double>();
       for (Double v : result.values()) {
       	MinList.add(v);
       }
       // 关闭资源
       table.close();
       return Collections.min(MinList);		
	}	
}
