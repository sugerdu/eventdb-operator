package org.osv.eventdb;

import java.io.IOException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.special.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryRequest;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryResponse;
import org.osv.eventdb.OperatorLibrary.OperatorLibraryService;

import net.blackruffy.root.*;
import static net.blackruffy.root.JRoot.*;
import net.sf.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperatorLibraryEndPoint extends OperatorLibraryService implements
Coprocessor, CoprocessorService {
	private RegionCoprocessorEnvironment env; // 定义环境
	public static final Logger Log = LoggerFactory.getLogger(OperatorLibraryEndPoint.class);
	public Service getService() {
		return (Service) this;
	}

	// 协处理器初始化时调用的方法
	public void start(CoprocessorEnvironment env) throws IOException {
		if (env instanceof RegionCoprocessorEnvironment) {
			this.env = (RegionCoprocessorEnvironment) env;
		} else {
			throw new CoprocessorException("no load region");
		}
	}

	// 协处理器结束时调用的方法
	public void stop(CoprocessorEnvironment env) throws IOException {

	}
	public ArrayList<Double> getdata(RpcController controller,OperatorLibraryRequest request) {
		Log.info("getdata has called");
		String tableName = request.getTablename();
		String runID = request.getRunID();
		String property = request.getProperty();
		String startvalue = request.getStartvalue();
		String endvalue = request.getEndvalue();
		ArrayList<Double> datalist= new ArrayList<>();
		//code(value)
		if(startvalue.contains("."))
			startvalue = getDoubleS(Double.parseDouble(startvalue));
		else
			startvalue = getIntS(Integer.parseInt(startvalue));
		if(endvalue.contains("."))
			endvalue = getDoubleS(Double.parseDouble(endvalue));
		else
			endvalue = getIntS(Integer.parseInt(endvalue));
		
		//生成rowkey
		String startrowkey = runID+"#"+property+"#"+startvalue;
		String endrowkey = runID+"#"+property+"#"+endvalue;
		//输出rowkey
		System.out.println("The rowkey is "+startrowkey+" and "+endrowkey);
		
		//设置扫描对象
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startrowkey));
		scan.setStopRow(Bytes.toBytes(endrowkey));
		scan.addFamily(Bytes.toBytes("data"));
		//扫描出索引表路径
		InternalScanner rs=null;//定义变量
		ArrayList<String[]> indexrs = new ArrayList<String[]>();
		try {  
			List<Cell> results = new ArrayList<Cell>();
			boolean hasMore = false;
			rs = env.getRegion().getScanner(scan); 			
			do {
                hasMore = rs.next(results);
                int temp = 0;
                String[] str = new String[4]; 
                for (Cell cell : results) {   	
                    str[temp] = new String(CellUtil.cloneValue(cell));
                    temp++;
                }
				//输出value
				System.out.println(str[0]+" "+str[1]+" "+str[2]+" "+str[3]);
                indexrs.add(str);
                results.clear();
            } while (hasMore);
			} catch (IOException e) {  
				ResponseConverter.setControllerException(controller, (IOException) e);
		}finally {
            if (rs != null) {
                try {
                	rs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
//		String[] content = new String[indexrs.size()];
		ArrayList<String> content=new ArrayList<>();
		//确定路径，调用readFile()方法返回索引
		try{
			for(int i=0;i<indexrs.size();i++) {
				//String path = "hdfs://sbd01:8020/eventdb/"+tableName.split(":")[1]+"/data/"+indexrs.get(i)[3]+".data";
				String path = "hdfs://localhost:9000/eventdb/"+tableName+"/data/"+indexrs.get(i)[3]+".data";
				String offset = indexrs.get(i)[2];
				String lengh = indexrs.get(i)[1];
				if(!new String(readFile(path,offset,lengh)).equals("no"))
				{
					content.add(new String(readFile(path,offset,lengh)));
				}			
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
//		byte[] rootIndex = content.getBytes();
		//进行JSON解析，读取ROOT文件，获取数据
		for(int i=0;i<content.size();i++)
		{
			try{
				JSONObject json = JSONObject.fromObject(content.get(i));
				Iterator json_iterator = json.keys();
				while(json_iterator.hasNext()) {
					String rootName = (String)json_iterator.next();
					Object[] root_offset = json.getJSONArray(rootName).toArray();
					final TFile file = newTFile(rootName+".root");
					final TTree tree= TTree (file.get("ntTAG"));
					for(int j=0;j<root_offset.length;j++) {
						tree.getEntry((long) root_offset[j]);
						double data=tree.getLeaf(property).getValue();
						//输出data
						System.out.print(data+" ");
						datalist.add(data);
					}
				}
			}catch (Exception e) {
				datalist.add(Double.MIN_VALUE);
			}
		}
		return datalist;
	}
	
	 /** 
     * 读取hdfs文件内容 
     * 
     * @param filePath 
	 * @return 
     * @throws IOException 
	 * @throws InterruptedException 
     */  
    public byte[] readFile(String indexfilePath,String indexoffset,String indexlengh) throws IOException, InterruptedException {  
        Configuration conf = new Configuration(); 
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        Path srcPath = new Path(indexfilePath);  
        FileSystem fs = null;  
        URI uri;  
        byte[] b = new byte[Integer.parseInt(indexlengh)];
        InputStream in = null;
        try {  
            uri = new URI(indexfilePath);  
            //fs = FileSystem.get(uri, conf,"root");
            fs = FileSystem.get(uri, conf,"shane"); 
            if (!fs.exists(srcPath))
            {
            	b=Bytes.toBytes("no");
            }
            else
            {           	 
            	 in = fs.open(srcPath);
                 in.skip(Long.parseLong(indexoffset));
                 in.read(b);
                 in.close();
            }
        } catch (URISyntaxException e) {  
            e.printStackTrace();  
        } finally {  
            IOUtils.closeStream(in);  
        }
		//输出index
		System.out.println(b);
        return b;
    }
	
	//code(value)转换
	public String getDoubleS(double d) {
		String s;
		long b;
		b = Double.doubleToLongBits(d);
		b = (b^(b>>63 | 0x8000000000000000L)) + 1;
		s = (Long.toHexString(b));
		return s;
	}
	public String getIntS(int d) {
		d = d ^ 0x80000000;
		String s = (Integer.toHexString(d));
		return s;
	}

	@Override
	public void getGamma(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
		System.out.println("let us get data");
		ArrayList<Double> datalist=getdata(controller,request);
		System.out.println("The data is "+datalist);
		OperatorLibraryResponse response = null;
		List<Double> GammaResults = new ArrayList<>();
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			GammaResults.add(Double.MIN_VALUE);
		}
		else
		{
			for(int i=0;i<datalist.size();i++) {
				GammaResults.add(Gamma.gamma(datalist.get(i)));
			}
		}
		try{
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addAllResult(GammaResults);
		response = responseBuilder.build();
		}catch (Exception e) {
			ResponseConverter.setControllerException(controller, (IOException) e);
		}
				
		//返回给客户端
		done.run(response);
	}

	@Override
	public void getErf(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
		ArrayList<Double> datalist=getdata(controller,request);
		/*ArrayList<Double> datalist = new ArrayList<Double>();
		datalist.add(0.01);
		datalist.add(0.02);
		datalist.add(0.03);*/
		OperatorLibraryResponse response = null;
		List<Double> ErfResults = new ArrayList<>();
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			ErfResults.add(Double.MIN_VALUE);
		}
		else
		{
			for(int i=0;i<datalist.size();i++) {
				ErfResults.add(Erf.erf(datalist.get(i)));
			}
		}
		try{
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addAllResult(ErfResults);
		response = responseBuilder.build();
		}catch(Exception e){
			ResponseConverter.setControllerException(controller, (IOException) e);
	}		
		//返回给客户端
		done.run(response);
	}

	@Override
	public void getSum(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
		ArrayList<Double> datalist=getdata(controller,request);
		OperatorLibraryResponse response = null;
		double sum = 0.0;
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			sum=Double.MIN_VALUE;
		}
		else
		{
			for(int i=0;i<datalist.size();i++) {
				sum=sum+datalist.get(i);
			}
		}
		try{
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addResult(sum);		
		response = responseBuilder.build();
		}catch (Exception e) {
			ResponseConverter.setControllerException(controller, (IOException) e);
		}		
		//返回给客户端
		done.run(response);
	}

	@Override
	public void getAve(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
		ArrayList<Double> datalist=getdata(controller,request);
		OperatorLibraryResponse response = null;
		double sum = 0.0;
		double ave = 0.0;
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			ave=Double.MIN_VALUE;
		}
		else
		{
			for(int i=0;i<datalist.size();i++) {
				sum=sum+datalist.get(i);
			}
		}
		try{
		ave=sum/datalist.size();
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addResult(ave);
		response = responseBuilder.build();
		}catch (Exception e) {
			ResponseConverter.setControllerException(controller, (IOException) e);
		}
		//返回给客户端
		done.run(response);
	}

	@Override
	public void getMax(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
		ArrayList<Double> datalist=getdata(controller,request);
		OperatorLibraryResponse response = null;
		double max=0.0;
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			max=Double.MIN_VALUE;
		}
		else
		{
			max=Collections.max(datalist);
		}		
		try{
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addResult(max);		
		response = responseBuilder.build();
		}catch (Exception e) {
			ResponseConverter.setControllerException(controller, (IOException) e);
		}
		//返回给客户端
		done.run(response);
	}

	@Override
	public void getMin(RpcController controller, OperatorLibraryRequest request,
			RpcCallback<OperatorLibraryResponse> done) {
		// TODO Auto-generated method stub
//		ArrayList<Double> datalist=getdata(controller,request);
		ArrayList<Double> datalist = new ArrayList<Double>();
		datalist.add(0.01);
		datalist.add(0.02);
		datalist.add(0.03);
		Log.info("getdata ret is: " + datalist);
		OperatorLibraryResponse response = null;
		double min=0.0;
		if(datalist.contains(Double.MIN_VALUE)||datalist.isEmpty())
		{
			min=Double.MIN_VALUE;
		}
		else
		{
			min=Collections.min(datalist);
		}
		try{
		OperatorLibraryResponse.Builder responseBuilder = OperatorLibraryResponse.newBuilder();
		responseBuilder.addResult(min);		
		response = responseBuilder.build();
		}catch (Exception e) {
			Log.info("your operator has exception." + e);
			ResponseConverter.setControllerException(controller, (IOException) e);
		}
		//返回给客户端
		done.run(response);
	}
}
