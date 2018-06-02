package org.osv.eventdb;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Scanner;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Admin; 
import org.apache.hadoop.hbase.Coprocessor;




import com.google.protobuf.ServiceException;

public class Main {
	
	public static void main(String[] args) throws ServiceException, Throwable {
		// 配置HBase
		Configuration conf = HBaseConfiguration.create();
	    conf.set("hbase.zookeeper.quorum", "127.0.0.1");
	    conf.set("hbase.zookeeper.property.clientPort", "2181");
	    conf.set("hbase.master", "192.168.0.178:61000");
	    String path = "hdfs://192.168.0.178:9000/user/eventdb-1.0.0.jar";
		// 建立一个数据库的连接
		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		// 连接请求表（判断输入是否正确）
		Scanner sc = new Scanner(System.in);
		System.out.println("请输入表名：");
		String table_name = null;
		boolean table_bool = true;
		while (table_bool) {
			table_name = sc.nextLine();
			
			if(table_name.contains(" ")||table_name.matches("\\s+"))
				System.out.println("没有此表，请重新输入表名：");		
			else if (!admin.tableExists(TableName.valueOf(table_name)))
				System.out.println("没有此表，请重新输入表名：");		
			else
				table_bool = false;
		}
/*		Scanner sc = new Scanner(System.in);
		String table_name = null;
		table_name = "endpointTest";*/
		// 加载协处理器
		System.out.println("正在加载协处理器……");
		HTableDescriptor tableDesc = admin.getTableDescriptor(TableName
				.valueOf(table_name));
		System.out.println("tableDesc :" + tableDesc);
		if (!tableDesc
				.hasCoprocessor(OperatorLibraryEndPoint.class.getCanonicalName())) {
			System.out.println("有coprocessor");
			admin.disableTable(TableName.valueOf(table_name));
			System.out.println("has disabled table " + table_name);
			tableDesc.addCoprocessor(OperatorLibraryEndPoint.class.getCanonicalName(),
								new Path(path), Coprocessor.PRIORITY_USER, null);
			System.out.println("add coprocessor ok");
			admin.modifyTable(TableName.valueOf(table_name), tableDesc);
			admin.enableTable(TableName.valueOf(table_name));
			System.out.println("enable table ok");
		}
		// 输入runID
		System.out.println("请输入runID");
		String	runID = sc.nextLine();
		//输入property
		System.out.println("请输入property");
		String	property = sc.nextLine();
		// 输入startvalue
		System.out.println("请输入起始范围");
		String	startvalue = sc.nextLine();
		// 输入endvalue
		System.out.println("请输入终止范围");
		String	endvalue = sc.nextLine();
/*		String runID = "47543";
		String	property = "NTracks";
		String	startvalue = "1";
		String	endvalue = "2";*/
		
		// 选择算子（判断输入算子是否正确：）
		Class<OperatorLibraryClient> cl = OperatorLibraryClient.class;
		Method[] mes = cl.getMethods();
		TreeMap<String, Method> methods = new TreeMap<String, Method>();
		for (int i = 0; i < mes.length - 9; i++) {
			methods.put(mes[i].getName(), mes[i]);
			methods.put(Integer.toString(i + 1), mes[i]);
			System.out.println(i + 1 + ":" + mes[i].getName());
		}
		System.out.println("请输入算子编号或名称：");
		String Operator_name = null;
		boolean Operator_bool = true;
		while (Operator_bool) {
			Operator_name = sc.next();
			if (methods.containsKey(Operator_name))
				Operator_bool = false;
			else
				System.out.println("没有此算子，请重新输入");
		}
		Object ar[] = { table_name, runID, property, startvalue,endvalue, conn };
		Object result_object = methods.get(Operator_name).invoke(
				OperatorLibraryClient.class.newInstance(), ar);
		System.out.println("before the if");
		if(methods.get(Operator_name).getReturnType()==List.class)
		{
			List<Double> result = (List<Double>) result_object;
			System.out.println("The result is "+result);
			
			if(result.contains(Double.MIN_VALUE))
			{
				System.out.println("输入有误(Startvalue or endvalue is error)");
			}
			else
			{
				System.out.println("结果为：");
				for (int i = 0; i < result.size(); i++) {
					System.out.println(result.get(i));
				}
			}		
		}else if(methods.get(Operator_name).getReturnType()==Double.class)
		{
			Double result = (Double) result_object;
			if(result==Double.MIN_VALUE)
			{
				System.out.println("输入有误(Startvalue or endvalue is error)");
			}
			else
			{
				System.out.println("结果为："+result);
			}
			
		}
		// 卸载协处理器
		System.out.println("正在卸载协处理器……");
		admin.disableTable(TableName.valueOf(table_name));
		tableDesc
				.removeCoprocessor(OperatorLibraryEndPoint.class.getCanonicalName());
		admin.modifyTable(TableName.valueOf(table_name), tableDesc);
		admin.enableTable(TableName.valueOf(table_name));
		System.out.println("卸载完成");
	}
}
