package SourceCode;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import static junit.framework.Assert.assertEquals;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.BigDecimalColumnInterpreter;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.ColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jasper.tagplugins.jstl.core.Catch;
public class hbase extends mapFrame {
    ArrayList<String> andOrArr = new ArrayList<String>() ;
    FilterList list;
    String keyField="";
    Scan colScan;
    String op1="";
    String op2="";
    /**
     * @param args the command line arguments
     */
    
    void andOrFunction(ArrayList andOrArr,String keyField,String op1,String op2){
    this.andOrArr = andOrArr;
    this.keyField = keyField; 
    this.op1 = op1;
    this.op2 = op2;
    }
    
     void hbaseFunction(String whCond,String columnName,ArrayList selectArr,int caseVal, ArrayList whrArr) throws MasterNotRunningException, IOException, Throwable {
        // TODO code application logic here
         Iterator it = andOrArr.iterator();
                    while(it.hasNext()){
                        System.out.println("hbase anddd or:: "+it.next());
                    }
        System.out.println("Im in HBASE");
        Configuration conf = HBaseConfiguration.create();

        HBaseAdmin admin = new HBaseAdmin(conf);

        HTableDescriptor[] tabDes = admin.listTables();
        HTablePool pool = new HTablePool(conf, 1000);
        //AggregationClient ac=new AggregationClient(conf);
      
     /*  byte b1[][]= new byte[10][];
       for(int i=0;i<4;i++){
       b1[0]="asd".getBytes();
       }
        Iterator itr = selectArr.iterator();
        while(itr.hasNext()){
            
        Bytes.toBytes(itr.next().toString());
        }*/
        
        for (int i = 0; i < tabDes.length; i++) {
            System.out.println("Tables are:" + tabDes[i].getNameAsString());
        }

        HTable table = new HTable(conf, "testdata");

        System.out.println("Select all query");
        Scan scan = new Scan();
        scan.setCaching(20);
//scan.addFamily(Bytes.toBytes("info"));
        scan.addFamily(Bytes.toBytes(columnName));
        
        System.out.println("\n");
        System.out.println("Column::: "+columnName);
        System.out.println("cond::: "+whCond);
       // int checker = whereQueryformatorhbase(whCond);
        System.out.println("Checker::: "+caseVal);
        switch (caseVal) {
            
            case 10:
                System.out.println("Select * from table");
                ResultScanner scanner = table.getScanner(scan);
              
                for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                    for (KeyValue keyValue : result.list()) {
                        System.out.println(Bytes.toString(keyValue.getQualifier())+":"+ Bytes.toString(keyValue.getValue()));
                    }
                }
                break;
                
            case 1021:
                System.out.println("Where name=aditi");
                Filter single = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s = new Scan();
                s.setFilter(single);
                ResultScanner rs = table.getScanner(s);
                for (Result r = rs.next(); (r != null); r = rs.next()) {
                    for (KeyValue kv : r.list()) {
                        System.out.println(Bytes.toString(kv.getQualifier())+":" + Bytes.toString(kv.getValue()));
                    }

                    System.out.println("\n");
                    System.out.println("Select name from info");
                    Filter col = new ColumnPrefixFilter(Bytes.toBytes(whrArr.get(1).toString()));
                    Scan s1 = new Scan();
                    s1.setFilter(col);
                    ResultScanner rs1 = table.getScanner(s1);
                    for (Result r1 = rs1.next(); (r1 != null); r1 = rs1.next()) {
                        for (KeyValue kv1 : r1.list()) {
                            System.out.println(Bytes.toString(kv1.getQualifier())+":" + Bytes.toString(kv1.getValue()));
                        }
                    }
                }
                break;

            case 103:
                System.out.println("\n");
                System.out.println("Where id>1");
                Filter mul = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.GREATER, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s3 = new Scan();
                s3.setFilter(mul);
                ResultScanner rs3 = table.getScanner(s3);
                for (Result r3 = rs3.next(); (r3 != null); r3 = rs3.next()) {
                    for (KeyValue kv3 : r3.list()) {
                        System.out.println(Bytes.toString(kv3.getQualifier())+":" + Bytes.toString(kv3.getValue()));
                    }
                }
                break;

            case 104:
                System.out.println("\n");
                System.out.println("where id<3");
                Filter mul8 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.LESS, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s8 = new Scan();
                s8.setFilter(mul8);
                ResultScanner rs8 = table.getScanner(s8);
                for (Result r8 = rs8.next(); (r8 != null); r8 = rs8.next()) {
                    for (KeyValue kv8 : r8.list()) {
                        System.out.println(Bytes.toString(kv8.getQualifier())+":" + Bytes.toString(kv8.getValue()));
                    }
                }
                break;
                
            case 105:
                System.out.println("\n");
                System.out.println("Where id!=1");
                Filter mul5 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s5 = new Scan();
                s5.setFilter(mul5);
                ResultScanner rs5 = table.getScanner(s5);
                for (Result r3 = rs5.next(); (r3 != null); r3 = rs5.next()) {
                    for (KeyValue kv3 : r3.list()) {
                        System.out.println(Bytes.toString(kv3.getQualifier())+":" + Bytes.toString(kv3.getValue()));
                    }
                }

                break;

            case 106:
                System.out.println("\n");
                System.out.println("where id>=3");
                Filter mul3 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s6 = new Scan();
                s6.setFilter(mul3);
                ResultScanner rs6 = table.getScanner(s6);
                for (Result r6 = rs6.next(); (r6 != null); r6 = rs6.next()) {
                    for (KeyValue kv6 : r6.list()) {
                        System.out.println(Bytes.toString(kv6.getQualifier())+":" + Bytes.toString(kv6.getValue()));
                    }
                }

                break;
            
                case 107:
                System.out.println("\n");
                System.out.println("Where id<=2");
                Filter mul4 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                Scan s7 = new Scan();
                s7.setFilter(mul4);
                ResultScanner rs7 = table.getScanner(s7);
                for (Result r7 = rs7.next(); (r7 != null); r7 = rs7.next()) {
                    for (KeyValue kv7 : r7.list()) {
                        System.out.println(Bytes.toString(kv7.getQualifier())+":" + Bytes.toString(kv7.getValue()));
                    }
                }

                break;

                case 108:
                     System.out.println("select * from tablename where field1 operator value1 or field2 operator value2");
                 FilterList list7=new FilterList(FilterList.Operator.MUST_PASS_ONE);
                 Filter f21=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(0)), andOrOperator(op1), Bytes.toBytes(andOrArr.get(1)));
                 list7.addFilter(f21);
                    System.out.println("108888");
                 Filter f22=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(2)), andOrOperator(op2), Bytes.toBytes(andOrArr.get(3)));
                 list7.addFilter(f22);
                 Scan s14=new Scan();
                 s14.setFilter(list7);
                 ResultScanner rs14=table.getScanner(s14);
                 for(Result r14=rs14.next();(r14!=null);r14=rs14.next()){
                     for(KeyValue kv14:r14.list()){
                         System.out.println(Bytes.toString(kv14.getQualifier())+":"+Bytes.toString(kv14.getValue()));
                     }
                 }
                break;
                
                case 109:
                     System.out.println("select * from tablename where field1 operator value1 and field2 operator value2");
                 FilterList list9=new FilterList(FilterList.Operator.MUST_PASS_ALL);
                 Filter f9=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(0)), andOrOperator(op1), Bytes.toBytes(andOrArr.get(1)));
                 list9.addFilter(f9);
                    System.out.println("108888");
                 Filter f09=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(2)), andOrOperator(op2), Bytes.toBytes(andOrArr.get(3)));
                 list9.addFilter(f09);
                 Scan s09=new Scan();
                 s09.setFilter(list9);
                 ResultScanner rs09=table.getScanner(s09);
                 for(Result r14=rs09.next();(r14!=null);r14=rs09.next()){
                     for(KeyValue kv14:r14.list()){
                         System.out.println(Bytes.toString(kv14.getQualifier())+":"+Bytes.toString(kv14.getValue()));
                     }
                 }
                break;
           
            case 11:
                System.out.println("select column from tablename");
                hbaseSelectMap(selectArr, table);
                colScan.setFilter(list);
                ResultScanner rs1 = table.getScanner(colScan);
                for (Result r1 = rs1.next(); (r1 != null); r1 = rs1.next()) {
                    for (KeyValue kv1 : r1.list()) {
                        System.out.println(Bytes.toString(kv1.getQualifier())+":" + Bytes.toString(kv1.getValue()));
                    }
                }

                break;
                 
                case 1121: 
                 System.out.println("select column from tablename where field=value");
              
                 System.out.println("select column from tablename where field>value");
                 
                 hbaseSelectMap(selectArr, table);
                 SingleColumnValueFilter f121=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                 list.addFilter(f121);
                 
                 colScan.setFilter(list);
                 ResultScanner rs121=table.getScanner(colScan);
                 for(Result r3=rs121.next();(r3!=null);r3=rs121.next()){
                     for(KeyValue kv3:r3.list()){
                            System.out.println(Bytes.toString(kv3.getQualifier())+":"+Bytes.toString(kv3.getValue()));
                     }
                 }
               break;
               
                case 113:
                       System.out.println("select column from tablename where field>value");
                 
                 hbaseSelectMap(selectArr, table);
                 SingleColumnValueFilter f13=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.GREATER, Bytes.toBytes(whrArr.get(1).toString()));
                 list.addFilter(f13);
                 
                 colScan.setFilter(list);
                 ResultScanner rs13=table.getScanner(colScan);
                 for(Result r3=rs13.next();(r3!=null);r3=rs13.next()){
                     for(KeyValue kv3:r3.list()){
                            System.out.println(Bytes.toString(kv3.getQualifier())+":"+Bytes.toString(kv3.getValue()));
                     }
                 }
               break;
               
               case 114:
                 System.out.println("select column from tablename where field<value");
                 hbaseSelectMap(selectArr, table);
                 SingleColumnValueFilter f14=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.LESS, Bytes.toBytes(whrArr.get(1).toString()));
                 list.addFilter(f14);
                 colScan.setFilter(list);
                 ResultScanner rs114=table.getScanner(colScan);
                 for(Result r3=rs114.next();(r3!=null);r3=rs114.next()){
                     for(KeyValue kv3:r3.list()){
                            System.out.println(Bytes.toString(kv3.getQualifier())+":"+Bytes.toString(kv3.getValue()));
                     }
                 }
               break;

               
               case 115:
                 System.out.println("select column from tablename where field!=value");
                 hbaseSelectMap(selectArr, table);
                 SingleColumnValueFilter f15=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                 list.addFilter(f15);
                
                 colScan.setFilter(list);
                 ResultScanner rs15=table.getScanner(colScan);
                 for(Result r3=rs15.next();(r3!=null);r3=rs15.next()){
                     for(KeyValue kv3:r3.list()){
                        System.out.println(Bytes.toString(kv3.getQualifier())+":" + Bytes.toString(kv3.getValue()));
                    }
                }
                break;

            case 116:
                hbaseSelectMap(selectArr, table);
                System.out.println("select column from table 116");
                SingleColumnValueFilter f16 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                list.addFilter(f16);
                colScan.setFilter(list);
                ResultScanner rs16 = table.getScanner(colScan);
                for (Result r1 = rs16.next(); (r1 != null); r1 = rs16.next()) {
                    for (KeyValue kv1 : r1.list()) {
                        System.out.println(Bytes.toString(kv1.getQualifier())+":" + Bytes.toString(kv1.getValue()));
                    }
                }


               break;               
                              
               case 117:
                 System.out.println("select column from tablename where field<=value");
                 hbaseSelectMap(selectArr, table);
                 SingleColumnValueFilter f17=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(whrArr.get(0).toString()), CompareFilter.CompareOp.LESS_OR_EQUAL, Bytes.toBytes(whrArr.get(1).toString()));
                 list.addFilter(f17);
                 Scan s17=new Scan();
                 colScan.setFilter(list);
                 ResultScanner rs17=table.getScanner(colScan);
                 for(Result r3=rs17.next();(r3!=null);r3=rs17.next()){
                     for(KeyValue kv3:r3.list()){
                            System.out.println(Bytes.toString(kv3.getQualifier())+":"+Bytes.toString(kv3.getValue()));
                     }
                 }
               break;
               
               case 118:
                  
                 System.out.println("select column from tablename where field1 operator value1 or field2 operator value2");
                 hbaseSelectMap(selectArr, table);
                 list=new FilterList(FilterList.Operator.MUST_PASS_ONE);
                 Filter f18=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(0)), andOrOperator(op1), Bytes.toBytes(andOrArr.get(1)));
                 list.addFilter(f18);
                    System.out.println("108888");
                 f18 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(2)), andOrOperator(op2), Bytes.toBytes(andOrArr.get(3)));
                 list.addFilter(f18);
                 
                 colScan.setFilter(list);
                 ResultScanner rs18=table.getScanner(colScan);
                 for(Result r14=rs18.next();(r14!=null);r14=rs18.next()){
                     for(KeyValue kv14:r14.list()){
                         System.out.println(Bytes.toString(kv14.getQualifier())+":"+Bytes.toString(kv14.getValue()));
                     }
                 }
                
                   break;
                   
               case 119:
                    
                 System.out.println("select column from tablename where field1 operator value1 and field2 operator value2");
                 hbaseSelectMap(selectArr, table);
                 list=new FilterList(FilterList.Operator.MUST_PASS_ALL);
                 Filter f19=new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(0)), andOrOperator(op1), Bytes.toBytes(andOrArr.get(1)));
                 list.addFilter(f19);
                    System.out.println("108888");
                 f18 = new SingleColumnValueFilter(Bytes.toBytes(columnName), Bytes.toBytes(andOrArr.get(2)), andOrOperator(op2), Bytes.toBytes(andOrArr.get(3)));
                 list.addFilter(f18);
                 
                 colScan.setFilter(list);
                 ResultScanner rs19=table.getScanner(colScan);
                 for(Result r14=rs19.next();(r14!=null);r14=rs19.next()){
                     for(KeyValue kv14:r14.list()){
                         System.out.println(Bytes.toString(kv14.getQualifier())+":"+Bytes.toString(kv14.getValue()));
                     }
                 }
                   break;
                   
              case 121:
                System.out.println("select avg(column) from tablename");
                int count = 0;
                int sum = 0;
                Filter f25 = new ColumnPrefixFilter(Bytes.toBytes(keyField));
                Scan s21 = new Scan();
                s21.setFilter(f25);
                ResultScanner rs21 = table.getScanner(s21);
                for (Result r21 = rs21.next(); (r21 != null); r21 = rs21.next()) {
                    count++;
                    int temp = Integer.parseInt(Bytes.toString(r21.value()));
                    sum += temp;
                }
                int avg = sum / count;
                System.out.println("Avg:" + avg);
                break;
                
               case 122:
                 
                   break;
                   
               case 123:
                    System.out.println("select max(column) from tablename");
                 int maxMarks=0;
                 int temp=0;
                 Filter f23=new ColumnPrefixFilter(Bytes.toBytes(keyField));
                 Scan s23=new Scan();
                 s23.setFilter(f23);
                 ResultScanner rs23=table.getScanner(s23);
                 for(Result r23=rs23.next();(r23!=null);r23=rs23.next()){
                     temp=Integer.parseInt(Bytes.toString(r23.value()));
                     if(temp>maxMarks){
                         maxMarks=temp;
                     }
                 }
                 System.out.println("Max:"+maxMarks);
                 
                   break;
                   
                case 124:
                   break;
                   
                case 125:
                    System.out.println("select min(column) from tablename");
                 int minMarks=100;
                 int temp2=0;
                 Filter f125=new ColumnPrefixFilter(Bytes.toBytes(keyField));
                 Scan s125=new Scan();
                 s125.setFilter(f125);
                 ResultScanner rs125=table.getScanner(s125);
                 for(Result r125=rs125.next();(r125!=null);r125=rs125.next()){
                     temp2=Integer.parseInt(Bytes.toString(r125.value()));
                     if(temp2<minMarks){
                         minMarks=temp2;
                     }
                     
                 }
                 System.out.println("Min:"+minMarks);
                   break;
                   
                case 126:
                    
                   break;
                case 127:
                    System.out.println("select count(column) from tablename");
                 int count2=0;
                 Scan s27=new Scan();
                 s27.addFamily(Bytes.toBytes(columnName));
                 ResultScanner rs27=table.getScanner(s27);
                 for(Result r27=rs27.next();(r27!=null);r27=rs27.next()){
                     count2++;
                 }
                 System.out.println("Count:"+count2);
                   break;
        }

    }
    
    void hbaseSelectMap(ArrayList selectArr,HTable table) throws IOException{
       list = new FilterList();
       colScan = new Scan();
        if (selectArr.size() == 1) {
                        System.out.println("select column from table");
                        long t4 = System.currentTimeMillis();
                        System.out.println("selectarr:: "+selectArr.get(0));
                       
                        Filter f1 = new ColumnPrefixFilter(Bytes.toBytes(selectArr.get(0).toString()));
                        list.addFilter(f1);
                        System.out.println("return if 1");
                    }
                    else if (selectArr.size() == 2) {

                        byte[][] name = new byte[][]{Bytes.toBytes(selectArr.get(0).toString()), Bytes.toBytes(selectArr.get(1).toString())};
                        list = new FilterList();
                        Filter f3 = new MultipleColumnPrefixFilter(name);
                        list.addFilter(f3);
                    }
        return;
    }
    
    CompareOp andOrOperator(String optr){
        CompareOp co = null;
        switch(optr){
            case "=":
            co = CompareFilter.CompareOp.EQUAL;
            break;
            
            case "!=":
            co = CompareFilter.CompareOp.NOT_EQUAL;
            break;
            
            case "<":
                co = CompareFilter.CompareOp.LESS;
                break;
                
            case ">":
                co = CompareFilter.CompareOp.GREATER;
                break;
                
            case "<=":
                co = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
                
            case ">=":
                co = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
        }
    
    return co;
    }
}

