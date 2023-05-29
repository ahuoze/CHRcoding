package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtil {
    public static <T> List<T> queryList(Connection connection,String sql,Class<T> clz,boolean underScoreToCamel) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException {
        //创建结果集合
        ArrayList<T> resultList = new ArrayList<>();

        PreparedStatement preparedStatement = connection.prepareStatement(sql);

        ResultSet resultSet = preparedStatement.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()){
            T t = clz.newInstance();

            //JDBC都是从1开始的
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名，列值
                String columnName = metaData.getColumnName(i);

                //判断是否要转换为驼峰命名
                if(underScoreToCamel){
                    //使用第三方依赖guava实现驼峰命名
                    columnName = CaseFormat.LOWER_UNDERSCORE  //小写下划线形式
                            .to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase()); //小驼峰形式
                }

                Object value = resultSet.getObject(i);

                //使用第三方Bean-utils给泛型对象 T 赋值

                BeanUtils.setProperty(t,columnName,value);
            }
            //将对象添加进集合
            resultList.add(t);
        }
        preparedStatement.close();
        resultSet.close();
        return resultList;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IllegalAccessException, InvocationTargetException, InstantiationException {
        Class.forName(GmallConfig.PHOENIX_DRIVER);

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        List<JSONObject> jsonObjects = queryList(connection,
                "select * from GMALL2021_REALTIME.DIM_USER_INFO",
                JSONObject.class,
                true);
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        connection.close();
    }
}
