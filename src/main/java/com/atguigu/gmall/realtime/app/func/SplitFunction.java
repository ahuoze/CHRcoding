package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str){
        try {
            List<String> strings = KeywordUtil.splitKeyWord(str);
            for (String string : strings) {
                collect(Row.of(string));
            }
        } catch (IOException e) {
            collect(Row.of(str));
        }
    }
}
