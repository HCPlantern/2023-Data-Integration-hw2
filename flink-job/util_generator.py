"""
须和 ch.sql 置于同一文件夹，产物也在同一文件夹
"""
IMPORT_TEMPLATE = """
package com.nju.allinplantern.flink.utils;
import com.nju.allinplantern.flink.pojo.eventbody.{_Po_class_name};
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.settings.ClickHouseProperties;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

"""

CLASS_TEMPLATE = """
public class {_Po_class_name}CkUtil extends RichSinkFunction<{_Po_class_name}> {{
    // ck 连接
    private ClickHouseConnection connection;

    private PreparedStatement preparedStatement;

    // 对应的 sql
    private static final String sql = \"{_sql}\";

    @Override
    public void open(Configuration parameters) throws Exception {{
        super.open(parameters);
    }}

    @Override
    public void close() throws Exception {{
        super.close();
        if (connection != null) {{
            connection.close();
        }}
    }}

    @Override
    public void invoke({_Po_class_name} value, Context context) throws Exception {{
        // 具体的sink处理
        String url = \"jdbc:clickhouse://172.17.188.153:8123\";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setUser(\"default\");
        properties.setPassword(\"16d808ef\");
        properties.setSessionId(\"default-session-id\");

        ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
        Map<ClickHouseQueryParam, String> additionalDBParams = new HashMap<>();
        additionalDBParams.put(ClickHouseQueryParam.SESSION_ID, \"new-session-id\");
        try {{
            if (connection == null) {{
                connection = dataSource.getConnection();
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
            }} else {{
                System.out.println(\"无需重新建立连接\");
            }}
            {_setters}
            preparedStatement.execute();
        }} catch (Exception e) {{
            e.printStackTrace();
        }}
    }}
}}
"""

SETTER_TEMPLATE = "preparedStatement.set{_Field_type}({_index}, value.get{_Field_name}());\n"
SQL_TEMPLATE = "INSERT INTO dm_v_tr_{_po_class_name}_mx({_fields}) VALUES ({_values})"

# 当前处理的表
field_list = []  # name,type
po_class_name = ""


def main():
    global po_class_name
    with open("ch.sql", 'r', encoding="UTF-8") as f:
        line = f.readline().strip()
        is_mx = False
        while line:
            line = line.strip()
            # 头部信息
            if line.find("CREATE") != -1 and line.find('mx') != -1:
                print(line)
                is_mx = True
                po_class_name = line[line.find('tr_') + 3: line.find('_mx')].capitalize()
                line = f.readline()
                continue
            # 尾部信息
            if line.startswith(')') and is_mx:
                print(line)
                is_mx = False
                create_file()
                clear()
                line = f.readline()
                continue
            # 属性信息
            if is_mx:
                print(line)
                field = line.split(' ')
                field[1] = field_type_converter(field[1])
                field_list.append(field)
            line = f.readline()


def create_file():
    global po_class_name
    with open(po_class_name + "CkUtil.java", 'w', encoding="UTF-8") as utilClass:
        utilClass.write(IMPORT_TEMPLATE.format(_Po_class_name=po_class_name))
        utilClass.write(CLASS_TEMPLATE.format(_Po_class_name=po_class_name,
                                              _sql=generate_sql(),
                                              _setters=generate_setters()))


def generate_sql():
    global po_class_name
    values = ['?' for i in range(len(field_list))]
    return SQL_TEMPLATE.format(_po_class_name=po_class_name.lower(),
                               _fields=','.join([field[0] for field in field_list]),
                               _values=','.join(values))


def generate_setters():
    setters = ""
    i = 1
    for field in field_list:
        setters += SETTER_TEMPLATE.format(_Field_type=field[1],
                                          _index=i,
                                          _Field_name=field[0].capitalize())
        i += 1
    return setters


def clear():
    global po_class_name
    field_list.clear()
    po_class_name = ""


def field_type_converter(origin_field_type):
    if str(origin_field_type).find("String") != -1:
        return "String"
    elif str(origin_field_type).find("int") != -1:
        return "Int"
    elif str(origin_field_type).find("decimal") != -1:
        return "BigDecimal"


if __name__ == '__main__':
    main()
