"""
须和 ch.sql 以及 数据及字段解释.txt 置于同一文件夹，产物也在同一文件夹
"""
IMPORT_TEMPLATE = """
package com.nju.allinplantern.flink.pojo.eventbody;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

"""

CLASS_TEMPLATE = """/**
 * {_Class_desc}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class {_Po_class_name} extends EventBody {{
    {_Field_lines}

    @Override
    public boolean isValid() {{
        return false;
    }}
}}
"""

FIELD_LINE_TEMPLATE = """/**
     * {_Field_desc}
     */
    private {_Field_type} {_Field_name};\n
"""

# 当前处理的表
field_list = []  # name,type
po_class_name = ""  # pojo类名，首字母大写
table_name = ""  # 用于数据及字段解释.txt

# 注释映射字典
desc_dict = {}


def main():
    global po_class_name
    init_desc()
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
    with open(po_class_name + ".java", 'w', encoding="UTF-8") as utilClass:
        utilClass.write(IMPORT_TEMPLATE)
        utilClass.write(CLASS_TEMPLATE.format(_Po_class_name=po_class_name,
                                              _Field_lines=generate_field_lines(),
                                              _Class_desc=desc_dict[po_class_name]))


def generate_field_lines():
    global po_class_name
    field_lines = ""
    for field in field_list:
        field_lines += FIELD_LINE_TEMPLATE.format(_Field_type=field[1],
                                                  _Field_name=field[0],
                                                  _Field_desc=desc_dict[po_class_name + field[0]])
    return field_lines


# 驼峰命名
# def generate_camel_case_name(name):
#     capitalize_name_list = map(lambda x: x.capitalize(), str(name).split('_'))
#     camel_case_name = ''.join(capitalize_name_list)
#     camel_case_name[:1].lower()
#     return camel_case_name


def clear():
    global po_class_name
    field_list.clear()
    po_class_name = ""


def field_type_converter(origin_field_type):
    if str(origin_field_type).find("String") != -1:
        return "String"
    elif str(origin_field_type).find("int") != -1:
        return "Integer"
    elif str(origin_field_type).find("decimal") != -1:
        return "BigDecimal"


# 从txt文件获取注释
def init_desc():
    global table_name
    with open("数据及字段解释.txt", 'r', encoding="UTF-8") as f:
        line = f.readline().strip()
        is_mx = False

        while line:
            line = line.strip()
            # 头部信息
            if line.find("dm_v_tr_") != -1:
                is_mx = True
                table_name = line[8: line.find('mx') - 1].capitalize()
                table_desc = line[line.find('\'') + 1:line.rfind('\'')]
                desc_dict[table_name] = table_desc
                line = f.readline()
                continue
            # 尾部信息
            if line.find(")") != -1 and is_mx:
                is_mx = False
                line = f.readline()
                continue
            # 成员变量信息
            if is_mx:
                field_and_desc = line.split("--")
                field_name = field_and_desc[0].strip().split(" ")[0]
                field_desc = field_and_desc[1].strip()
                desc_dict[table_name + field_name] = field_desc
            line = f.readline()


if __name__ == '__main__':
    main()
