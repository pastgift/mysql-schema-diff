# MySQL 数据库结构对比工具

2022-08-13 改为支持 Python 3.x

## 安装依赖包

```sh
pip install -r requirements.txt
```

## 使用方式

```python
python mysql_schema_diff.py <user>:<passwd>@<host>:<port>/<db> <user>:<passwd>@<host>:<port>/<db>
```

## 对比结果示例

### 结构一致时：

```
基准数据库：host=<host>, user=<user>, passwd=***, db=<db>
目标数据库：host=<host>, user=<user>, passwd=***, db=<db>

数据库结构完全一致
```

### 存在差异时：

```
基准数据库：host=<host>, user=<user>, passwd=***, db=<db>
目标数据库：host=<host>, user=<user>, passwd=***, db=<db>

目标数据库相对于基准数据库存在以下差异：

- [缺少表] tb_main_media_library

* [差异表] tb_main_courses
    - [缺少列] allowCopy
    + [多余列] allowcopy

* [差异表] vw_all_aliyun_account_pools
    - [缺少列] isInternational
    * [差异列] internalUserId
        ORDINAL_POSITION ------------- 从基准数据库的`4`被改为目标数据库的`3`

* [差异表] tb_main_enterprise_applications
    * [差异列] id
        COLUMN_DEFAULT --------------- 从基准数据库的`NULL`被改为目标数据库的`<空字符串>`

* [差异表] tb_main_course_categories
    * [差异列] updateTimestamp
        ORDINAL_POSITION ------------- 从基准数据库的`10`被改为目标数据库的`8`
    - [缺少列] imageURL
    * [差异列] createTimestamp
        ORDINAL_POSITION ------------- 从基准数据库的`9`被改为目标数据库的`7`
    - [缺少列] specialization

- [缺少表] tb_main_subscribe_emails

- [缺少表] tb_main_dynamic_navi
```