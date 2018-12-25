from trod.utils import Dict


SQL = Dict(
    create="CREATE TABLE `{tn}` ({cd}) ENGINE={eg} DEFAULT CHARSET={cs} COMMENT='{cm}';",
    drop="DROP TABLE `{table_name}`;",
    show=Dict(
        tables='SHOW TABLES',
        status='SHOW TABLE STATUS',
        create="SHOW CREATE TABLE `{table_name}`;",
        columns="SHOW FULL COLUMNS FROM `{table_name}`;",
        indexs="SHOW INDEX FROM `{table_name}`;"
    ),
    exist="SELECT table_name FROM information_schema.tables WHERE table_name ='{table_name}';",
    alter="ALTER TABLE `{table_name}` {clause};",
    insert="INSERT INTO {table_name} ({cols}) VALUES ({values});",
    delete="DELETE FROM `{table_name}` WHERE {condition};",
    update_=Dict(
        complete="UPDATE `{table_name}` SET {kv} WHERE {condition};",
        no_where="UPDATE `{table_name}` SET {kv}"
    ),
    select=Dict(
        complete="SELECT {cols} FROM `{table_name}` {where_clause} {group_by_clause} {order_clause} {limit_clause}",
        by_id="SELECT {cols} FROM `{table_name}` WHERE {condition}=%;",
        by_ids="SELECT {cols} FROM `{table_name}` WHERE {condition} IN %;",
    )
)
