# -*- coding: utf-8 -*-
def create_hive_module_input_table(hive_connection, table, hdfs_file, columns, id_task=None, sep="\t"):
    """
    create a hive table and store the information in a hdfs file
    :param hive_connection: The connection to hive
    :param table_name: The name of the table to create
    :param hdfs_file: The path hdfs to store the file
    :param columns: A list of tupes (field:type) of the table columns
    :param id_task: The id of the task to generate unique tables
    :param sep: The field delimiter in the new file (default=\t)
    :return: The name of the hive table created
    """
    definition = []

    # Name with the uuid of the task
    if id_task:
        table = table + '_' + id_task
    sentence = "CREATE EXTERNAL TABLE IF NOT EXISTS {table} ({columns}) \
                ROW FORMAT DELIMITED \
                FIELDS TERMINATED BY '{sep}' \
                STORED AS TEXTFILE LOCATION '{hdfs_file}'"
    sentence.format(table=table,
                    columns=",".join(["{} {}".format(c[0], c[1]) for c in columns]),
                    sep=sep,
                    hdfs_file=hdfs_file)
    try:
        hive_connection.execute(sentence)
    except Exception as e:
        raise Exception('Failed to create HIVE table {}: {}'.format(table, e))
    return table


def delete_hive_table(hive_connection, table):
    """
    Deletes a hive table
    :param hive_connection: The connection to hive
    :param table: The name of the table to delete
    :return: True if the table has been deleted
    """
    sentence = "DROP TABLE {}".format(table)
    try:
        hive_connection.execute(sentence)
    except Exception as e:
        raise Exception('Failed to delete HIVE table {}: {}'.format(table, e))
    return True


def create_hive_table_from_hbase_table(hive_connection, table_hive, table_hbase, hive_key, columns, id_task=None):
    """
    Creates a hive table linked to an hbase table
    :param hive_connection: The connection to hive
    :param table_hive: the hive table to be created
    :param table_hbase: the hbase table with the data
    :param hive_key: a dictionary {key:type} with the key and type of hive keys
    :param hive_columns: a list of tuples (key, type, column) with the key and type of hive columns and the column of hbase table
    :param id_task: The id of the task to generate unique tables
    :return: the name of the table
    """
    if id_task:
        table_hive = table_hive + '_' + id_task
    sentence = "CREATE EXTERNAL TABLE IF NOT EXISTS \
         {table_hive}( key struct<{hive_key}>, {hive_columns} ) \
         ROW FORMAT DELIMITED \
         COLLECTION ITEMS TERMINATED BY '~' \
         STORED BY \
         'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \
          WITH SERDEPROPERTIES \
          ('hbase.columns.mapping' = ':key, {hbase_columns') \
          TBLPROPERTIES \
          ('hbase.table.name' = '{table_hbase}')"
    sentence.format(table_hive=table_hive,
                    hive_key=",".join(["{}:{}".format(k,t) for k,t in hive_key.items()]),
                    hive_columns=",".join(["{} {}".format(c[0],c[1]) for c in columns]),
                    hbase_columns=",".join([c[2] for c in columns]),
                    table_hbase=table_hbase)

    try:
        hive_connection.execute(sentence)
    except Exception as e:
        raise Exception('Failed to create HIVE temporary table {}: {}'.format(table_hive, e))
    return table_hive