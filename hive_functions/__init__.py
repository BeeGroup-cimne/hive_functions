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
    :return: The name of the hive table
    """
    definition = []

    # Name with the uuid of the task
    if id_task:
        table = table + '_' + id_task
    sentence = "CREATE EXTERNAL TABLE IF NOT EXISTS {table} ({columns}) \
                ROW FORMAT DELIMITED \
                FIELDS TERMINATED BY '{sep}' \
                STORED AS TEXTFILE LOCATION '{hdfs_file}'"
    sentence = sentence.format(table=table,
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


def create_hive_table_from_hbase_table(hive_connection, table_hive, table_hbase, key, columns, id_task=None):
    """
    Creates a hive table linked to an hbase table
    :param hive_connection: The connection to hive
    :param table_hive: the hive table to be created
    :param table_hbase: the hbase table with the data
    :param key: a list of tuples (hbase_key_name, type) of the hbase key
    :param columns: a list of tuples (hive_column_name, type, "<hbase_column_family>:<hbase_column_name>") of the hbase columns
    :param id_task: The id of the task to generate unique tables
    :return: the name of the hive table
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
              ('hbase.columns.mapping' = ':key, {hbase_columns}') \
              TBLPROPERTIES \
              ('hbase.table.name' = '{table_hbase}')"
    sentence = sentence.format(table_hive=table_hive,
                    hive_key=",".join(["{}:{}".format(k[0],k[1]) for k in key]),
                    hive_columns=",".join(["{} {}".format(c[0],c[1]) for c in columns]),
                    hbase_columns=",".join([c[2] for c in columns]),
                    table_hbase=table_hbase)

    try:
        hive_connection.execute(sentence)
    except Exception as e:
        raise Exception('Failed to create HIVE table {}: {}'.format(table_hive, e))
    return table_hive


def create_hive_partitioned_table(hive_connection, table, columns, partitioner_columns, hdfs_file,
                                  drop_old_table, id_task=None, sep='\t'):
    """
        Creates a hive table linked to an hbase table
        :param hive_connection: The connection to hive
        :param table_hive: the hive partitioned table to be created
        :param columns: a list of tuples (hive_column_name, type)
        :param partitioner_columns: a list of tuples (hive_column_name, type) of the columns acting as the partitioner of the table.
        :param hdfs_file: The path hdfs to store the file
        :param drop_old_table: Drop the old table before the generation of the new one in order to delete deprecated records.
        :param id_task: The id of the task to generate unique tables
        :return: the name of the hive table
    """
    if id_task:
        table = table + '_' + id_task

    # HIVE sentence definition
    sentence = "CREATE TABLE IF NOT EXISTS {table}({hive_columns})\
                PARTITIONED BY ({hive_partitioner})\
                ROW FORMAT DELIMITED\
                FIELDS TERMINATED BY '{sep}'\
                STORED AS TEXTFILE LOCATION '{location}'"
    sentence = sentence.format(table= table,
                               hive_columns= ",".join(["{} {}".format(c[0],c[1]) for c in columns]),
                               hive_partitioner= ",".join(["{} {}".format(c[0], c[1]) for c in partitioner_columns]),
                               sep = sep,
                               location = hdfs_file)
    # If table_needs_to_be_recreated==True, delete the old partitioned table. This only happens when the maximum number of periods of the tertiary measures is bigger than the latest number of periods
    if drop_old_table is True:
        drop_table = 'DROP TABLE %s' % table
        hive_connection.execute(drop_table)

    try:
        hive_connection.execute(sentence)
    except Exception as e:
        raise Exception('Failed to create HIVE partitioned table {}: {}'.format(table, e))
    return table
