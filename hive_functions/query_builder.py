# -*- coding: utf-8 -*-
class BaseQueryBuilder:
    """
    Base class to generate Hive Queries
    """
    def __init__(self, hive_client):
        self.client = hive_client

    def exists_table(self, table):
        """
        Checks if the table exists in hive
        :param table: The table name to check
        :return: True if the table exists, else False
        """
        self.client.execute("SHOW TABLES LIKE '{}'".format(table))
        for i in self.client.fetch():
            return True
        return False


class RawQueryBuilder(BaseQueryBuilder):
    """
    Helper to generate Hive Queries manually
    """

    def execute_query(self, query):
        """
        Executes the "query" in hive
        :param query: The hive query to execute
        :return: None
        """
        try:
            self.client.execute(query)
        except Exception as e:
            raise Exception("HIVE query failed: {} ".format(e))


class QueryBuilder(BaseQueryBuilder):
    """
    Helper to generate Hive Queries
    """
    def __init__(self, hive_client):
        super(QueryBuilder, self).__init__(hive_client)
        self.table = None
        self.join = []
        self.insert = None
        self.select = None
        self.group = None
        self.order = None
        self.where = None
        self.union = None
        self.dynamic = None
        self.memory_extra = None
        self.reducers = None

    def add_memory_extra(self):
        """
        sets the hive.map.aggr to false
        :return: self
        """
        self.memory_extra = 'SET hive.map.aggr=false'
        return self

    def add_reducers(self, n):
        """
        sets the number of reducers
        :param n: the number of reducers
        :return: self
        """
        self.reducers = 'SET mapred.reduce.tasks={}'.format(str(n))
        return self

    def add_dynamic(self):
        """
        Sets the hive.exec.dynamic.partition.mode to nonstrict
        :return: self
        """
        self.dynamic = 'SET hive.exec.dynamic.partition.mode=nonstrict;'
        return self

    def add_from(self, table, alias=None):
        """
        set a table in the form of the query
        :param table: The table to add
        :param alias: The name to call the table with "as"
        :return: self
        """
        if not alias:
            self.table = 'FROM {} '.format(table)
        else:
            self.table = 'FROM {} {} '.format(table, alias)
        return self

    def add_union_all(self, query):
        """
        joins the query with an union all
        :param query: the query to join
        :return: self
        """
        self.union = 'UNION ALL {} '.format(query)

        return self

    def add_join(self, table, alias, condition=None):
        """
        adds a join to the table
        :param table: the table
        :param alias: the name of the table
        :param condition: the condition of the join
        :return: self
        """
        if condition:
            self.join.append('JOIN {} {} ON ({}) '.format(table, alias, condition))
        else:
            self.join.append('JOIN {} {} '.format(table, alias))

        return self

    def add_full_outer_join(self, table, alias, condition=None):
        """
        Add a full outer join
        :param table: The table
        :param alias: the name of the table
        :param condition: the condition
        :return: self
        """
        if condition:
            self.join.append('FULL OUTER JOIN {} {} ON ({}) '.format(table, alias, condition))
        else:
            self.join.append('FULL OUTER JOIN {} {}'.format(table, alias))

        return self

    def add_left_outer_join(self, table, alias, condition=None):
        """
        add a left outer join
        :param table:
        :param alias:
        :param condition:
        :return: self
        """
        if condition:
            self.join.append('LEFT OUTER JOIN {} {} ON ({}) '.format(table, alias, condition))
        else:
            self.join.append('LEFT OUTER JOIN {} {} '.format(table, alias))
        return self

    def add_right_outer_join(self, table, alias, condition=None):
        """
        Adds a rigth outer join
        :param table:
        :param alias:
        :param condition:
        :return: self
        """
        if condition:
            self.join.append('RIGHT OUTER JOIN {} {} ON ({}) '.format(table, alias, condition))
        else:
            self.join.append('RIGHT OUTER JOIN {} {} '.format(table, alias))
        return self

    def add_insert(self, table=None, overwrite=True, directory=None, partition=None):
        """
        Adds an insert
        :param table:
        :param overwrite:
        :param directory:
        :param partition:
        :return: self
        """
        if not directory and table:
            if partition:
                self.insert = 'INSERT {} TABLE {} PARTITION ({})'.format('' if not overwrite else 'OVERWRITE',
                                                                         table, partition)
            else:
                self.insert = 'INSERT {} TABLE {}'.format('' if not overwrite else 'OVERWRITE', table)
        elif directory:
            self.insert = 'INSERT {} DIRECTORY \'{}\''.format('' if not overwrite else 'OVERWRITE', directory)
        else:
            raise Exception('Error adding INSERT sentence into HIVE query. Neither table or directory specified')
        return self

    def add_select(self, select):
        """
        adds a select
        :param select:
        :return: self
        """
        self.select = 'SELECT {} '.format(select)

        return self

    def add_groups(self, groups):
        """
        Adds a group
        :param groups:
        :return: self
        """
        self.group = 'GROUP BY {} '.format(','.join(groups))

        return self

    def add_order(self, name, order=None):
        """
        adds an order
        :param name:
        :param order:
        :return: self
        """
        self.order = 'ORDER BY {} {} '.format(name, 'ASC' if not order else order)

        return self

    def add_sort(self, content):
        """
        add a sort
        :param content:
        :return: self
        """
        self.sort = 'SORT BY {} '.format(content)

        return self

    def add_where(self, condition):
        """
        adds a where
        :param condition:
        :return: self
        """
        self.where = 'WHERE {} '.format(condition)

        return self

    def add_and_where(self, condition):
        """
        adds and "AND" in where
        :param condition:
        :return: self
        """
        self.where += 'AND {} '.format(condition)

        return self

    def create_query(self):
        """
        creates the query to execute
        :return: the query
        """
        if not self.table or not self.insert or not self.select:
            raise Exception('Creating query with some missing parts')

            # Add from
        query_parts = [self.table]

        # Add join sentence if needed
        if self.join:
            query_parts.append(' '.join(self.join))

        # Add insert and select sentences (mandatory)
        query_parts.append(self.insert)
        query_parts.append(self.select)

        if self.where:
            query_parts.append(self.where)

        if self.group:
            query_parts.append(self.group)

        if self.order:
            query_parts.append(self.order)

        if self.union:
            query_parts.append(self.union)

        if self.dynamic:
            self.client.execute(self.dynamic)

        if self.memory_extra:
            self.client.execute(self.memory_extra)

        if self.reducers:
            self.client.execute(self.reducers)

        return '\n'.join(query_parts)

    def execute_query(self):
        """
        executes the query
        :return:
        """
        try:
            self.client.execute(self.create_query())
        except Exception as e:
            raise Exception("HIVE query failed: {}".format(e))
