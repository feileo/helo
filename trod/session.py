# -*- coding=utf8 -*-
"""
"""


class Session():
    """ 管理 Connector
        维护一块缓存
    """

    def __init__(self, connector):
        self.set_buffer = set()

    # @property
    # def status(self):
    #     return {
    #         'conn_pool': self.executer.conn_pool.me,
    #         'set_buffer': None
    #     }

    # def close(self):
    #     return self.executer.close()
    #     # submit buffer

    # def create_table(self, sql):
    #     return self.executer.submit(sql)

    # def show(self, table):
        # show_sql = f'show full columns from {table}'
    #     return self.executer.select(show_sql)
