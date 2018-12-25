from trod.db.executer import RequestClient
from trod.extra.logger import Logger
from trod.model.loader import Loader
from trod.model.model import TrodModel
from trod.utils import Dict


class Trod:

    Model = TrodModel
    Client = None

    def _checker(self):
        if self.Client is None:
            raise RuntimeError(
                'No binding database or closed, unbinding is not allowed'
            )

    async def bind(self, url,
                   minsize=None, maxsize=None,
                   timeout=None, pool_recycle=None,
                   echo=None, loop=None, **kwargs):

        result = await RequestClient.bind_db(
            url=url, minsize=minsize, maxsize=maxsize,
            timeout=timeout, pool_recycle=pool_recycle,
            echo=echo, loop=loop, **kwargs
        )
        if result is True:
            self.Client = RequestClient()
        return result

    async def unbind(self):
        self._checker()
        return await RequestClient.close()

    @property
    def db_info(self):
        info_dict = Dict()
        if self.Client is None:
            return info_dict
        info_dict.update(
            info=RequestClient.get_conn_info(),
            status=RequestClient.get_conn_status()
        )
        return info_dict

    @property
    def is_bind(self):
        return bool(self.Client)

    def text(self, sql, args=None, rows=None):
        self._checker()
        result = self.Client.text(sql, args=args, rows=rows)
        if result.is_fetch:
            return Loader(self.Model, result.data).load()
        return result.data

    async def create_all(self, module):
        self._checker()
        for key, value in vars(module).items():
            if hasattr(value, '__base__') and value.__base__ is self.Model:
                if not await value.exist():
                    await value.create()
                    Logger.info(
                        "Created <Table '{table_name}'> in <db: '{db}'>".format(
                            table_name=key, db=self.db_info.info.db.db
                        )
                    )
                else:
                    Logger.error("<Table '{}>' already exists".format(key))

    async def drop_all(self, module):
        self._checker()
        for key, value in vars(module).items():
            if hasattr(value, '__base__') and value.__base__ is self.Model:
                if await value.exists():
                    await value.drop()
                    Logger.info(
                        "dropped <Table '{table_name}'> from <db: '{db}'>".format(
                            table_name=key, db=self.db_info.info.db.db
                        )
                    )
