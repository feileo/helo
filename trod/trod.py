from trod.db.executer import RequestClient
from trod.errors import NoBindError
from trod.extra.logger import Logger
from trod.model.model import _Model
from trod.utils import Dict, async_dict_formatter


class _TrodModel(_Model):
    """ Trod model, but not exposed """

    @classmethod
    async def create(cls):
        """ A coroutine that create the table of model mappings.

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)

        For example:
            If you define a `User` model, you can:
                await User.create()
        """

        return await cls._create()

    @classmethod
    async def drop(cls):
        """ A coroutine that drop the table of model mappings.

        If you define a `User` model, you can:
            await User.drop()

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)
        """

        return await cls._drop()

    @classmethod
    async def alter(cls, modify_col=None, add_col=None, drop_col=None):
        """ A coroutine that alter the table of model mappings.
            At present, only the changes of the model can be manually transferred,
            and it is not automatically recognized.
            I will have time to optimize it in the future.

        Args:
            modify_col: Has been modified column in model.
            add_col: Newly added column in model
            drop_all:  Deleted column in model

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)
            Returns None if no arguments are given.

        For example:
            If you define a `User` model, and added a new column 'name', you can:
                await User.alter(add_col=User.name)

            If you add 'name' and 'age' columns you should：
                await User.alter(add_col=[User.name,User.age])
        """

        return await cls._alter(
            modify_col=modify_col, add_col=add_col, drop_col=drop_col
        )

    @classmethod
    async def show_create(cls):
        """ A coroutine that show created table's create syntax.

        Return:
            A `Trod.utils.Dict` object:
                Dict(table_name='', create_syntax='')
        """

        return await cls._show_create()

    @classmethod
    async def show_struct(cls):
        """ A coroutine that show created table's current structure.

        Return:
            A `Trod.utils.Dict` object:
                Dict(columns=[Dict(field)], indexs=[Dict(key)])
        """

        return await cls._show_struct()

    @classmethod
    async def exist(cls):
        """ A coroutine that table is exist.

        Return:
            A boolean value indicating whether the table already exists """

        return await cls._exist()

    @classmethod
    async def add(cls, instance):
        """ A coroutine that add a model instance,
            it corresponds to a row of data in the data table.

        Args:
            instance: a Trod.Model instance

        Return:
            A int primary key of this row of data
            None of add failed

        For example:
            user = User(name='test')
            user_id = await User.add(user)
        """

        return await cls._add(instance)

    @classmethod
    async def batch_add(cls, instances):
        """ A coroutine that batch add model instances,

        Args:
            instances: a list or tuple of Trod.Model instances

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)
        """

        return await cls._batch_add(instances)

    @classmethod
    async def remove(cls, where):
        """ A coroutine that remove by where.

        Args:
            where: a _Where or _Logic instance,
                   but you don't have to care about this, just use it like this:
                   1. User.name == user.name
                   2. And(User.name == user.name, User.age < user.age)
                   3. Or(User.age == user.age, User.id.in_([1,2,3]))

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)

        For example:
            1. await User.remove(User.name == user.name)
            2. await User.remove(
                   Or(User.age == user.age, User.id.in_([1,2,3]))
               )
        """
        return await cls._remove(where)

    @classmethod
    async def updete(cls, data, where):
        """ A coroutine that update by where.

        Args:
            data: a dict or Trod.utils.Dict object
            where: a _Where or _Logic instance,
                   detailed introduction can participate in remove docstring

        Return:
            A `Trod.utils.Dict` object:
                Dict(last_id=0, affected=0)
        """

        return await cls._updete(data, where)

    @classmethod
    async def get(cls, id_):
        """ A coroutine that get by id.

        Args:
            id_: primary key

        Return: Trod.Model instance
        """

        return await cls._get(id_)

    @classmethod
    async def batch_get(cls, id_list, cols=None):
        """ A coroutine that batch get by id list.

        Args:
            id_list: primary key list or tuple
            cols: query columns, default None is select all columns

        Return: a list of Trod.Model instances
        """

        return await cls._batch_get(id_list, cols=cols)

    @classmethod
    def query(cls, *cols):
        """ A coroutine that query.
            This method is more common and suitable for more complex queries.

        Args:
            cols: model columns

        Return:
            a _Generator instance

        For example:
            users = await User.query(User.name, User.age).filter(
                Or(User.age == user.age, User.id.in_([1,2,3]))
            ).order_by(
                user.age, desc=True
            ).row(limit=100, offset=10)
            or:
            count = await User.query(Func.count()).scalar()

            Func class provides a simple and commonly used function
            And have to be aware of is：
            The following methods must be called to return data：
                1. rows(limit, offset)
                2. first()
                3. all()
                4. scalar()
        """

        return cls._query(*cols)

    async def save(self):
        """ A coroutine that model's instance method for save self.

        For example:
            user = User(name='test')
            await user.save()
        """

        return await self._save()

    async def delete(self):
        """ A coroutine that model's instance method for delete self. """

        return await self._delete()


class Trod:
    """ Provide almost all the features through this class
        Usually you only need to instantiate a global Trod instance in your project.

    For example:
        db = Trod()
    """

    Model = _TrodModel
    Client = None

    def _checker(self):
        if self.Client is None:
            raise NoBindError(
                'No binding database or closed, unbinding is not allowed'
            )

    async def bind(self, url,
                   minsize=None, maxsize=None,
                   timeout=None, pool_recycle=None,
                   echo=None, loop=None, **kwargs):

        """ A coroutine that bind a database to your project.

        Args:
            url: db url.
            minsize: minimum sizes of the pool
            maxsize: maximum sizes of the pool.
            timeout: timeout of connection.
                     abandoning the connection from the pool after not getting the connection
            pool_recycle: connection reset period, default -1,
                          indicating that the connection will be reclaimed after a given time,
                          be careful not to exceed MySQL default time of 8 hours
            echo: executed log SQL queryes
            loop: is an optional event loop instance,
                  asyncio.get_event_loop() is used if loop is not specified.
            kwargs: and etc.
        """

        result = await RequestClient.bind_db(
            url=url, minsize=minsize, maxsize=maxsize,
            timeout=timeout, pool_recycle=pool_recycle,
            echo=echo, loop=loop, **kwargs
        )
        if result is True:
            self.Client = RequestClient()
        return True

    async def unbind(self):
        """ A coroutine that unbind database. """
        self._checker()
        return await RequestClient.unbind()

    @property
    def db_info(self):
        """ Get the basic info of the database connection """

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
        """ Returns a boolean value is whether the database binded """

        return bool(self.Client)

    @async_dict_formatter
    async def text(self, sql, args=None, rows=None):
        """ A coroutine that used to directly execute SQL statements """

        self._checker()
        result = await self.Client.text(sql, args=args, rows=rows)
        return result.data

    async def create_all(self, module):
        """ A coroutine that create all the models of a module

        Args:
            module: a module that defines several models

        Return:
            A list of created Model name

        For example:
            from tests import models
            await db.create_all(models)
        """

        self._checker()
        succeed = []
        for key, value in vars(module).items():
            if hasattr(value, '__base__') and value.__base__ is self.Model:
                if not await value.exist():
                    await value.create()
                    Logger.info(
                        "created Model '{table_name}' in db: '{db}'".format(
                            table_name=key, db=self.db_info.info.db.db
                        )
                    )
                    succeed.append(key)
                else:
                    Logger.error("table '{}' already exists".format(key))
        return succeed

    async def batch_create(self, *models):
        """ A coroutine that batch create same model.

        Args:
            models: one or more models

        Return:
            A list of created table name

        For example:
            from tests.models import User, Order
            await db.batch_create(User, Order)
        """

        self._checker()
        succeed = []
        if not models:
            Logger.warning("parameter 'models' is empty, 'batch_create' nothing to do")
            return succeed

        for model in models:
            if not issubclass(model, self.Model):
                raise ValueError(
                    'create model type must be {}, get {}'.format(self.Model, model)
                )
            if not await model.exist():
                await model.create()
                Logger.info(
                    "created table '{table_name}' in db: '{db}'".format(
                        table_name=model.__meta__.table,
                        db=self.db_info.info.db.db
                    )
                )
                succeed.append(model.__meta__.table)
            else:
                Logger.error(
                    message="table '{}' already exists".format(model.__meta__.table)
                )
        return succeed

    async def drop_all(self, module):
        """ A coroutine that drop all the models of a module

        Args:
            module: a module that defines several models

        Return:
            A list of droped Model name

        For example:
            from tests import models
            await db.drop_all(models)
        """

        self._checker()
        succeed = []
        for key, value in vars(module).items():
            if hasattr(value, '__base__') and value.__base__ is self.Model:
                if await value.exist():
                    await value.drop()
                    Logger.info(
                        "dropped Model '{model_name}' from db: '{db}'".format(
                            model_name=key, db=self.db_info.info.db.db
                        )
                    )
                    succeed.append(key)
                else:
                    Logger.error(
                        message="drop table '{}' does not exist".format(key)
                    )
        return succeed

    async def batch_drop(self, *models):
        """ A coroutine that batch drop same model.

        Args:
            models: one or more models

        Return:
            A list of droped table name

        For example:
            from tests.models import User, Order
            await db.batch_drop(User, Order)
        """

        self._checker()
        succeed = []
        if not models:
            Logger.warning("parameter 'models' is empty, 'batch_drop' nothing to do")
            return succeed

        for model in models:
            if not issubclass(model, self.Model):
                raise ValueError(
                    'drop model type must be {}, get {}'.format(self.Model, model)
                )
            if await model.exist():
                await model.drop()
                Logger.info(
                    "dropped table '{table_name}' from db: '{db}'".format(
                        table_name=model.__meta__.table,
                        db=self.db_info.info.db.db
                    )
                )
                succeed.append(model.__meta__.table)
            else:
                Logger.error(
                    message="drop table '{}' does not exist".format(model.__meta__.table)
                )
        return succeed
