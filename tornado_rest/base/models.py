import logging
from datetime import timedelta
from bson.objectid import ObjectId
from tornado import gen, ioloop
from tornado.options import options
import motor
from schematics.models import Model
from schematics.types import NumberType, BaseType
from pymongo.errors import ConnectionFailure

l = logging.getLogger(__name__)
MAX_FIND_LIST_LEN = 100


class ObjectIdType(NumberType):
    def __init__(self, **kwargs):
        number_class = kwargs.pop('number_class', ObjectId)
        number_type = kwargs.pop('number_type', "ObjectId")
        super(ObjectIdType, self).__init__(
            number_class=number_class,
            number_type=number_type, **kwargs)


class ReferenceType(BaseType):
    def __init__(self, model, *args, **kwargs):
        self._model = model
        super(ReferenceType, self).__init__(*args, **kwargs)

    @property
    def model(self):
        return self._model


class DictType(BaseType):
    def __init__(self, *args, **kwargs):
        super(DictType, self).__init__(*args, **kwargs)

    def to_primitive(self, value, context=None):
        return value

    def to_native(self, value):
        return value


class IncludedType(BaseType):
    def __init__(self, model, *args, **kwargs):
        self._model = model
        super(IncludedType, self).__init__(*args, **kwargs)

    @property
    def model(self):
        return self._model

    def to_primitive(self, value, context=None):
        return value.to_primitive()

    def to_native(self, value):
        return value


class GeoPoint(BaseType):
    def __init__(self, *args, **kwargs):
        super(GeoPoint, self).__init__(*args, **kwargs)

    def to_primitive(self, value, context=None):
        # return {"type": "Point", "coordinates": [value[0], value[1]]}
        return value

    def to_native(self, value):
        return value


class BaseModel(Model):
    """
    Provides generic methods to work with model.
    Why use `MyModel.find_one` instead of `db.collection.find_one` ?
    1. Collection name is declared inside the model, so it is not needed
        to provide it all the time
    2. This `MyModel.find_one` will return MyModel instance, whereas
        `db.collection.find_one` will return dictionary

    Example with directly access:

        db_result = yield motor.Op(db.collection_name.find_one({"i": 3}))
        obj = MyModel(db_result)

    Same example, but using MyModel.find_one:

        obj = yield MyModel.find_one(db, {"i": 3})
    """

    _id = NumberType(number_class=ObjectId, number_type="ObjectId")
    _references = []

    @classmethod
    def get_reference_type(cls, name):
        return cls._fields.get(name, None)

    @classmethod
    def process_query(cls, query):
        """
        query can be modified here before actual providing to database.
        """
        query = dict(query)
        return query

    @classmethod
    def references(cls):
        for item in cls._fields.items():
            if isinstance(item[1], ReferenceType):
                cls._references.append(item[0])
        return cls._references

    def to_json(self):
        json_data = self
        if isinstance(json_data._id, ObjectId):
            json_data._id = str(json_data._id)
        if 'role' in self._options.roles:
            return json_data.to_primitive(role='role')
        else:
            return json_data.to_primitive()

    @classmethod
    def get_model_name(cls):
        return cls.MONGO_COLLECTION

    @classmethod
    def get_collection(cls):
        return getattr(cls, 'MONGO_COLLECTION', None)

    @classmethod
    def check_collection(cls, collection):
        return collection or cls.get_collection()

    @classmethod
    def find_list_len(cls):
        return getattr(cls, 'FIND_LIST_LEN', MAX_FIND_LIST_LEN)

    @classmethod
    @gen.coroutine
    def find_one(cls, db, query, collection=None, model=True):
        result = None
        query = cls.process_query(query)
        for i in cls.reconnect_amount():
            try:
                result = yield motor.Op(
                    db[cls.check_collection(collection)].find_one, query)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(
                    i,
                    'find_one')
                if exceed:
                    raise e
            else:
                if model and result:
                    result = cls.make_model(result, "find_one")
                raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def remove_entries(cls, db, query, collection=None):
        """
        Removes documents by given query.
        Example:
            obj = yield ExampleModel.remove_entries(
                self.db, {"first_name": "Hello"})
        """
        c = cls.check_collection(collection)
        query = cls.process_query(query)
        for i in cls.reconnect_amount():
            try:
                yield motor.Op(db[c].remove, query)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(
                    i,
                    'remove_entries')
                if exceed:
                    raise e
            else:
                return

    @gen.coroutine
    def remove(self, db, collection=None):
        """
        Removes current instance from database.
        Example:
            obj = yield ExampleModel.find_one(self.db, {"last_name": "Sara"})
            yield obj.remove(self.db)
        """
        _id = self.to_primitive()['_id']
        yield self.remove_entries(db, {"_id": _id}, collection)

    @gen.coroutine
    def save(self, db, collection=None, ser=None):
        """
        If object has _id, then object will be rewritten.
        If not, object will be inserted and _id will be assigned.
        Example:
            obj = ExampleModel({"first_name": "Vasya"})
            yield obj.save(self.db)
        """
        c = self.check_collection(collection)
        data = self.get_data_for_save(ser)
        result = None
        for i in self.reconnect_amount():
            try:
                result = yield motor.Op(db[c].save, data)
            except ConnectionFailure as e:
                exceed = yield self.check_reconnect_tries_and_wait(i, 'save')
                if exceed:
                    raise e
            else:
                if result:
                    self._id = result
                return

    @gen.coroutine
    def insert(self, db, collection=None, ser=None, **kwargs):
        """
        If object has _id, then object will be inserted with given _id.
        If object with such _id is already in database, then
        pymongo.errors.DuplicateKeyError will be raised.
        If object has no _id, then object will be inserted and _id will be
        assigned.

        Example:
            obj = ExampleModel({"first_name": "Vasya"})
            yield obj.insert(self.db)
        """
        c = self.check_collection(collection)
        data = self.get_data_for_save(ser)
        for i in self.reconnect_amount():
            try:
                result = yield motor.Op(db[c].insert, data, **kwargs)
            except ConnectionFailure as e:
                exceed = yield self.check_reconnect_tries_and_wait(i, 'insert')
                if exceed:
                    raise e
            else:
                if result:
                    self._id = result
                return

    @gen.coroutine
    def update(self, db, query=None, collection=None, ser=None, upsert=False,
               multi=False):
        """
        Updates the object. If object has _id, then try to update the object.
        If object with given _id is not found in database, or object doesn't
        have _id field, then save it and assign generated _id.
        Difference from save:
            Suppose such object in database:
                {"_id": 1, "foo": "egg1", "bar": "egg2"}
            We want to save following data:
                {"_id": 1, "foo": "egg3"}
            If we'll run save, then in database will be following data:
                {"_id": 1, "foo": "egg3"} # "bar": "egg2" is removed
            But if we'll run update, then existing fields will be kept:
                {"_id": 1, "foo": "egg3", "bar": "egg2"}
        Example:
            obj = yield ExampleModel.find_one(self.db, {"first_name": "Foo"})
            obj.last_name = "Bar"
            yield obj.update(self.db)
        """
        c = self.check_collection(collection)
        data = self.get_data_for_save(ser)
        if query is None and '_id' not in data:
            yield self.save(db, c, ser=data)
        else:
            if not query:
                _id = data.pop("_id")
                query = {"_id": _id}
            for i in self.reconnect_amount():
                try:
                    result = yield motor.Op(
                        db[c].update,
                        query, {"$set": data}, upsert=upsert, multi=multi)
                except ConnectionFailure as e:
                    exceed = yield self.check_reconnect_tries_and_wait(
                        i,
                        'update')
                    if exceed:
                        raise e
                else:
                    l.debug("Update result: {0}".format(result))
                    return

    @classmethod
    def get_cursor(cls, db, query, collection=None, fields={}):
        c = cls.check_collection(collection)
        query = cls.process_query(query)
        return db[c].find(query, fields) if fields else db[c].find(query)

    @classmethod
    @gen.coroutine
    def find(cls, cursor, model=True, list_len=None):
        """
        Returns a list of found documents.

        :arg cursor: motor cursor for find
        :arg model: if True, then construct model instance for each document.
            Otherwise, just leave them as list of dicts.
        :arg list_len: list of documents to be returned.

        Example:
            cursor = ExampleModel.get_cursor(self.db, {"first_name": "Hello"})
            objects = yield ExampleModel.find(cursor)
        """
        result = None
        list_len = list_len or cls.find_list_len() or MAX_FIND_LIST_LEN
        for i in cls.reconnect_amount():
            try:
                result = yield motor.Op(cursor.to_list, list_len)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(i, 'find')
                if exceed:
                    raise e
            else:
                if model:
                    field_names_set = set(cls._fields.keys())
                    for i in xrange(len(result)):
                        result[i] = cls.make_model(
                            result[i], "find", field_names_set)
                raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def count(cls, cursor, model=True):
        result = None

        for i in cls.reconnect_amount():
            try:
                result = yield motor.Op(cursor.count)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(i, 'count')
                if exceed:
                    raise e
            else:
                raise gen.Return(result)

    @classmethod
    @gen.coroutine
    def aggregate(cls, db, pipe_list, collection=None):
        c = cls.check_collection(collection)
        for i in cls.reconnect_amount():
            try:
                result = yield motor.Op(db[c].aggregate, pipe_list)
            except ConnectionFailure as e:
                exceed = yield cls.check_reconnect_tries_and_wait(
                    i,
                    'aggregate')
                if exceed:
                    raise e
            else:
                raise gen.Return(result)

    @staticmethod
    def reconnect_amount():
        return xrange(options.mongodb_reconnect_retries + 1)

    @classmethod
    @gen.coroutine
    def check_reconnect_tries_and_wait(cls, reconnect_number, func_name):
        if reconnect_number >= options.mongodb_reconnect_retries:
            raise gen.Return(True)
        else:
            timeout = options.mongodb_reconnect_timeout
            l.warning(
                "ConnectionFailure #{0} in {1}.{2}. Waiting {3} seconds"
                .format(
                    reconnect_number + 1, cls.__name__, func_name, timeout))
            io_loop = ioloop.IOLoop.instance()
            yield gen.Task(io_loop.add_timeout, timedelta(seconds=timeout))

    def get_data_for_save(self, ser):
        data = ser or self.to_primitive()
        if '_id' in data and data['_id'] is None:
            del data['_id']
        return data

    @classmethod
    def make_model(cls, data, method_name, field_names_set=None):
        """
        Create model instance from data (dict).
        """
        if field_names_set is None:
            field_names_set = set(cls._fields.keys())
        else:
            if not isinstance(field_names_set, set):
                field_names_set = set(field_names_set)
        new_keys = set(data.keys()) - field_names_set
        if new_keys:
            l.warning(
                "'{0}' has unhandled fields in DB: "
                "'{1}'. {2} returned data: '{3}'"
                .format(cls.__name__, new_keys, data, method_name))
            for new_key in new_keys:
                del data[new_key]
        return cls(data)


class OnlyIdModel(BaseModel):
    _id = ObjectIdType()

