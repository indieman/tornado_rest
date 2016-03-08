__author__ = 'indieman'

import json
from tornado.web import RequestHandler, HTTPError
from bson import ObjectId
from bson.errors import InvalidId

from tornado import gen

from schematics.exceptions import ValidationError, ModelConversionError
from .models import BaseModel, OnlyIdModel


class UnknownNestedResource(Exception):
    pass


class ObjectDoesNotExist(Exception):
    pass


class Unauthorized(Exception):
    pass


class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, BaseModel):
            return o.to_json()
        return json.JSONEncoder.default(self, o)


#def is_allow(f):

    #def wrapper(self, pk=None, nested=None, nested_pk=None):
        #if f.__name__ in self.allowed_methods:
            #return f(self, pk, nested, nested_pk)
        #else:
            #return self.write_error(405, "You can't make it", [])

    #return wrapper

def is_allow(f):

    def wrapper(self, *args, **kwargs):
        if f.__name__ in self.allowed_methods:
            return f(self, *args, **kwargs)
        else:
            return self.write_error(405, "You can't make it", [])

    return wrapper

class SimpleHandler(RequestHandler):
    data = None
    model = None
    db = None

    _filter = {}
    _fields = {}
    _query = {}
    _sort = [("_id", 1), ]
    _limit = 20
    _skip = 0

    def get_cursor(self, model=None):
        if not model:
            self._cursor = self.model.get_cursor(
                self.db,
                query=self._query,
                fields=self._fields
            ).sort(self._sort).skip(self._skip).limit(self._limit)
        else:
            self._cursor = model.get_cursor(
                self.db,
                query=self._query,
                fields=self._fields
            ).sort(self._sort).skip(self._skip).limit(self._limit)

        return self._cursor

    def initialize(self, **kwargs):
        super(SimpleHandler, self).initialize(**kwargs)
        self.db = self.settings["db"]

    def write_error(self, code, message="Error", errors=[], **kwargs):
        result = {
            "code": code,
            "message": message,
            "errors": errors
        }
        self._status_code = code
        self.finish(JSONEncoder().encode(result))

    def render(self, data, **kwargs):

        self.finish(JSONEncoder().encode(data))

    def prepare(self):
        self.get_cursor()


class BaseHandler(SimpleHandler):
    query = None
    signals = None
    _data = None

    allowed_methods = []

    def options(self, *args, **kwargs):
        allowed_methods = ",".join([method.upper() for method in self.allowed_methods])
        self.add_header("Allow", allowed_methods)
        self.finish()

    def get(self, *args, **kwargs):
        pass

    def post(self, *args, **kwargs):
        pass

    def put(self, *args, **kwargs):
        pass

    def patch(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def pre_get(self):
        pass

    def post_get(self):
        pass

    def pre_post(self):
        pass

    def post_post(self):
        pass

    def pre_put(self):
        pass

    def post_put(self):
        pass

    def pre_patch(self):
        pass

    def post_patch(self):
        pass

    def pre_delete(self):
        pass

    def post_delete(self):
        pass


class BaseOneHandler(BaseHandler):

    allowed_methods = ["options", "get", "patch", "put", "delete"]

    @gen.coroutine
    @is_allow
    def get(self, pk, *args, **kwargs):
        self.pre_get()

        try:
            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        else:
            self.render(object)

        self.post_get()

    @gen.coroutine
    @is_allow
    def patch(self, pk, *args, **kwargs):
        self.pre_patch()

        try:
            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

            raw_data = json.loads(self.request.body)

            for item in raw_data:
                object[item] = raw_data[item]

            object.validate()

            yield object.update(self.db)
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except (ModelConversionError, ValidationError) as e:
            self.write_error(422, "Validation Failed", [e.message])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        except ValueError:
            self.write_error(400, "Bad Request", [])
        else:
            self.render(object.to_primitive())
            self.post_patch()

    @gen.coroutine
    @is_allow
    def put(self, pk, *args, **kwargs):
        self.pre_put()
        try:
            raw_data = json.loads(self.request.body)

            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

            object = self.model(raw_data)

            object._id = ObjectId(pk.decode("utf-8"))

            object.validate(strict=True)
            yield object.update(self.db)

        except (ModelConversionError, ValidationError) as e:
            self.write_error(422, "Validation Failed", [e.message])
        except ValueError:
            self.write_error(400, "Bad Request", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(object.to_primitive())
        self.post_put()

    @gen.coroutine
    @is_allow
    def delete(self, pk, *args, **kwargs):
        self.pre_delete()

        try:
            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

            yield object.remove(self.db)
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])

        self.post_delete()


class BaseManyHandler(BaseHandler):

    allowed_methods = ["options", "head", "get", "post"]

    @gen.coroutine
    @is_allow
    def get(self, *args, **kwargs):
        self.pre_get()

        objects = yield self.model.find(self._cursor)
        objects = map(lambda item: item.to_json(), objects)

        self.render(objects)
        self.post_get()

    @gen.coroutine
    @is_allow
    def post(self, *args, **kwargs):
        self.pre_post()

        try:
            raw_data = json.loads(self.request.body)
            object = self.model(raw_data)
            object.validate(strict=True)
        except (ModelConversionError, ValidationError) as e:
            self.write_error(422, "Validation Failed", [e.message])
        except ValueError:
            self.write_error(400, "Bad Request", [])
        else:
            yield object.insert(self.db)
            self.render(object.to_primitive())

        self.post_post()

    @gen.coroutine
    @is_allow
    def head(self, *args, **kwargs):

        count = yield self.model.count(self._cursor)

        self.add_header("X-Count-Per-Page", self._limit)
        self.add_header("X-Total-Items", count)

        self.finish()


class BaseNestedOneHandler(BaseHandler):

    allowed_methods = ["options", "get", "put", "patch", "delete"]

    @gen.coroutine
    @is_allow
    def get(self, pk, nested, nested_pk, *args, **kwargs):
        self.pre_get()

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            nested_field = getattr(self.model, nested, None)

            object = yield nested_field.model.find_one(
                self.db,
                {"_id": ObjectId(nested_pk.decode("utf-8"))}
            )
            if not object:
                raise ObjectDoesNotExist()

        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(object)

        self.post_get()

    @gen.coroutine
    @is_allow
    def put(self, pk, nested, nested_pk, *args, **kwargs):

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            nested_field = getattr(self.model, nested, None)
            nested_object = yield nested_field.model.find_one(
                self.db,
                {"_id": ObjectId(nested_pk.decode("utf-8"))}
            )

            if not nested_object:
                raise ObjectDoesNotExist()

            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

            nested_list = getattr(object, nested)
            nested_list.append(nested_object._id)
            setattr(object, nested, list(set(nested_list)))

            yield object.update(self.db)
        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(nested_object.to_primitive())


    @gen.coroutine
    @is_allow
    def patch(self, pk, nested, nested_pk, **kwargs):

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            raw_data = json.loads(self.request.body)

            id_model = OnlyIdModel(raw_data)
            id_model.validate()

            new_el = ObjectId(raw_data["_id"].decode("utf-8"))
            old_el = ObjectId(nested_pk.decode("utf-8"))

            nested_field = getattr(self.model, nested, None)
            nested_object = yield nested_field.model.find_one(
                self.db,
                {"_id": new_el}
            )

            if not nested_object:
                raise ObjectDoesNotExist()

            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )
            if not object:
                raise ObjectDoesNotExist()

            nested_list = getattr(object, nested)

            if old_el not in nested_list:
                raise ObjectDoesNotExist()

            nested_list.remove(old_el)
            nested_list.append(new_el)

            setattr(object, nested, list(set(nested_list)))
            yield object.update(self.db)
        except (ValidationError, ModelConversionError) as e:
            self.write_error(422, "Validation Failed", [e.messages])
        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except ValueError:
            self.write_error(400, "Bad Request", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(nested_object.to_primitive())

    @gen.coroutine
    @is_allow
    def delete(self, pk, nested, nested_pk, *args, **kwargs):

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            nested_field = getattr(self.model, nested, None)

            nested_object = yield nested_field.model.find_one(
                self.db,
                {"_id": ObjectId(nested_pk.decode("utf-8"))}
            )

            if not nested_object:
                raise ObjectDoesNotExist()

            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not object:
                raise ObjectDoesNotExist()

            nested_list = getattr(object, nested)
            nested_list.remove(nested_object._id)
            setattr(object, nested, nested_list)
            yield object.update(self.db)

        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(nested_object.to_primitive())


class BaseNestedManyHandler(BaseHandler):

    allowed_methods = ["options", "head", "get", "post"]

    @gen.coroutine
    @is_allow
    def get(self, pk, nested, *args, **kwargs):
        self.pre_get()

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            main_object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            nested_ids = getattr(main_object, nested, [])
            nested_field = getattr(self.model, nested, None)
            cursor = nested_field.model.get_cursor(self.db, {"_id": {"$in": nested_ids}})

            objects = yield nested_field.model.find(cursor)

            if not objects:
                raise ObjectDoesNotExist()

            objects = map(lambda item: item.to_json(), objects)

        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(objects)

        self.post_get()

    @gen.coroutine
    @is_allow
    def post(self, pk, nested, *args, **kwargs):
        self.pre_post()

        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            raw_data = json.loads(self.request.body)

            nested_field = getattr(self.model, nested, None)
            nested_object = nested_field.model(raw_data)
            nested_object.validate(strict=True)

            yield nested_object.insert(self.db)

            object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )
            if not object:
                raise ObjectDoesNotExist()

            nested_list = getattr(object, nested)
            nested_list.append(nested_object._id)
            setattr(object, nested, list(set(nested_list)))

            yield object.update(self.db)

        except UnknownNestedResource:
            self.write_error(404, "Unknown resource", [])
        except (ModelConversionError, ValidationError) as e:
            self.write_error(422, "Validation Failed", [e.message])
        except ValueError:
            self.write_error(400, "Bad Request", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.render(nested_object.to_primitive())

        self.post_post()

    @gen.coroutine
    @is_allow
    def head(self, pk, nested, *args, **kwargs):
        try:
            if nested not in self.model.references():
                raise UnknownNestedResource()

            main_object = yield self.model.find_one(
                self.db,
                {"_id": ObjectId(pk.decode("utf-8"))}
            )

            if not main_object:
                raise ObjectDoesNotExist()

            nested_field = getattr(self.model, nested, None)

            count = yield nested_field.model.count(self._cursor)

            self.add_header("X-Count-Per-Page", self._limit)
            self.add_header("X-Total-Items", count)
        except UnknownNestedResource:
            self.write_error(404, "Unknown nested resource", [])
        except InvalidId:
            self.write_error(404, "Invalid id", [])
        except ObjectDoesNotExist:
            self.write_error(404, "Object does not exist", [])
        else:
            self.finish()

