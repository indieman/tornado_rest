from schematics.transforms import blacklist, whitelist
import itertools


class BaseMixin(object):
    pass


class SortMixin(BaseMixin):

    def prepare(self):

        sort = self.get_argument('$sort', None, strip=False)
        sort_list = []

        if sort:
            sort_params = sort.split(',')
            for p in sort_params:
                p = ''.join(p.split())
                if u'-' == p[0]:
                    direction = -1
                    p = p[1:]
                elif u'+' == p[0]:
                    direction = 1
                    p = p[1:]
                else:
                    direction = 1
                sort_list.append((p, direction))

        if sort_list:
            self._sort = sort_list

        super(SortMixin, self).prepare()


class PaginationMixin(BaseMixin):

    def prepare(self):

        page = int(self.get_argument('$page', 0))
        display = int(self.get_argument('$display', 20))
        self._skip = page * display
        self._limit = display

        super(PaginationMixin, self).prepare()


class FilterMixin(BaseMixin):

    def prepare(self):
        modifications = {'__lte': '$lte', '__lt': '$lt', '__gte': '$gte', '__gt': '$gt', '__in': '$in', '': ''}
        fields = self.model.fields.keys()
        query = {}

        for field in itertools.product(fields, modifications.keys()):
            param = ''.join(field)
            value = self.get_argument(param, None)
            if value:
                if '__in' == field[1]:
                    query[field[0]] = {modifications[field[1]]: value.split(',')}
                elif '' == field[1]:
                    query[field[0]] = value
                else:
                    query[field[0]] = {modifications[field[1]]: value}
        self._query = query
        super(FilterMixin, self).prepare()


class OnlyMixin(BaseMixin):

    def prepare(self):

        only = self.get_argument('$only', None)

        if only:
            self._fields = {}
            for field in only.split(','):
                self._fields[''.join(field.split())] = 1

        super(OnlyMixin, self).prepare()


class ExcludeMixin(BaseMixin):

    def prepare(self):
        exclude = self.get_argument('$exclude', None)
        if exclude:
            self._fields = {}
            for field in exclude.split(','):
                self._fields[''.join(field.split())] = 0

        super(ExcludeMixin, self).prepare()


class EmbedMixin(BaseMixin):
    pass


class FieldMixin(BaseMixin):
    pass

