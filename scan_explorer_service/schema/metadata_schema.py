import shlex
from marshmallow import Schema, fields, exceptions, validate
from scan_explorer_service.schema.models_schema import ArticleSchema, CollectionSchema, PageSchema


class PaginationSchema(Schema):
    page = fields.Int()
    pages = fields.Int()
    per_page = fields.Int()
    total = fields.Int()
    text_query = fields.Str()


class PaginatedCollectionsSchema(PaginationSchema):
    items = fields.Nested(CollectionSchema, many=True)


class PaginatedArticlesSchema(PaginationSchema):
    items = fields.Nested(ArticleSchema, many=True)


class PaginatedPagesSchema(PaginationSchema):
    items = fields.Nested(PageSchema, many=True)


class DelimitedListField(fields.List):
    def _deserialize(self, value, attr, data, **kwargs):
        try:
            qs_arr = [q for q in shlex.split(value) if ':' in q]
            qs_dict = dict(kv.split(':') for kv in qs_arr)
            return qs_dict
        except AttributeError:
            raise exceptions.ValidationError(
                f"{attr} is not a delimited list it has a non string value {value}."
            )


class SearchQuerySchema(Schema):
    q = DelimitedListField(fields.String(
        validate=validate.Length(min=1)), required=True)
    page = fields.Int()
    limit = fields.Int()
