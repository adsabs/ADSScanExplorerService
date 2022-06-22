from marshmallow_sqlalchemy import SQLAlchemySchema, auto_field
from marshmallow import fields
from scan_explorer_service.models import Article, Collection, Page




class ArticleSchema(SQLAlchemySchema):
    class Meta:
        model = Article

    id = auto_field()
    bibcode = auto_field()
    collection_id = auto_field()
    thumbnail = fields.Str(dump_only=True)
    page_count = fields.Int(dump_only=True)


class PageSchema(SQLAlchemySchema):
    class Meta:
        model = Page
    
    id = auto_field()
    name = auto_field()
    label = auto_field()
    format = auto_field()
    color_type = auto_field()
    page_type = auto_field()
    width = auto_field()
    height = auto_field()
    collection_id = auto_field()
    volume_running_page_num = auto_field()
    articles = fields.Nested(ArticleSchema(only = ['id'], many=True))
    thumbnail = fields.Str(dump_only=True)


class CollectionSchema(SQLAlchemySchema):
    class Meta:
        model = Collection

    id = auto_field()
    journal = auto_field()
    volume = auto_field()
    type = auto_field()
    thumbnail = fields.Str(dump_only=True)
    page_count = fields.Int(dump_only=True)
    pages = fields.Nested(PageSchema, many=True)
