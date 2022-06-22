import os
import sys
from adsmutils import ADSFlask
from scan_explorer_service.schema.models_schema import *
from scan_explorer_service.schema.metadata_schema import *
from .views import *
from .extensions import *
from apispec.ext.marshmallow import MarshmallowPlugin
from flasgger import APISpec


def register_extensions(app: ADSFlask):
    """ Register extensions.

    Args:
        app (ADSFlask): Application object
    """
    compress.init_app(app)
    limiter.init_app(app)
    discoverer.init_app(app)
    swagger.init_app(app)
    swagger.template = APISpec(
        title='ADS Scan Explorer Service',
        version='1.0',
        openapi_version='2.0',
        plugins=[MarshmallowPlugin()]
    ).to_flasgger(
        app,
        definitions=[ArticleSchema, PageSchema, CollectionSchema,
        PaginatedCollectionsSchema, PaginatedArticlesSchema, PaginatedPagesSchema, 
        SearchQuerySchema],
    )

    manifest_factory.set_base_image_uri(app.config.get('IMAGE_API_BASE_URL'))
    manifest_factory.set_iiif_image_info(2.0, 2)  # Version, ComplianceLevel


def register_views(app: ADSFlask):
    """ Register all views with the flask application.
    Args:
        app (ADSFlask): Application object
    """
    app.register_blueprint(bp_manifest)
    app.register_blueprint(bp_metadata)

    @app.after_request
    def after_request(response):
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', '*')
        response.headers.add('Access-Control-Allow-Methods',
                             'GET,PUT,POST,DELETE,PATCH,OPTIONS')
        return response


def create_app(**config):
    """ Create application and initialize dependencies.

    Returns:
        ADSFlask: Application object
    """
    opath = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if opath not in sys.path:
        sys.path.insert(0, opath)

    if config:
        app = ADSFlask(__name__, static_folder=None, local_config=config)
    else:
        app = ADSFlask(__name__, static_folder=None)

    app.url_map.strict_slashes = False

    if app.config['ENV'] == "development":
        app.debug = True

    if app.debug:
        manifest_factory.set_debug("error_on_warning")
    else:
        manifest_factory.set_debug("error")

    register_views(app)
    register_extensions(app)

    return app


app = create_app()
