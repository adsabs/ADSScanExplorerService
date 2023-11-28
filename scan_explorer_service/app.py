import os
import sys
from adsmutils import ADSFlask
from .views import *
from .extensions import *

def register_extensions(app: ADSFlask):
    """ Register extensions.

    Args:
        app (ADSFlask): Application object
    """
    #compress.init_app(app)
    limiter.init_app(app)
    discoverer.init_app(app)
    # appmap_flask.init_app(app)
    
    manifest_factory.set_iiif_image_info(2.0, 2)  # Version, ComplianceLevel


def register_views(app: ADSFlask):
    """ Register all views with the flask application.
    Args:
        app (ADSFlask): Application object
    """
    app.register_blueprint(bp_manifest)
    app.register_blueprint(bp_metadata)
    app.register_blueprint(bp_proxy)

    @app.after_request
    def after_request(response):
        if not response.headers.get('Access-Control-Allow-Origin'):
            response.headers.add('Access-Control-Allow-Origin', '*')
        if not response.headers.get('Access-Control-Allow-Headers'):
            response.headers.add('Access-Control-Allow-Headers', '*')
        if not response.headers.get('Access-Control-Allow-Methods'):
            response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,PATCH,OPTIONS')
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

    register_views(app)
    register_extensions(app)

    if app.config['ENV'] == "development":
        app.debug = True
        manifest_factory.set_debug("error_on_warning")
    else:
        manifest_factory.set_debug("error")

    return app


app = create_app()



