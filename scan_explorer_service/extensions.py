from .manifest_factory import ManifestFactoryExtended
from flask_compress import Compress
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_discoverer import Discoverer
from flasgger import Swagger

manifest_factory = ManifestFactoryExtended()
compress = Compress()
limiter = Limiter(key_func = get_remote_address)
discoverer = Discoverer()
swagger = Swagger()

