import enum
import opensearchpy
from flask import current_app, jsonify


class ApiErrors(enum.Enum):
    SearchError = 1


def search_error_response(exc):
    """Map a search failure onto the right status: the caller's fault, ours, or a transient outage.

    A 4xx says the caller must change the request, so it is used only for failures the caller
    can act on. Our own misconfiguration and unexpected failures are 5xx, which keeps them
    visible to alerting, and their text stays in the log rather than the response.
    """
    if isinstance(exc, opensearchpy.exceptions.RequestError):
        current_app.logger.warning("OpenSearch rejected the query: %s", exc)
        return jsonify(message=str(exc), type=ApiErrors.SearchError.value), 400

    if isinstance(exc, (opensearchpy.exceptions.ConnectionError,
                        opensearchpy.exceptions.ConnectionTimeout)):
        current_app.logger.exception("OpenSearch unreachable: %s", exc)
        return jsonify(message='Search service temporarily unavailable',
                       type=ApiErrors.SearchError.value), 503

    if isinstance(exc, opensearchpy.exceptions.TransportError):
        status = getattr(exc, 'status_code', None)
        current_app.logger.exception("OpenSearch transport error %s: %s", status, exc)
        if isinstance(status, int) and status >= 500:
            return jsonify(message='Search service temporarily unavailable',
                           type=ApiErrors.SearchError.value), 503
        return jsonify(message='Internal error', type=ApiErrors.SearchError.value), 500

    if isinstance(exc, ValueError):
        return jsonify(message=str(exc), type=ApiErrors.SearchError.value), 400

    current_app.logger.exception("An exception has occurred: %s", exc)
    return jsonify(message='Internal error', type=ApiErrors.SearchError.value), 500
