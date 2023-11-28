
from flask import current_app, url_for

def url_for_proxy(endpoint: str, **values):
    values['_external'] = False

    server, prefix = proxy_url()
    current_app.logger.debug(f"Server is {server} and prefix is {prefix}.") 
    path = url_for(endpoint, **values).lstrip('/')

    current_app.logger.debug(f"Url is {server}/{prefix}/{path}.") 
    return f'{server}/{prefix}/{path}'

def proxy_url():
    server = current_app.config.get('PROXY_SERVER').rstrip('/')
    prefix = current_app.config.get('PROXY_PREFIX').strip('/')
    return server, prefix