import urllib.parse as urlparse

from trod.errors import InvaildDBUrlError
from trod import utils


class UrlParser:
    """ database url parser """

    SCHEMES = ('mysql',)

    __slots__ = ('url', )

    def __init__(self, url):
        self.url = url

    @utils.troddict_formatter()
    def parse(self):
        """ do parse database url """

        if not self._is_illegal_url():
            raise InvaildDBUrlError(f'Invalid db url {self.url}')
        self._register()

        url = urlparse.urlparse(self.url)

        if url.scheme not in self.SCHEMES:
            raise ValueError(f'Unsupported scheme {url.scheme}')

        path, query = url.path[1:], url.query
        if '?' in path and not url.query:
            path, query = path.split('?', 2)

        query = urlparse.parse_qs(query)

        hostname = url.hostname or ''
        if '%2f' in hostname.lower():
            hostname = url.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = hostname.replace('%2f', '/').replace('%2F', '/')

        db_meta = {
            'db': urlparse.unquote(path or ''),
            'user': urlparse.unquote(url.username or ''),
            'password': urlparse.unquote(url.password or ''),
            'host': hostname,
            'port': url.port or '',
        }

        options = {}
        for key, values in query.items():
            if url.scheme == 'mysql' and key == 'ssl-ca':
                options['ssl'] = {'ca': values[-1]}
                continue

            options[key] = values[-1]

        if options:
            db_meta.update(options)

        return db_meta

    def _register(self):
        """ Register database schemes in URLs """

        urlparse.uses_netloc.extend(self.SCHEMES)

    def _is_illegal_url(self):
        """ A bool of is illegal url """

        url = urlparse.urlparse(self.url)
        if all([url.scheme, url.netloc]):
            return True
        return False
