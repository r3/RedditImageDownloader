import configparser
import itertools
import logging
import os
import urllib

import imgurpython
import requests

from utils import Downloadable


CONFIG_FILE = 'config.ini'

log = logging


class SourceManager():
    source_name = 'Abstract Source'
    _connected = False
    _configured = False
    _config = None

    @classmethod
    def _configure(cls, *args, **kwargs):
        raise NotImplementedError()

    @classmethod
    def _connect(cls, *args, **kwargs):
        raise NotImplementedError()

    def _query_condition(self):
        raise NotImplementedError()

    @classmethod
    def match_source(cls, url):
        raise NotImplementedError()

    def downloadables_from_url(self, url):
        raise NotImplementedError()


class GfycatManager(SourceManager):
    source_name = 'gfycat'

    @classmethod
    def match_source(cls, url):
        parsed = urllib.parse.urlparse(url)
        matches = 'gfycat.com' in parsed.hostname
        log.debug("Source {} GfycatManager".format(
            'matchs' if matches else 'does not match'))
        return matches

    def downloadables_from_url(self, url):
        image_name = url.split('/')[-1]
        image_name = image_name.split('?')[0]
        gif_url = 'http://giant.gfycat.com/{}.gif'.format(image_name)
        log.debug("Yielding Downloadable from URL: {}".format(gif_url))
        yield Downloadable(gif_url)


class DirectLinkManager(SourceManager):
    source_name = 'directlink'
    accepted_extensions = None

    def __init__(self):
        if self._configured is False:
            self._configure(CONFIG_FILE)

    @classmethod
    def _configure(cls, config_file):
        cls._configured = True

        if cls._config is None:
            cls._config = configparser.ConfigParser()

        log.debug("Configuring the DirectLinkManager")

        with open(config_file) as stream:
            cls._config.read_file(stream)
            extensions = cls._config.get(cls.source_name,
                                         'AcceptedExtensions')

        cls.accepted_extensions = extensions.split(',')

    @classmethod
    def match_source(cls, url):
        if cls.accepted_extensions is None:
            cls._configure(CONFIG_FILE)

        parsed = urllib.parse.urlparse(url)
        segment = parsed.path[1:].split('?')[0]
        extension = os.path.splitext(segment)[-1]

        if extension:
            matches = extension in cls.accepted_extensions
            log.debug("Source {} a recognized extension".format(
                'matches' if matches else 'does not match'))
            return matches

        msg = "Source lacks an extension and does not match DirectLinkManager"
        log.debug(msg)
        return False

    def downloadables_from_url(self, url):
        clean_url = url.split('?')[0]
        log.debug("Yielding downloadable from URL: {}".format(clean_url))
        yield Downloadable(clean_url)


class ImgurManager(SourceManager):
    source_name = 'imgur'
    _query_url = 'https://api.imgur.com/3/credits'
    _client = None
    _remains = None

    def __init__(self):
        if self._configured is False:
            self._configure(CONFIG_FILE)

        if not self._connected:
            self._connect()

    @classmethod
    def _configure(cls, config_file):
        cls._configured = True

        if cls._config is None:
            cls._config = configparser.ConfigParser()
        else:
            return

        with open(config_file) as stream:
            cls._config.read_file(stream)
            user = cls._config.get(cls.source_name, 'Username', fallback='')
            password = cls._config.get(cls.source_name,
                                       'Password',
                                       fallback='')
        cls.credentials = {'user': user, 'pass': password}

    @classmethod
    def _connect(cls):
        log.debug("Configuring the ImgurManager")
        if cls._client is None:
            cls._client = imgurpython.client.ImgurClient(
                cls.credentials['user'], cls.credentials['pass'])

        if cls._remains is None:
            try:
                results = requests.get(cls._query_url).json()
                cls._remains = {'client': results['data']['ClientRemaining'],
                                'user': results['data']['UserRemaining']}
            except KeyError:
                import ipdb; ipdb.set_trace()

        cls.connected = True

    @classmethod
    def _query_condition(cls, min_limit=1):
        client_remains = cls._remains['client'] > min_limit
        user_remains = cls._remains['user'] > min_limit
        msg = "Imgur quota remains: user->{} client->{}"
        log.debug(msg.format(user_remains, client_remains))
        return client_remains and user_remains

    @classmethod
    def _decrement_query_count(cls):
        cls._remains['client'] -= 1
        cls._remains['user'] -= 1
        msg = "Decremented quota: user->{} client->{}"
        log.debug(msg.format(cls._remains['user'], cls._remains['client']))

    def _id_from_album(self, parsed_url):
        ident = parsed_url.path[3:].split('?')[0]
        msg = "Got album id '{}' from URL: {}"
        log.debug(msg.format(ident, parsed_url.geturl()))
        return ident

    def _id_from_image(self, parsed_url):
        filename = parsed_url.path[1:].split('?')[0]
        ident = os.path.splitext(filename)[0]
        msg = "Got image id '{}' from URL: {}"
        log.debug(msg.format(ident, parsed_url.geturl()))
        return ident

    def _handle_album(self, album_id):
        self._decrement_query_count()

        try:
            album = self._client.get_album_images(album_id)
        except imgurpython.helpers.error.ImgurClientError:
            msg = "There was a problem attempting to get an album with id: {}"
            log.warning(msg.format(album_id))
            return

        for image, count in zip(album, itertools.count(1)):
            log.debug("Yielding Downloadable from URL: {}".format(image.link))
            yield Downloadable(url=image.link,
                               relation_id=album_id,
                               number=count)

    def _handle_image(self, image_id):
        self._decrement_query_count()

        try:
            image = self._client.get_image(image_id)
        except imgurpython.helpers.error.ImgurClientError:
            msg = "There was a problem attempting to get a image with id: {}"
            log.warning(msg.format(image_id))
            return

        log.debug("Yielding Downloadable from URL: {}".format(image.link))
        yield Downloadable(image.link)

    @classmethod
    def match_source(cls, url):
        parsed = urllib.parse.urlparse(url)
        matches = 'imgur.com' in parsed.hostname
        log.debug("Source {} ImgurManager".format(
            'matches' if matches else 'does not match'))
        return matches

    def downloadables_from_url(self, url):
        parsed = urllib.parse.urlparse(url)
        msg = "ImgurManager is managing an {} at URL: {}"

        if parsed.path.startswith('/a/'):
            log.debug(msg.format('album', url))
            album_id = self._id_from_album(parsed)
            yield from self._handle_album(album_id)
        else:
            log.debug(msg.format('image', url))
            image_id = self._id_from_image(parsed)
            yield from self._handle_image(image_id)


class DeviantArtManager(SourceManager):
    _query_url = 'http://backend.deviantart.com/oembed?url={}'

    @classmethod
    def match_source(cls, url):
        parsed = urllib.parse.urlparse(url)
        matches = 'deviantart.com' in parsed.hostname
        log.debug("Source {} DeviantArtManager".format(
            'matches' if matches else 'does not match'))
        return matches

    def downloadables_from_url(self, url):
        encoded = urllib.parse.quote(url, safe="~()*!.'")
        request = requests.get(self._query_url.format(encoded))
        link = request.json().get('url')
        log.debug("Yielding Downloadable from URL: {}".format(link))
        yield Downloadable(link) if link else None

managers = (DirectLinkManager, GfycatManager, ImgurManager, DeviantArtManager)
