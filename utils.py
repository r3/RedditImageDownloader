import configparser
import logging
import os
import random
import re
import requests
import shutil
import sys
import tempfile


CONFIG_FILE = 'config.ini'
log = logging


class RequestFailed(Exception):
    pass


class Downloadable():
    config_file = 'imagespider.ini'
    _pattern = re.compile('\W')
    _fuzzy_hashes = {}
    _comparisons_selected = tuple()
    _config = None
    max_name_length = None
    dest_dir = None

    def __init__(self, url, number=None, relation_id=''):
        self.url = url.split('?')[0]
        self.number = str(number).zfill(3) if number is not None else ''
        self.relation_id = self._pattern.sub('', relation_id)
        self._subreddit = None
        self.__safe_filename = None

        if self._config is None:
            self._configure()

    @classmethod
    def _configure(cls):
        log.debug("Configuring Downloadable")
        cls._config = configparser.ConfigParser()
        with open(CONFIG_FILE) as configuration:
            cls._config.read_file(configuration)

        given_destination = cls._config.get('DEFAULT', 'DestinationDirectory')
        cls.dest_dir = os.path.abspath(given_destination)
        cls.max_name_length = int(cls._config.get('DEFAULT', 'MaxNameLength'))
        cls._overwrite = cls._config.getboolean('DEFAULT',
                                                'Overwrite',
                                                fallback=False)
        cls._skip_collisions = cls._config.getboolean('DEFAULT',
                                                      'SkipCollidingNames',
                                                      fallback=False)

    def pull(self):
        try:
            request = make_request(self.url)
        except RequestFailed:
            log.warning("Failed to download from URL: {}".format(self.url))
            return False

        with tempfile.TemporaryDirectory() as temp:
            new_copy = os.path.join(temp, self.safe_filename())
            write_request(request, new_copy)

            local_copy_exists = os.path.exists(self.destination)
            if local_copy_exists and self._skip_collisions:
                log.info("Local copy detected, skipping colliding image")
                return False
            elif local_copy_exists and self._overwrite:
                log.info("Local copy detected, overwriting it")
                old_copy = os.path.join(temp, 'to_delete.tmp')
                shutil.move(self.destination, old_copy)
                shutil.move(new_copy, self.destination)
            elif local_copy_exists:
                log.info("Local copy detected, creating a unique filename")
                self.safe_filename(guarantee_unique=True)
                self.pull()
            else:
                log.debug("Saving image: {}".format(self.destination))
                shutil.move(new_copy, self.destination)

        log.debug("Saving successful")
        return True

    def safe_filename(self, guarantee_unique=False):
        if self.__safe_filename is not None and not guarantee_unique:
            msg = "Returning cached name: {}"
            log.debug(msg.format(self.__safe_filename))
            return self.__safe_filename

        segment, extension = os.path.splitext(self.url)
        extension = extension.lower()
        filename = segment.split('/')[-1]
        if len(filename) > self.max_name_length:
            filename = filename[:self.max_name_length]

        includes = (self.subreddit, self.relation_id, self.number, filename)
        filename = '-'.join(x for x in includes if x).lower()

        if guarantee_unique:
            log.debug("Generating a unique filename for Downloadable")

            proposed_path = os.path.join(self.dest_dir, filename + extension)
            if os.path.exists(proposed_path):
                filename += '-'
            while os.path.exists(proposed_path):
                filename += str(random.randint(0, 9))
                proposed_path = os.path.join(self.dest_dir,
                                             filename + extension)

        msg = "Suggesting name '{}' for: {}"
        log.debug(msg.format(filename + extension, self.url))
        self.__safe_filename = filename + extension
        return self.__safe_filename

    @property
    def destination(self):
        return os.path.join(self.dest_dir, self.safe_filename())

    @property
    def subreddit(self):
        return self._subreddit if self._subreddit is not None else ''

    @subreddit.setter
    def subreddit(self, name):
        self.__safe_filename = None
        no_spaces = name.replace(' ', '_')
        clean_name = no_spaces.lower()
        msg = "Subreddit changed to {}, invalidating cached name"
        log.debug(msg.format(clean_name))
        self._subreddit = clean_name


class HashableSubredditWrapper():
    def __init__(self, subreddit):
        self.wrapped = subreddit

    def __getattr__(self, attrib):
        return getattr(self.wrapped, attrib)

    def __hash__(self):
        return hash(self.wrapped.id)

    def __eq__(self, other):
        return self.wrapped.id == other.wrapped.id


def get_logger(name=__name__, level=log.ERROR):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stdout)
    format_str = '%(asctime)s - %(levelname)s - %(message)s (in:%(funcName)s)'
    formatter = logging.Formatter(format_str)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def get_config(path):
    config = configparser.ConfigParser()
    with open(path) as stream:
        config.read_file(stream)
    return config


def make_request(url):
    log.debug("Requesting URL: {}".format(url))
    try:
        request = requests.get(url)
    except requests.exceptions.ConnectionError:
        msg = "Failed to connect to URL: {}".format(url)
        log.error(msg)
        raise RequestFailed(msg)
    except requests.exceptions.ReadTimeout:
        msg = "Failed to connect to URL: {}".format(url)
        log.error(msg)
        raise RequestFailed(msg)

    if request.status_code != 200:
        msg = "Request failed with status: {}"
        raise RequestFailed(msg.format(request.status_code))

    log.debug("Request successful")
    return request


def write_request(request, destination, chunk_size=1024):
    log.debug("Writing to '{}'".format(destination))
    with open(destination, 'wb') as stream:
        for chunk in request.iter_content(chunk_size):
            stream.write(chunk)
    log.debug("Writing successful")
