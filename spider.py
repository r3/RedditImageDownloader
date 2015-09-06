#!/usr/bin/env python3

"""
spider.py

This program parses a list of subreddits and downloads any highly rated images

Reads 'subs.lst' which should be a text file with one sub per line. The subs
should be written as only a name. The sub, 'http://reddit.com/r/cute' would be
written simply as 'cute' with a newline following. A sample list shoule be
included as 'subs.lst.example'. The images will be dumped to a single flat
directory, and will be named based on the reddit submission title.
"""

import functools
import logging
import sys

import praw
import requests

import source_managers
import utils


__author__ = 'Ryan Roler'
__copyright__ = 'Copyright 2015, Ryan Roler'
__credits__ = ['Ryan Roler', ]
__license__ = 'GPL'
__maintainer__ = 'Ryan Roler'
__email__ = '@'.join(('ryan.roler', 'gmail.com'))
__status__ = 'Prototype'


APP_NAME = 'imagespider'
CONFIG_PATH = 'config.ini'
REDDIT = praw.Reddit(user_agent=APP_NAME)

log = logging


@functools.lru_cache(maxsize=100)
def _get_highest_score_from_subreddit(hashable_subreddit):
    sub = hashable_subreddit
    err = "There was a problem querying the API, assuming score is too low"

    try:
        top_scoring_submission = next(sub.get_top_from_all(limit=1))
        score = top_scoring_submission.score
    except praw.errors.HTTPException:
        log.error(err)
        return False
    except praw.errors.InvalidSubreddit:
        log.error(err)
        return False
    except requests.exceptions.ReadTimeout:
        log.error(err)
        return False
    except requests.exceptions.ConnectionError:
        log.error(err)
        return False

    msg = "Highest score in sub, '{}' is {}"
    log.debug(msg.format(sub.display_name, score))
    return score


def _absolute_comparator(submission, minimum):
    return submission.score > minimum


def _relative_comparator(submission, minimum):
    hashed_subreddit = utils.HashableSubredditWrapper(submission.subreddit)
    top_score = _get_highest_score_from_subreddit(hashed_subreddit)

    relative_score = submission.score / top_score * 100
    msg = "Relative score is {}, highest score in sub is {}"
    log.debug(msg.format(relative_score, top_score))
    return relative_score > minimum


@functools.lru_cache(maxsize=100)
def _get_fetched_subreddit(name):
    subreddit = REDDIT.get_subreddit(name)

    try:
        disp_name = subreddit.display_name  # resolve lazy object
        log.debug("Fetched subreddit: {}".format(disp_name))
        return subreddit
    except praw.errors.HTTPException:
        msg = "Couldn't query sub: {}"
    except praw.errors.InvalidSubreddit:
        msg = "Queried subreddit, '{},' does not exist, skipping it"
    except requests.exceptions.ReadTimeout:
        msg = "Timed out querying sub: {}"
    except requests.exceptions.ConnectionError:
        msg = "Connection reset or aborted by peer while querying sub: {}"

    msg = msg.format(name)
    log.warning(msg)
    raise utils.RequestFailed(msg)


def _get_submission_generator(subreddit, func_name, limit):
    func = getattr(subreddit, func_name)

    try:
        msg = "Querying generator of submissions for subreddit: {}"
        log.debug(msg.format(subreddit.display_name))
        return func(limit=limit)
    except praw.errors.HTTPException:
        msg = "Couldn't query a sub, skipping it"
    except requests.exceptions.ReadTimeout:
        msg = "Timed out querying a sub, skipping it"
    except requests.exceptions.ConnectionError:
        msg = "Connection reset or aborted by peer"

    log.warning(msg)
    raise utils.RequestFailed(msg)


def _get_submissions_from_subreddit(subreddit, func_name, limit):
    try:
        submissions = _get_submission_generator(subreddit, func_name, limit)
    except utils.RequestFailed:
        return

    while True:
        try:
            submission = next(submissions)
            msg = "Working on sub '{}' processing: {}"
            log.info(msg.format(submission.subreddit, submission.title))
            yield submission
        except StopIteration:
            log.debug("Exhausted submissions from subreddit query")
            break
        except praw.errors.HTTPException:
            log.error("Couldn't query a submission, skipping it")
            continue
        except requests.exceptions.ReadTimeout:
            log.error("Timed out querying a submission, skipping it")
            continue
        except requests.exceptions.ConnectionError:
            log.error("Connection reset or aborted by peer, giving up on sub")
            break


def _get_sub_list(path):
    with open(path) as sub_list:
        for sub in (x.strip() for x in sub_list if not x.startswith('#')):
            yield sub


def submissions_from_subreddit(subreddit_name,
                               #func_name='get_top_from_day',
                               func_name='get_top_from_all',
                               limit_per_sub=40):
    try:
        subreddit = _get_fetched_subreddit(subreddit_name)
    except utils.RequestFailed:
        log.error("Failed to get subreddit")
        return

    yield from _get_submissions_from_subreddit(subreddit,
                                               func_name,
                                               limit_per_sub)


def downloadables_from_submission(submission):
    """Yields any image URLs found at a given reddit submission

    Imgur is parsed specially to download any images in albums
    found at the given URL.  If the link is direct to an image, the passed
    URL will be yielded. If no images are found at the link, yields None.
    """
    for manager in source_managers.managers:
        if manager.match_source(submission.url):
            instance = manager()
            msg = "Manager '{}' matches URL: {}"
            log.debug(msg.format(instance.source_name, submission.url))
            yield from instance.downloadables_from_url(submission.url)
            return


if __name__ == '__main__':
    config = utils.get_config(CONFIG_PATH)
    levels = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL}
    level_from_config = config.get('DEFAULT', 'LogLevel', fallback='error')
    selected_level = levels[level_from_config]
    log = utils.get_logger('main', selected_level)
    utils.log = utils.get_logger('utils', selected_level)
    source_managers.log = utils.get_logger('source_managers', selected_level)

    sub_list_path = config.get('DEFAULT', 'SubList')
    score_minimum = config.getint('DEFAULT',
                                  'MinimumScore',
                                  fallback=0)
    score_is_relative = config.getboolean('DEFAULT',
                                          'RelativeScore',
                                          fallback=False)

    if score_is_relative:
        score_is_sufficient = _relative_comparator
    else:
        score_is_sufficient = _absolute_comparator

    for subreddit_name in _get_sub_list(sub_list_path):
        for submission in submissions_from_subreddit(subreddit_name):
            if not score_is_sufficient(submission, score_minimum):
                msg = ("Insufficient score on submission, skipping "
                       "submission '{}' and all remaining submissions "
                       "in subreddit: {}")
                log.info(msg.format(submission.title, subreddit_name))
                break

            downloadables = downloadables_from_submission(submission)
            for downloadable in downloadables:
                log.info("Downloading from URL: {}".format(downloadable.url))
                downloadable.subreddit = submission.subreddit.display_name
                downloadable.pull()

    try:
        log.info("Done processing subreddits")
        sys.exit()
    except ResourceWarning:
        pass
