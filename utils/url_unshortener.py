import inspect
import os

import requests
from requests.exceptions import InvalidURL, MissingSchema
from requests_futures.sessions import FuturesSession
from concurrent.futures import wait
import app.cs_logger

ENV = os.environ['CS_ENV']
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), "..")
log = app.cs_logger.get_logger(ENV, BASE_DIR)


def bulkUnshorten(urls, workers=20, REQUEST_TIMEOUT=10, HOPS_LIMIT=5):
    """ Slightly less performant version that will keep input and output dimensions 1-1.
        :param urls urls to follow redirects
        :param workers number of concurrent futures to pursue
        :param REQUEST_TIMEOUT  number of seconds to wait for a url to return a status code
        :param HOPS_LIMIT  maximum number of 30x's to follow

        :returns [{url_dict}] list of dictionaries containing the url unshortening results.

        When initially called, an array of string objects will be passed to the function.
        If there is an error, a status code of 4xx is recorded within the dict.
        Otherwise, a status code of 200 should be returned.

        Data structure is list of URLdicts
        URLdict is {'curr_url', 'hops', 'status_code', 'success', 'error', 'final_url', 'original_url'}"""

    def setErrorOnUrls(urls, error_url, err_msg, status_code=None):
        """utility function to set urls"""
        return_urls = []
        for url_dict in urls:
            if url_dict['curr_url'] == error_url:
                url_dict['error'] = err_msg
                url_dict['success'] = False
                if status_code:
                    url_dict['status_code'] = status_code
            return_urls.append(url_dict)
        return return_urls

    log.info("Bulk Unshorten 2 called with {len_urls} urls. Num workers specified: {workers}".format(len_urls=len(urls), workers=workers))
    # log.info("Hops limit is {HOPS_LIMIT} and request Timeout Seconds = {REQUEST_TIMEOUT}".format(HOPS_LIMIT=HOPS_LIMIT, REQUEST_TIMEOUT=REQUEST_TIMEOUT))

    # Allow passing in of one url as a string object
    if (isinstance(urls, str)):
        urls = [urls]

    # If method is being called initally, create a dictionary for the urls passed.  When the method calls
    # itself, it will pass this object to itself as needed.
    if (isinstance(urls, list)):
        url_objects = urls[:]
        urls = []
        for url in url_objects:
            url_dict = {'original_url': url, "final_url": None, "hops": 0, 'status_code': None, "success": None,
                        "error": None}
            try:
                req = requests.Request('HEAD', url)
                normalized_url = req.prepare().url
                url_dict["curr_url"] = normalized_url
            except InvalidURL as e:  # there are no guarantees in this world.
                log.info("Error Unshortening: InvalidURL on {url}. Error {e}".format(url=url, e=e))
                url_dict['success'] = False
            except MissingSchema as e:
                log.info("Error Unshortening: MissingSchema on {url}. Error {e}".format(url=url, e=e))
                url_dict['success'] = False
            finally:
                urls.append(url_dict)
        # log.debug('Starting URLS before unshortening are: {urls}'.format(urls=urls))

    while True:

        session = FuturesSession(max_workers=workers)
        futures = []

        for url_dict in urls:
            if url_dict['success'] is not None: continue
            if url_dict['hops'] >= HOPS_LIMIT: continue
            key = url_dict['curr_url']
            futures.append(session.head(key, timeout=REQUEST_TIMEOUT))

        if futures:
            done, incomplete = wait(futures)
            # log.debug("done:{0} incomplete:{1}".format(len(done), len(incomplete)))
            for obj in done:
                try:
                    result = obj.result()
                except requests.exceptions.ConnectTimeout as e:
                    urls = setErrorOnUrls(urls, e.request.url, "ConnectTimeout")
                    continue
                except requests.exceptions.ReadTimeout as e:
                    urls = setErrorOnUrls(urls, e.request.url, "ReadTimeout")
                    continue
                except requests.exceptions.ConnectionError as e:
                    urls = setErrorOnUrls(urls, e.request.url, "ReadTimeout")
                    continue
                except UnicodeDecodeError as e:
                    log.error('Unicode error: {} for obj:'.format(e.object))
                    continue

                if result.status_code == 200:
                    for url_dict in urls:
                        if url_dict['curr_url'] == result.url:
                            url_dict['success'] = True
                            url_dict['final_url'] = result.url
                            url_dict['status_code'] = result.status_code
                elif result.status_code == 301 or result.status_code == 302:
                    try:
                        redirect_url = result.headers['location']

                        # Handle a location header that returns a relative path instead of an absolute path.  This is now allowed
                        # under RFC 7231.  If the returned location does not begin with http, then it is a relative path and should
                        # be concatenated to the original url

                        if not redirect_url.lower().startswith("http"):
                            redirect_url = result.url + redirect_url

                        # Normalize the url using the requests module
                        req = requests.Request('HEAD', redirect_url)
                        redirect_url = req.prepare().url
                        for url_dict in urls:
                            if url_dict['curr_url'] == result.url:
                                url_dict['hops'] += 1
                                url_dict['final_url'] = redirect_url
                                url_dict['status_code'] = result.status_code
                                url_dict['curr_url'] = redirect_url
                    except KeyError:  # no location to find
                        urls = setErrorOnUrls(urls, result.url, 'BadRedirect')
                    except InvalidURL:
                        urls = setErrorOnUrls(urls, result.url, 'InvalidURL')
                else:
                    urls = setErrorOnUrls(urls, result.url, err_msg=result.status_code, status_code=result.status_code)
            session.close()
        else:
            return urls


# def bulkUnshortenLegacy(urls, workers=20):
#     """
#     Slightly more performant version, but will collapse output dimension based on a dict of the final URLs.
#     This function will unshorten an array of shortened URLS
#         The second optional argument is the number of workers to run in parallel
#
#         When initially called, an array of string objects will be passed to the function.
#         The function will then create a dictionary to keep track of all urls, the number of hops and
#         the final destination url.  If there is an error, a status code of 4xx is recorded within the dict.
#         Otherwise, a status code of 200 should be returned.
#
#         Global timeouts
#         - REQUEST_TIMEOUT is the timeout when waiting for a reply from a remote server
#         - HOPS_LIMIT is the maximum number of redirect hops allowed
#     """
#     REQUEST_TIMEOUT = 10
#     HOPS_LIMIT = 5
#     log.debug(f"Bulk Unshorten called with {len(urls)} urls. Num workers specified: {workers}")
#     log.debug(f"Hops limit is {HOPS_LIMIT} and request Timeout Seconds = {REQUEST_TIMEOUT}")
#
#     # Allow passing in of one url as a string object
#     if (isinstance(urls, str)):
#         urls = [urls]
#
#     # If method is being called initally, create a dictionary for the urls passed.  When the method calls
#     # itself, it will pass this object to itself as needed.
#     if (isinstance(urls, list)):
#         url_objects = urls[:]
#         urls = {}
#         for url in url_objects:
#             try:
#                 req = requests.Request('HEAD', url)
#                 normalized_url = req.prepare().url
#                 urls[normalized_url] = {"hops": 0, "status_code": None, "success": None, "final_url": None,
#                                         "error": None,
#                                         "original_url": url}
#             except InvalidURL as e:  # there are no guarantees in this world.
#                 log.error(f"Error Unshortening: InvalidURL on {url}. Error {e}")
#                 url['success'] = False
#             except MissingSchema as e:
#                 log.error(f"Error Unshortening: MissingSchema on {url}. Error {e}")
#                 url['success'] = False
#         log.debug(f'Starting URLS before unshortening are: {urls}')
#
#     while True:
#         # print('there are {} urls in the middle'.format(len(urls)))
#
#         session = FuturesSession(max_workers=workers)
#         futures = []
#
#         for key in urls:
#             if urls[key]['success'] is not None: continue
#             if urls[key]['hops'] >= HOPS_LIMIT: continue
#             futures.append(session.head(key, timeout=REQUEST_TIMEOUT))
#
#         if futures:
#             done, incomplete = wait(futures)
#             for obj in done:
#                 try:
#                     result = obj.result()
#                 except requests.exceptions.ConnectTimeout as e:
#                     url = e.request.url
#                     urls[url]['error'] = "ConnectTimeout"
#                     urls[url]['success'] = False
#                     continue
#                 except requests.exceptions.ReadTimeout as e:
#                     url = e.request.url
#                     urls[url]['error'] = "ReadTimeout"
#                     urls[url]['success'] = False
#                     continue
#                 except requests.exceptions.ConnectionError as e:
#                     url = e.request.url
#                     urls[url]['error'] = "ConnectionError"
#                     urls[url]['success'] = False
#                     continue
#                 except UnicodeDecodeError as e:
#                     log.error('Unicode error: {} for obj:'.format(e.object))
#                     continue
#
#                 if result.status_code == 200:
#                     urls[result.url]['success'] = True
#                     urls[result.url]['final_url'] = result.url
#                     urls[result.url]['status_code'] = result.status_code
#                 elif result.status_code == 301 or result.status_code == 302:
#                     try:
#                         redirect_url = result.headers['location']
#
#                         # Handle a location header that returns a relative path instead of an absolute path.  This is now allowed
#                         # under RFC 7231.  If the returned location does not begin with http, then it is a relative path and should
#                         # be concatenated to the original url
#
#                         if not redirect_url.lower().startswith("http"):
#                             redirect_url = result.url + redirect_url
#
#                         # Normalize the url using the requests module
#                         req = requests.Request('HEAD', redirect_url)
#                         redirect_url = req.prepare().url
#
#                         urls[result.url]['hops'] += 1
#                         urls[result.url]['final_url'] = redirect_url
#                         urls[result.url]['status_code'] = result.status_code
#                         # Troublesome line that was collapsing the output dict by the URLS that had the same final destination
#                         urls[redirect_url] = urls.pop(result.url)
#                     except KeyError:  # no location to find
#                         urls[result.url]['error'] = "BadRedirect"
#                         urls[result.url]['success'] = False
#                     except InvalidURL:
#                         urls[result.url]['error'] = "InvalidURL"
#                         urls[result.url]['success'] = False
#                 else:
#                     urls[result.url]['success'] = False
#                     urls[result.url]['status_code'] = result.status_code
#         else:
#
#             # print('there are {} urls at the end'.format(len(urls)))
#             url_dict = {}
#
#             for key in urls:
#                 original_url = urls[key]['original_url']
#                 url_dict[original_url] = urls[key]
#
#             return url_dict
