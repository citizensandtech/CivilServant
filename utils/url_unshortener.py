import requests
from requests.exceptions import InvalidURL
from requests_futures.sessions import FuturesSession
from concurrent.futures import wait


def bulkUnshorten(urls, workers=20):
    # This function will unshorten an array of shortened URLS
    # The second optional argument is the number of workers to run in parallel

    # When initially called, an array of string objects will be passed to the function.
    # The function will then create a dictionary to keep track of all urls, the number of hops and
    # the final destination url.  If there is an error, a status code of 4xx is recorded within the dict.
    # Otherwise, a status code of 200 should be returned.

    # Global timeouts
    # - REQUEST_TIMEOUT is the timeout when waiting for a reply from a remote server
    # - HOPS_LIMIT is the maximum number of redirect hops allowed

    REQUEST_TIMEOUT = 3
    HOPS_LIMIT = 3

    # Allow passing in of one url as a string object
    if (isinstance(urls, str)):
        urls = [urls]

    # If method is being called initally, create a dictionary for the urls passed.  When the method calls
    # itself, it will pass this object to itself as needed.
    if (isinstance(urls, list)):
        url_objects = urls[:]
        urls = {}
        for url in url_objects:
            try:
                req = requests.Request('HEAD', url)
                normalized_url = req.prepare().url
                urls[normalized_url] = {"hops": 0, "status_code": None, "success": None, "final_url": None, "error": None,
                                        "original_url": url}
            except InvalidURL: # there are no guarantees in thi world.
                pass # this url won't be coming along for the ride.
        # print('there are {} urls at the beggining'.format(len(urls)))

    while True:
        # print('there are {} urls in the middle'.format(len(urls)))

        session = FuturesSession(max_workers=workers)
        futures = []

        for key in urls:
            if urls[key]['success'] is not None: continue
            if urls[key]['hops'] >= HOPS_LIMIT: continue
            futures.append(session.head(key, timeout=REQUEST_TIMEOUT))

        if futures:
            done, incomplete = wait(futures)
            for obj in done:
                try:
                    result = obj.result()
                except requests.exceptions.ConnectTimeout as e:
                    url = e.request.url
                    urls[url]['error'] = "ConnectTimeout"
                    urls[url]['success'] = False
                    continue
                except requests.exceptions.ReadTimeout as e:
                    url = e.request.url
                    urls[url]['error'] = "ReadTimeout"
                    urls[url]['success'] = False
                    continue
                except requests.exceptions.ConnectionError as e:
                    url = e.request.url
                    urls[url]['error'] = "ConnectionError"
                    urls[url]['success'] = False
                    continue


                if result.status_code == 200:
                    urls[result.url]['success'] = True
                    urls[result.url]['final_url'] = result.url
                    urls[result.url]['status_code'] = result.status_code
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

                        urls[result.url]['hops'] += 1
                        urls[result.url]['final_url'] = redirect_url
                        urls[result.url]['status_code'] = result.status_code
                        # print('would pop')
                        # print('redirect url is: {}, result.url is {}'.format(redirect_url, result.url))
                        # urls[redirect_url] = urls.pop(result.url)
                    except KeyError: #no location to find
                        urls[result.url]['error'] = "BadRedirect"
                        urls[result.url]['success'] = False
                    except InvalidURL:
                        urls[result.url]['error'] = "InvalidURL"
                        urls[result.url]['success'] = False

                else:
                    urls[result.url]['success'] = False
                    urls[result.url]['status_code'] = result.status_code
                    # log.info('bad redirect found in {}'.format(result))
        else:

            # print('there are {} urls at the end'.format(len(urls)))
            url_dict = {}

            for key in urls:
                original_url = urls[key]['original_url']
                url_dict[original_url] = urls[key]

            return url_dict
