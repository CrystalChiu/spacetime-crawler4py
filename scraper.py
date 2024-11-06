import utils.config as config
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import Counter
import nltk
from nltk.corpus import stopwords
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

nltk.download('stopwords')

#NOTE: You need to be connected to UCI vpn

# GLOBALS:
word_counter = Counter()
seen_urls = {} # (URL, time accessed)
longest_page_pair = ("", 0)  # (URL, word count)
subdomain_counts = {}  # (subdomain, unique page count)
most_common_words = [] # stores 50 most common words
cal_page_count = 0 # how many calendar pages we run into in a row

# GLOBAL CONST REFERENCES:
MAX_CAL_PAGES = 0
TRAP_TIME_THRESHOLD = 60 #1 minute
stop_words = set(stopwords.words('english'))

# Takes page to be crawled, determines if status 200 page has no textual content
def is_dead_url(content):
    #TODO: log?
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text().strip()

    return len(text) == 0

# Takes page to be crawled and does prelim check
# Should not parse if page is too large or contributes low information gain
def should_parse(url, resp):
    threshold = (5 * 1024 * 1024) * 3 #1MB to be safe since only crawling text content
    min_text_ratio = 0.015

    # try to get size from header if present
    try:
        if hasattr(resp, 'headers'):
            content_length = resp.headers.get('Content-Length')
            content_type = resp.headers.get('Content-Type', '')

            if content_length is not None:
                content_length = int(content_length)
            if content_length > threshold:
                print(f"Skipping {url} - Content too large: {content_length} bytes")
            return False

            excluded_media_types = [
                'application/pdf', 'image/', 'video/', 'audio/'
            ]
            if any(media_type in content_type for media_type in excluded_media_types):
                print(f"Skipping {url} - Excluded Content-Type: {content_type}")
                return False

        # grab text content
        soup = BeautifulSoup(resp.raw_response.content, "html.parser")
        text_content = soup.get_text(separator=" ").strip()
        word_count = len(text_content.split())

        # calc text to content ratio (text vs markup + text)
        content_size = len(resp.raw_response.content)
        text_ratio = len(text_content) / content_size if content_size > 0 else 0

        # low ratio --> low information gain
        if text_ratio < min_text_ratio:
            print(f"Skipping {url} - Low text_ratio {text_ratio}")
            return False
    except Exception as e:
        print(f"Exception occurred while processing {url}: {e}")
        return False

    return True

#ex: url is "http://www.ics.uci.edu", resp is page itself
# Parses the response, extract information, returns list of urls extracted from page
# TODO: Goal Info
#  1. Unique page count
#  2. page with longest # of words (tokens?)
#  3. 50 most common words (NOT COUNTING STOP WORDS) --> require a LIST ordered by frequency
#  4. list of subdomains ordered alphabetically + # of unique pages detected in each subdomain (displayed: subdomain, number)

def normalize_url(parsed_url):
    # List of query parameters to exclude
    excluded_params = [
        'sessionId',
        'PHPSESSID',
        'JSESSIONID',
        'utm_source',
        'utm_medium',
        'utm_campaign',
        'utm_term',
        'utm_content',
        'gclid',
        'fbclid',
        'ref',
        'tracking_id',
        'referrer',
        'source',
        'entry',
        'index',
        'view',
        'sort',
        'filter',
        'format',
        't',
        'token',
        'lang',
        'locale',
        'calendar',
        'eventDate',
        'eventDisplay',
        'post_type',
        'outlook-ical',
        'ical',
        'comment'
    ]

    query_params = parse_qs(parsed_url.query)
    for param in excluded_params:
        query_params.pop(param, None)

    # rebuild query w/o params
    normalized_query = urlencode(query_params, doseq=True)
    normalized_url = urlunparse(parsed_url._replace(query=normalized_query))

    return normalized_url


def scraper(url, resp):
    global longest_page_pair, word_counter, seen_urls

    try:
        # initially decide if we parse
        if (resp.status != 200
                or is_dead_url(resp.raw_response.content)
                or not should_parse(url, resp)
                or is_calendar_page(url)
        ):
            print(f"\tNote {url} was not parsed")
            return []

        # skip if url has already been seen, otherwise add to set of seen urls
        parsed_url = urlparse(url)
        defragmented_url = urlunparse(parsed_url._replace(fragment=''))
        parsed_defragmented_url = urlparse(defragmented_url)
        normalized_url = normalize_url(parsed_defragmented_url)

        if normalized_url in seen_urls:
            last_seen = seen_urls[normalized_url]
            if time.time() - last_seen < TRAP_TIME_THRESHOLD:
                print(f"Potential infinite trap detected at normalized url {normalized_url} \n\t formerly {url} - Accessed too recently")
            else:
                print(f"Already seen {normalized_url} - Skipping")
            return []
        seen_urls[normalized_url] = time.time() # update dict

        # extract next urls and make sure they're not traps
        valid_links = extract_next_links(url, resp)

        # extract words (text) from page
        content = resp.raw_response.content.decode('utf-8')
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text(separator=' ')
        tokens = text.split()
        num_tokens = len(tokens)

        # update longest page pair
        if num_tokens > longest_page_pair[1]:
            longest_page_pair = (defragmented_url, num_tokens)

        # count unique words excluding stop words
        filtered_tokens = [
            word.lower() for word in tokens
            if word.lower() not in stop_words and len(word) > 1 and re.search(r"[a-zA-Z0-9]{2,}", word)
        ]
        word_counter.update(filtered_tokens)

        # update subdomain info
        subdomain = parsed_url.netloc.split('.')[0]  # Extract subdomain
        if subdomain in subdomain_counts:
            subdomain_counts[subdomain] += 1
        else:
            subdomain_counts[subdomain] = 1

        return valid_links
    except Exception as e:
        print(f"Error while processing URL {url}: {e}")
        return []

# def can_fetch(url):
#     """
#     Check if a URL can be fetched based on the site's robots.txt.
#     """
#     parsed_url = urlparse(url)
#     base_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
#
#     rp = RobotFileParser()
#     rp.set_url(base_url)
#     rp.read()
#
#     user_agent=config.user_agent
#     return rp.can_fetch(user_agent, url)

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    links = []

    # get links from a tags
    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']
        if is_valid(link):
            links.append(link)

    # get redirects from 'meta' tags with 'http-equiv' set to 'refresh'
    for meta_tag in soup.find_all('meta', {'http-equiv': 'refresh'}, content=True):
        content = meta_tag['content']
        if 'url=' in content.lower():
            redirect_url = content.split('url=')[-1].strip()
            if is_valid(redirect_url):
                links.append(redirect_url)

    return links

def is_calendar_page(url):
    parsed = urlparse(url)

    calendar_patterns = [
        r"(?:/day/(?:(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})))",  # /day/yyyy-mm-dd or /day/mm-dd-yyyy
        r"(?:/month/(?:(\d{4}-\d{2})|(\d{2}-\d{4})))",  # /month/yyyy-mm or /month/mm-yyyy
        r"(?:/events/(?:(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})))",  # /events/yyyy-mm-dd or /events/mm-dd-yyyy
        r"(?:=(?:(\d{4}-\d{2}-\d{2})|(\d{2}-\d{2}-\d{4})))",  # =yyyy-mm-dd or =mm-dd-yyyy ( means date is passed as query param)
        r"(?:/(?:(\d{4}-\d{2})|(\d{2}-\d{4})))", # /yyyy-mm or /mm-yyyy might need to stop this
        r"(?<=\?|&)ical=[^&]*", #ical=...
        r"(?<=\?|&)outlook-ical=[^&]*" #outlook-ical=...
    ]

    combined_pattern = '|'.join(calendar_patterns)
    if re.search(combined_pattern, parsed.path) or re.search(combined_pattern, parsed.query):
        return True

    if("ical=" in parsed.query):
        return True

    return False

def is_valid(url):
    global cal_page_count

    # Decide whether to crawl this url or not.
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.

    valid_domains = [
        ".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu",
        ".stat.uci.edu", "today.uci.edu/department/information_computer_sciences"
    ]

    try:
        parsed = urlparse(url)

        if parsed.scheme not in set(["http", "https"]):
            return False

        domain_allowed = any(parsed.netloc.endswith(domain) for domain in valid_domains)
        path_allowed = "today.uci.edu/department/information_computer_sciences" in parsed.netloc + parsed.path
        if not (domain_allowed or path_allowed):
            return False

        path = parsed.path.lower()
        query = parsed.query.lower()
        if (re.search(r"/(?:uploads|files)(?:/|$)", path)
                or "login" in path
                or "action=download" in query
                or "action=login" in query
                or "share=" in query
        ):
            print(f"Skipping {url} due to heuristic match for non-text/invalid content")
            return False

        if is_calendar_page(url):
            print(f"Skipping {url} due to calendar trap")
            return False

        if re.match(
            r".*(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", path):
            return False

        defragmented_url = urlunparse(parsed._replace(fragment=''))
        parsed_defragmented_url = urlparse(defragmented_url)
        normalized_url = normalize_url(parsed_defragmented_url)
        if normalized_url in seen_urls:
            seen_urls[normalized_url]
            print(f"Skipping - Already seen normalized url {normalized_url} from {url}")
            return False

        return True
    except TypeError:
        print("TypeError for ", parsed)
        raise

# Called post crawl to output info collected
def get_summary_info():
    # longest page URL and word count
    url_longest_page, longest_word_count = longest_page_pair

    # most common words
    most_common_words = word_counter.most_common(50)

    # sorted subdomain info alphabetically
    subdomain_info = sorted(subdomain_counts.items())

    # print summary information
    print("Unique pages count:", len(seen_urls))
    print("Longest page's URL:", url_longest_page)
    print("Longest page word count:", longest_word_count)
    print("Most common words:")
    for word, count in most_common_words:
        print(f"  {word}: {count}")
    print("Subdomain info:")
    for subdomain, count in subdomain_info:
        print(f"  {subdomain}: {count}")
