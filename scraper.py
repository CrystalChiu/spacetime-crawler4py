import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import Counter
import nltk
from nltk.corpus import stopwords
import time

nltk.download('stopwords')

#NOTE: You need to be connected to UCI vpn

# GLOBALS:
discovery_timestamps = {} # (domain Name, time accessed)
word_counter = Counter()
seen_urls = set()
longest_page_pair = ("", 0)  # (URL, word_count)
subdomain_counts = {}  # (subdomain, unique page count)
most_common_words = [] # stores 50 most common words

# GLOBAL CONST REFERENCES:
stop_words = set(stopwords.words('english'))
trap_patterns = [
    r"(?:[?&]page=\d+)",
    r"(?:[?&]date=)",
    r"(?:/calendar/)",
    r"(?:session_id|sessionid=)",
    r"(?:/search\?)"
]

#TODO:
# 1. (OK) Crawl all pages with high textual information content (i think it just means pages with a lot of text...)
# 2. (TD) Detect and avoid infinite traps (what patterns?)
# 3. (TD) Detect and avoid sets of similar pages with no information (near duplicate? but what about pages that have one irrelevant sentence...?)
# 4. (OK) Detect and avoid dead URLs that return a 200 status but no data (check status code and text length)
# 5. (TT) Scraper func
# Detect and avoid crawling very large files, especially if they have low information value (need to find threshold for "very large", low information value = mostly templating?)
# AVOID penalties for crawling useless families of pages
# NOTE: you must decide on a reasonable definition for a low information value page

# Input: page being crawled, Output: Boolean if there is near duplicate page already crawled
def has_near_duplicate(content):
    # use token intersection?
    # we can define a 90% threshold
    # perhaps we tokenize each file we input?
    # do we have space for dat??
    return False

# Takespage to be crawled, recognizes URL patterns to determine if crawler is stuck in infinite trap
def is_infinite_trap(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc

    # need to keep track of WHEN we discover each domain
    if domain not in discovery_timestamps:
        discovery_timestamps[domain] = []

    if any(re.search(pattern, url) for pattern in trap_patterns):
        print(f"Potential infinite trap detected at domain {domain} based on URL")
        return True

    # check rate of discovery
    # too many in short time with similar url --> trap
    # FIXME: adjust threshold based on experimentation
    DISCOVERY_RATE_THRESHOLD = 50 #urls/min
    DISCOVERY_TIME_THRESHOLD = 60 #seconds

    current_time = time.time()
    discovery_timestamps[domain].append(current_time)
    discovery_timestamps[domain] = [
        timestamp for timestamp in discovery_timestamps[domain]
        if current_time - timestamp <= DISCOVERY_TIME_THRESHOLD
    ]

    if len(discovery_timestamps[domain]) > DISCOVERY_RATE_THRESHOLD:
        print(f"Potential infinite trap detected at domain {domain} based on discovery time")
        return True

    return False

# Takes page to be crawled, determines if status 200 page has no textual content
def is_dead_url(content):
    soup = BeautifulSoup(content, "html.parser")
    text = soup.get_text().strip()

    return len(text) == 0

# Takes page to be crawled and does prelim check
# Should not parse if page is too large or contributes low information gain
def should_parse(resp):
    threshold = 5 * 1024 * 1024 #1MB to be safe since only crawling text content
    min_text_ratio = 0.1

    # try to get size from header if present
    content_length = resp.headers.get('Content-Length', None)
    if content_length and int(content_length) > threshold:
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
        return False

    return True

#ex: url is "http://www.ics.uci.edu", resp is page itself
# Parses the response, extract information, returns list of urls extracted from page
# TODO: Goal Info
#  1. Unique page count
#  2. page with longest # of words (tokens?)
#  3. 50 most common words (NOT COUNTING STOP WORDS) --> require a LIST ordered by frequency
#  4. list of subdomains ordered alphabetically + # of unique pages detected in each subdomain (displayed: subdomain, number)

def scraper(url, resp):
    global longest_page_pair, word_counter, seen_urls

    # TODO: Custom PQ with (URL, # words)
    # TODO: Custom PQ with (Subdomain, # unique pages) use a COMPARATOR
    #  - Im not sure when we want to count the most common words...

    # initially decide if we parse
    if (resp.status != 200
        or is_dead_url(resp.raw_response.content)
        or not should_parse(resp)
    ):
        return []

    # skip if url has already been parsed, otherwise add to set of seen urls
    parsed_url = urlparse(url)
    defragmented_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

    if defragmented_url in seen_urls:
        return []
    seen_urls.add(defragmented_url)

    # extract next urls
    # check first if they're traps
    links = extract_next_links(url, resp)
    valid_links = [link for link in links if is_valid(link) and not is_infinite_trap(link)]

    # extract words from page
    content = resp.raw_response.content.decode('utf-8')  # Assuming the content is UTF-8 encoded
    tokens = content.split()
    num_tokens = len(tokens)

    # update longest page pair
    if num_tokens > longest_page_pair[1]:  # compare with the current longest word count
        longest_page_pair = (defragmented_url, num_tokens)

    # count unique words excluding stop words
    filtered_tokens = [word.lower() for word in tokens if word.lower() not in stop_words]
    word_counter.update(filtered_tokens)

    # update subdomain info
    subdomain = parsed_url.netloc.split('.')[0]  # Extract subdomain
    if subdomain in subdomain_counts:
        subdomain_counts[subdomain] += 1
    else:
        subdomain_counts[subdomain] = 1

    return valid_links

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

    for a_tag in soup.find_all('a', href=True):
        link = a_tag['href']

        if is_valid(link):
            links.append(link)

    return links

def is_valid(url):
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

        return not re.match(
                r".*\.(css|js|bmp|gif|jpe?g|ico"
                + r"|png|tiff?|mid|mp2|mp3|mp4"
                + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
                + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
                + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
                + r"|epub|dll|cnf|tgz|sha1"
                + r"|thmx|mso|arff|rtf|jar|csv"
                + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())
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

    return {
        "unique_pages_count": len(seen_urls),
        "longest page's url": url_longest_page,
        "longest_page_word_count": longest_word_count,
        "most_common_words": most_common_words,
        "subdomain_info": [(subdomain, count) for subdomain, count in subdomain_info]
    }