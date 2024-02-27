import gzip
import json
import re

from adblockparser import AdblockRules
from bs4 import BeautifulSoup
from tldextract import extract

from .resources.easylist_selectors import EASYLIST_SELECTORS

# https://gist.github.com/gruber/8891611
# regex_s = r"(?i)\badurl=((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9\.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"

regex_s = r"(?i)\b(adurl|redirect)=((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9\.\-]+[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{};:\'\".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))"


def get_data_l_from_link(href_url):
    # Because of the regex, if found matches, the tuple it returns is (keyword, landing page)
    landing_page_url_l = re.findall(regex_s, href_url)

    # landing_page_url[0] = keyword
    # landing_page_url[1] = landing page
    current_data_l = [
        (href_url, landing_page_url[0], landing_page_url[1])
        for landing_page_url in landing_page_url_l
    ]

    return current_data_l


def check_source_string(ad_block_rules, iframe_key, data_d, depth=0, parent_key=""):
    """Recursively check the source strings"""
    source_s = data_d["source"]
    iframe_d = data_d["iframes"]

    # https://stackoverflow.com/questions/3075550/how-can-i-get-href-links-from-html-using-python
    soup = BeautifulSoup(source_s, "html.parser")

    # print(f'iframe_key = {iframe_key}  parent_key={parent_key}  depth = {depth}' )
    # print(soup)

    # Amplitude 0
    link_data_l = soup.findAll("a", attrs={"href": re.compile("^http?s://")})
    link_l = [link_data.get("href") for link_data in link_data_l]

    # We are getting the iframes content separetely but we are getting them
    # Amplitude 1
    # link_data_l_a_tags = set(soup.findAll('a', attrs={'href': re.compile("^http?s://")}, recursive=True))
    # link_data_l_div_tags = set(soup.findAll('div', attrs={'href': re.compile("^http?s://")}, recursive=True))
    # link_data_l = list(link_data_l_a_tags) + list(link_data_l_div_tags)

    # Amplitude 2
    # link_data_l = list(set(soup.findAll(attrs={'href': re.compile("^http?s://")})))

    # control_page_domain = 'apnews'
    # link_l = []
    # for link_data in link_data_l:
    #     if extract(link_data.get('href')).domain != control_page_domain:
    #         link_l.append(link_data.get('href'))

    # Amplitude 3
    # link_data_l = list(set(soup.findAll(href=re.compile("^http?s://"))))
    # control_page_domain = 'apnews'
    # link_l = []
    # for link_data in link_data_l:
    #     if extract(link_data.get('href')).domain != control_page_domain:
    #         link_l.append(link_data.get('href'))

    # print(link_l)
    # We will test if filtering by page domain lets us filter a little bit all of the URLs so we can focus on the important ones

    # From all the hrefs of <a> tags, collect the ones that would be ads (blocklists)
    ad_link_l = [
        link for link in link_l if ad_block_rules.should_block(link, {"script": False})
    ]
    # print(ad_link_l)
    # Extract all the URLs from adurl or redirect from the ad URLs (links)
    current_data_l_l_tmp = [get_data_l_from_link(link) for link in ad_link_l]
    # Flatten the list
    current_data_l_tmp = [item for sublist in current_data_l_l_tmp for item in sublist]
    # Build the return value in its correct shape
    current_data_l = [
        (iframe_key, href_url, keyword, landing_page_url)
        for (href_url, keyword, landing_page_url) in current_data_l_tmp
    ]

    data_l_l = [
        check_source_string(ad_block_rules, k, v, depth + 1, iframe_key)
        for (k, v) in iframe_d.items()
    ]
    data_l = [item for sublist in data_l_l for item in sublist]

    return current_data_l + data_l


def process(filter_file_path, input_json_gz_path, output_directory_path=""):

    # Open filter lists' paths and parse them
    filter_l_l = [open(path).read().splitlines() for path in filter_file_path]
    filter_l = [item for sublist in filter_l_l for item in sublist]

    ad_block_rules = AdblockRules(filter_l)

    f = gzip.open(input_json_gz_path, "rb")

    data_d = json.load(f)
    # Print the nest level together with the keys
    for k, v in data_d.items():
        print(k)
        print(f"Second level keys within key '{k}':\n")
        for k2, v2 in v.items():
            print(k2)
            for k3, v3 in v2.items():
                print(k3)
                print(v3)
    # with open("example_result.json", "w") as outfile:
    # json.dump(data_d, outfile, indent=4, sort_keys=False)

    # Start recursively checking the source
    data_l = check_source_string(ad_block_rules, "init", data_d)
    data_l_dict = {
        iframe_key: {
            "href_url": href_url,
            "keyword": keyword,
            "landing_page_url": landing_page_url,
        }
        for iframe_key, href_url, keyword, landing_page_url in data_l
    }

    return data_l_dict


# ads = process(
#     ["resources/easylist.txt", "resources/easyprivacy.txt"],
#     "../datadir/style_and_fashion_experiment_accept/sources/8469840698848334-78e8ba0552ec488a919d8caf91b456a5-www.json.gz",
# )
# print(ads)


compressed_file = gzip.open(
    "datadir/ads_screenshots/sources/3569587893305363-3e03db8cd200d3b17328c23606e8ae58-N.json.gz",
    "rb",
)
source_page_dict = json.load(compressed_file)

html_content = source_page_dict["source"]

soup = BeautifulSoup(html_content, "html.parser")

for selector in EASYLIST_SELECTORS:
    print(f"Searching for ads with selector '{selector}'")
    ads = soup.select(selector)
    for ad in ads:
        print(f"Found ad with selector '{selector}':", ad, "\n")
