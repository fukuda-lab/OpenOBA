import json
import gzip
from bs4 import BeautifulSoup
from urllib.parse import urlparse


def read_json_file(file_path):
    """Load and return the content of the JSON file, handling .gz files automatically."""
    if file_path.endswith(".gz"):
        with gzip.open(file_path, "rt", encoding="utf-8") as file:
            return json.load(file)
    else:
        with open(file_path, "r", encoding="utf-8") as file:
            return json.load(file)


def is_advertisement_url(url):
    """Determine if a URL is likely an advertisement based on domain or patterns."""
    ad_domains = [
        "doubleclick.net",
        "googlesyndication.com",
        "taboola.com",
        "amazon-adsystem.com",
    ]
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    if any(ad_domain in domain for ad_domain in ad_domains):
        return True
    # Add more sophisticated checks here if necessary
    return False


def extract_ad_urls_from_html(html_content):
    """Parse HTML content using BeautifulSoup and extract URLs likely to be ads."""
    soup = BeautifulSoup(html_content, "html.parser")
    ad_urls = []

    for link in soup.find_all("a", href=True):
        if is_advertisement_url(link["href"]):
            ad_urls.append(link["href"])

    for iframe in soup.find_all("iframe", src=True):
        if is_advertisement_url(iframe["src"]):
            ad_urls.append(iframe["src"])

    return ad_urls


def analyze_document(document, doc_type="Page"):
    """Analyze a single document/page or iframe and extract ad URLs."""
    print(f"\nAnalyzing {doc_type}: {document.get('doc_url', 'No URL')}")
    source = document.get("source", "")
    if source:
        ad_urls = extract_ad_urls_from_html(source)
        if ad_urls:
            print(f"Extracted Advertisement URLs ({len(ad_urls)}):")
            for url in ad_urls:
                print(f" - {url}")
        else:
            print("No advertisement URLs found.")
    else:
        print("No source found.")


def analyze_iframes(iframes):
    """Recursively analyze nested iframes."""
    for iframe_id, iframe_content in iframes.items():
        analyze_document(iframe_content, doc_type="Iframe")
        nested_iframes = iframe_content.get("iframes", {})
        if nested_iframes:
            analyze_iframes(nested_iframes)


def main():
    json_file_path = "../datadir/style_and_fashion_experiment_accept/sources/8469840698848334-78e8ba0552ec488a919d8caf91b456a5-www.json.gz"  # Update this to your JSON file's path
    json_data = read_json_file(json_file_path)

    # Analyze the main page
    analyze_document(json_data)

    # Recursively analyze iframes
    iframes = json_data.get("iframes", {})
    if iframes:
        analyze_iframes(iframes)


if __name__ == "__main__":
    main()
