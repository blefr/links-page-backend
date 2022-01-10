import csv
from os import supports_follow_symlinks
import re
import ssl
import feedparser
import requests
import gspread

from urllib.parse import urlparse
from collections import Counter
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials

from prefect import task, Flow
from prefect.schedules import CronSchedule
from prefect.storage import GitHub
from prefect.run_configs import LocalRun
from prefect.client import Secret
from prefect.tasks.secrets import PrefectSecret

### RSS FEED LINK
RSS = "https://www.blef.fr/datanews/xml/"

### USER AGENT
HEADERS = {"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36"}

### GOOGLE SHEET CREDITENTIALS
SHEET_NAME = "blef_links"
SHEET_ID = "22868124"
CSV_FILE_NAME = "./links.csv"

### GITHUB TOKEN
GITHUB_TOKEN = "GITHUB_TOKEN"

### KEYWORDS
DATA_FUNDRAISING = ["data fundraising"]
DATA_MESH = ["data mesh"]
DATA_WAREHOUSE = ["data warehouse", "data lake", "lake", "warehouse"]
DATA_MANAGMENT = ["data managment", "data organisation", "managment", "organization"]
ELT_ETL = ["etl / elt", "etl", "elt", "airflow", "dbt"]
MODERN_DATA_STACK = ["modern data stack", "data stack", "stack"]
DATA_ANALYTICS = [
    "data analytics",
    "dataviz",
    "data vizualization",
    "bi",
    "looker",
    "tableau",
]
DATA_MONITORING = ["data monitoring", "monitoring", "data quality"]
IA = [
    "ia",
    "artificial intelligence",
    "machine learning",
    "neural networks",
    "deep learning",
]
ALL_CATEGORIES = (
    DATA_MESH,
    DATA_WAREHOUSE,
    DATA_MANAGMENT,
    ELT_ETL,
    MODERN_DATA_STACK,
    DATA_ANALYTICS,
    DATA_MONITORING,
    IA,
)

### LINKS
ALL_LINKS = []
LINKS_CATEGORISED = []


def find_most_frequent_domains(links):
    """
    finds most frequent domains in a list of links

    :param links: str list

    returns dict
    """
    domains = []
    for link in links:
        domain = urlparse(link).netloc
        domains.append(domain)
    most_frequent_domains = {k: v for k, v in Counter(domains).most_common(500)}
    return most_frequent_domains


def filter_link(link):
    """
    checks if str contains blacklisted words

    :param link: str

    returns bool
    """
    if (
        "unsplash" not in link
        and "blef.fr" not in link
        and "mailto" not in link
        and "www.google.com" not in link
    ):
        return True
    else:
        return False


def find_properties(link, category, newsletter_title, page):
    """
    finds published time, article title and article description

    :param link: str
    :param category: str
    :param newsletter_title: str
    :param page: BeautifulSoup

    returns a list of values (final list to add to google sheet)
    """
    title = ""
    published_time = "0"
    description = ""
    time = page.find("meta", {"property": "article:published_time"})
    desc = page.find("meta", {"name": "description"})
    if page.title:
        title = page.title.text.split("|", 1)[0]
    if time:
        published_time = str(time["content"][:10])
    if desc:
        try:
            description = desc["content"]
        except KeyError:
            pass
    return [f"{link} ", category, newsletter_title, published_time, title, description]


def get_links(entry):
    """
    either categorises a link directly or appends it to an array for later categorisation

    :param entry: FeedParserDict element
    """
    link_pattern = 'href=".*?(?="|$)'
    data_fundraising_pattern = "(?i)Data fundraising.*?(?=<h2|$)"
    match_link = re.findall(link_pattern, entry.content[0].value)
    match_datafund = re.findall(data_fundraising_pattern, entry.content[0].value)
    if match_datafund:
        links_datafound = re.findall(link_pattern, match_datafund[0])
        for l in links_datafound:
            n = l.replace('href="', "")
            if filter_link(n):
                html_text = requests.get(n, headers=HEADERS).text
                page = BeautifulSoup(html_text, "html.parser")
                LINKS_CATEGORISED.append(
                    find_properties(n, DATA_FUNDRAISING[0], entry.title, page)
                )
    if match_link:
        for m in match_link:
            n = m.replace('href="', "")
            if filter_link(n):
                if match_datafund:
                    if n not in match_datafund[0]:
                        ALL_LINKS.append([n, entry.title])
                else:
                    ALL_LINKS.append([n, entry.title])


@task
def get_blef_rss(rss):
    """
    gets all entries from rss feed

    :param rss: rss
    """
    if hasattr(ssl, "_create_unverified_context"):
        ssl._create_default_https_context = ssl._create_unverified_context
    feed = feedparser.parse(rss)
    for entry in feed.entries:
        if entry.has_key("content"):
            get_links(entry)
    print("total links : ", len(ALL_LINKS))
    return ALL_LINKS


def find_title_categories(title):
    """
    finds keywords in title param

    :param title: str

    returns list of categories
    """
    title_categories = []
    for arr in ALL_CATEGORIES:
        if any(keyword in title.lower() for keyword in arr):
            title_categories.append(arr[0])
    return title_categories


def find_body_categories(body):
    """
    finds most frequent keyword in body param

    :param body: str

    returns list of categories
    """
    body_categories = []
    frequency = [["", 0]]
    for arr in ALL_CATEGORIES:
        score = 0
        for keyword in arr:
            score += body.count(keyword)
        frequency.append([arr[0], score])
    frequency.pop(0)
    max_value = 0
    max_index = -1
    for index, freq in enumerate(frequency):
        if freq[1] > max_value:
            max_value = freq[1]
            max_index = index
    if max_index > -1:
        body_categories.append(frequency[max_index][0])
    return body_categories


@task
def categorisation(links):
    """
    categorises links and appends them to a final array

    :param links: array
    """
    for index, link in enumerate(links):
        category = "others"
        categories = []
#        if index == 20:
#           break
        try:
            html_text = requests.get(link[0], headers=HEADERS, allow_redirects=False).text            
            page = BeautifulSoup(html_text, "html.parser")
            if "github" in link[0]:
                if page.article:
                    categories = find_body_categories(page.article.text)
            else:
                if page.title:
                    title_categories = find_title_categories(page.title.text)
                if page.body:
                    body_categories = find_body_categories(page.body.text)
                c = body_categories + title_categories
                categories = list(set(c))
            for i, cat in enumerate(categories):
                if i == 0:
                    category = cat
                elif i != 0:
                    category += f",{cat}"
            LINKS_CATEGORISED.append(find_properties(link[0], category, link[1], page))
        except ValueError:
            print(link[0])
    return LINKS_CATEGORISED


@task
def sort_links():
    sorted_links = sorted(LINKS_CATEGORISED,key = lambda x: ''.join(filter(str.isdigit, x[2])), reverse = True)
    return sorted_links


@task
def write_csv(links):
    """
    writes array to a csv file

    :param links: array
    """
    with open(CSV_FILE_NAME, "w+", newline="") as f:
        w = csv.writer(f)
        w.writerows(links)
    return True


@task
def send_gsheet(task_4, client_secret, sheet_name, csv_file_name):
    """
    sends a csv file to an existing google sheet

    :param client_secret: str (file path)
    :param sheet_name: str
    :param csv_file_name: str (file path)
    """
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(client_secret,scopes=scope)
    client = gspread.authorize(credentials)
    spreadsheet = client.open(sheet_name)
    body = {
        "requests": [
            {
                "deleteRange": {
                    "range": {
                        "sheetId": SHEET_ID
                    },
                    "shiftDimension": "ROWS"
                }
            }
        ]
    }
    spreadsheet.batch_update(body)
    spreadsheet.values_update(
        sheet_name,
        params={'valueInputOption': 'USER_ENTERED'},
        body={'values': list(csv.reader(open(csv_file_name, encoding='utf-8')))}
    )



### Prefect Flow

flow_scheduler = CronSchedule(
    cron='0 14 * * 5'
)
flow_storage = GitHub(
    repo="blefr/links-page-backend",
    path="./links_backend_prefect.py",
    access_token_secret=GITHUB_TOKEN
)


def prefect_flow():
    with Flow(name='backend-links', storage=flow_storage) as flow:
        task_1 = get_blef_rss(RSS)
        task_2 = categorisation(task_1)
        task_3 = sort_links(task_2)
        task_4 = write_csv(task_3)
        task_5 = send_gsheet(task_4, PrefectSecret("GCP_CREDENTIALS"), SHEET_NAME, CSV_FILE_NAME)
        print("done :D")
    return flow

if __name__ == '__main__':  
    flow = prefect_flow()
    #flow.register(project_name="links-page-backend")
