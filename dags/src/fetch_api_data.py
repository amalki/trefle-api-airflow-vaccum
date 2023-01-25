import time
import requests
from requests import Response
import csv
import logging

from airflow.models import Variable


def get_page_pairs_by_files() -> list:
    """get a list of page numbers pairs based on the number of api pages you need for each page and total number of pages

    Returns:
        list: list of page numbers pairs
    """

    n_pages_total = int(Variable.get("n_pages_max"))
    n_pages_per_file = int(Variable.get("n_pages_per_file"))  # 730 optimaly
    pages_tuples_by_files = [
        (i * n_pages_per_file + 1, (i + 1) * n_pages_per_file)
        for i in range(0, n_pages_total // n_pages_per_file)
    ]

    return pages_tuples_by_files


def get_resp_one_page(page: int, token: str) -> Response:
    """return a response from the trefle plants endpoint

    Args:
        page (int): page to fetch
        token (str): auth token

    Raises:
        SystemExit: generic request error

    Returns:
        Response: api response
    """

    url = f"https://trefle.io/api/v1/plants?token={token}&page={page}"

    try:
        resp = requests.get(url)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

    return resp


def get_data_plants_between_pages(n_page_min: int, n_page_max: int) -> list:
    """get all data from n_page_min up to n_page_max

    Args:
        n_page_min (int): first page number to fetch data from
        n_page_max (int): last page number to fetch data from

    Returns:
        list: _description_
    """

    logging.info("fetching data from API trefle for new file...")
    all_list_of_dict_plants = []
    for page in range(n_page_min, n_page_max + 1):
        logging.info(f"current page: {page}")
        resp = get_resp_one_page(page, Variable.get("trefle_api_token"))

        list_of_dict_plants = resp.json().get("data")

        if list_of_dict_plants == None:
            break

        all_list_of_dict_plants.extend(list_of_dict_plants)

        time.sleep(0.5)  # to respect 120 requests per minute limit rate

    return all_list_of_dict_plants


def get_data_all_plants(n_page_max: int) -> list:
    """get all data from page 1 up to n_page_max

    Args:
        n_page_max (int): last page number to fetch data from

    Returns:
        all_list_of_dict_plants: data fetched as a list of dicts
    """

    all_list_of_dict_plants = get_data_plants_between_pages(1, n_page_max)

    return all_list_of_dict_plants


def write_dict_list_to_csv(all_list_of_dict_plants: list, csv_path: str) -> None:
    """write a list of dict to local path

    Args:
        all_list_of_dict_plants (list): list of dicts
        csv_path (str): target path for writing
    """

    keys = all_list_of_dict_plants[0].keys()

    with open(csv_path, "w", newline="") as f:
        dict_writer = csv.DictWriter(f, keys, delimiter="|")
        # dict_writer.writeheader()
        dict_writer.writerows(all_list_of_dict_plants)


def fetch_data_to_local_old():

    all_list_of_dict_plants = get_data_all_plants(n_page_max=400)

    write_dict_list_to_csv(all_list_of_dict_plants, csv_path="/opt/data/plants.csv")


def fetch_data_to_local(**context):

    list_local_file_paths = []

    page_pairs_list = get_page_pairs_by_files()

    for page_pair in page_pairs_list:
        n_page_min = page_pair[0]
        n_page_max = page_pair[1]

        list_of_dict_plants_one_file = get_data_plants_between_pages(
            n_page_min, n_page_max
        )

        file_path = f"/opt/data/plants-{n_page_min}.csv"
        write_dict_list_to_csv(list_of_dict_plants_one_file, csv_path=file_path)

        list_local_file_paths.append(file_path)

    context["task_instance"].xcom_push("local_file_paths", list_local_file_paths)
