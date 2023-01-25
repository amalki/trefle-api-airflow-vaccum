import requests
from requests import Response
import csv

token = "qbJR7wvmVypfTrQv8bNmBpS-gK0pRxEHrE1SOOiGacs"


def get_resp_one_page(page: int, token: str) -> Response:
    url = f"https://trefle.io/api/v1/plants?token={token}&page={page}"

    resp = requests.get(url)

    return resp


def get_data_all_plants(n_page_max: int) -> list:

    all_list_of_dict_plants = []
    for page in range(1, n_page_max + 1):
        print(f"current page: {page}")
        resp = get_resp_one_page(page, token)

        list_of_dict_plants = resp.json()["data"]

        all_list_of_dict_plants.extend(list_of_dict_plants)

    return all_list_of_dict_plants


def write_dict_list_to_csv(all_list_of_dict_plants: list, csv_path: str) -> None:

    keys = all_list_of_dict_plants[0].keys()

    with open(csv_path, "w", newline="") as f:
        dict_writer = csv.DictWriter(f, keys, delimiter=";")
        # dict_writer.writeheader()
        dict_writer.writerows(all_list_of_dict_plants)


def fetch_data_to_local():

    all_list_of_dict_plants = get_data_all_plants(n_page_max=5)

    write_dict_list_to_csv(all_list_of_dict_plants, csv_path="/opt/data/plants.csv")


n_pages_total = 2180
n_pages_per_file = 730
l = [
    (i * n_pages_per_file + 1, (i + 1) * n_pages_per_file)
    for i in range(0, n_pages_total // n_pages_per_file)
]

print(l)
