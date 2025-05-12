#
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# import re
# import time
# from urllib.parse import urljoin
# import os
#
# def find_country_links(url: str, session) -> list[str]:
#     links = []
#     try:
#         response = session.get(url)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Request failed: {e}")
#         return links
#
#     soup = BeautifulSoup(response.text, 'html.parser')
#     pattern = re.compile(r"^https://www.worldometers.info/demographics/(?!world).*?-demographics/")
#
#     for link in soup.find_all('a', href=True):
#         full_url = urljoin(url, link['href'])
#         if pattern.match(full_url):
#             links.append(full_url)
#     return links
#
# def extract_country_data(url: str, session, index: int = None, total: int = None) -> dict:
#     try:
#         response = session.get(url)
#         response.raise_for_status()
#     except requests.RequestException as e:
#         print(f"Failed to fetch {url}: {e}")
#         return None
#
#     soup = BeautifulSoup(response.text, 'html.parser')
#
#     def extract_country_name():
#         try:
#             h1 = soup.find('h1')
#             if h1:
#                 return h1.get_text(strip=True).replace(" Demographics", "")
#         except:
#             return None
#
#     def extract_life_expectancy(label_text):
#         try:
#             card = soup.find('div', string=re.compile(label_text, re.IGNORECASE)).find_parent('div', class_='w-full')
#             value = card.find('div', class_='text-2xl').get_text(strip=True)
#             return float(value)
#         except:
#             return None
#
#     def extract_urban_population_percentage():
#         try:
#             h2 = soup.find('h2', id='urb')
#             p = h2.find_next('p')
#             match = re.search(r'([\d.,]+)\s*%', p.text)
#             if match:
#                 return float(match.group(1).replace(',', ''))
#         except:
#             return None
#
#     def extract_urban_population_absolute():
#         try:
#             h2 = soup.find('h2', id='urb')
#             p = h2.find_next('p')
#             match = re.search(r'urban.*?([\d,]+)', p.text, re.IGNORECASE)
#             if match:
#                 return int(match.group(1).replace(',', ''))
#         except:
#             return None
#
#     def extract_population_density():
#         try:
#             h2 = soup.find('h2', id='population-density')
#             p = h2.find_next('p')
#             match = re.search(r'([\d,]+)\s*people per Km2', p.text)
#             if match:
#                 return int(match.group(1).replace(',', ''))
#         except:
#             return None
#
#     country_name = extract_country_name()
#     if not country_name:
#         print(f"[{index}/{total}]  Could not extract country name from {url}")
#         return None
#
#     le_both = extract_life_expectancy("Both Sexes")
#     le_female = extract_life_expectancy("Females")
#     le_male = extract_life_expectancy("Males")
#     urban_pct = extract_urban_population_percentage()
#     urban_abs = extract_urban_population_absolute()
#     pop_density = extract_population_density()
#
#     # Debug print
#     print(f"[{index}/{total}]  {country_name}")
#     print(f"  LifeExpectancy - Both: {le_both}, Female: {le_female}, Male: {le_male}")
#     print(f"  Urban Pop %: {urban_pct}, Urban Pop #: {urban_abs}, Density: {pop_density}")
#
#     data = {
#         "Country": country_name,
#         "LifeExpectancy Both": le_both,
#         "LifeExpectancy Female": le_female,
#         "LifeExpectancy Male": le_male,
#         "UrbanPopulation Percentage": urban_pct,
#         "UrbanPopulation Absolute": urban_abs,
#         "PopulationDensity": pop_density,
#         "ScrapeTimestamp": pd.Timestamp.now().isoformat()
#     }
#
#     return data
#
# def crawl_demographics():
#     base_url = "https://www.worldometers.info/demographics/"
#     session = requests.Session()
#     session.headers.update({
#         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
#     })
#
#     print("Crawling base page...")
#     country_links = find_country_links(base_url, session)
#     print(f"Found {len(country_links)} country links")
#
#     data_list = []
#     for i, link in enumerate(country_links, start=1):
#         data = extract_country_data(link, session, index=i, total=len(country_links))
#         if data:
#             data_list.append(data)
#         time.sleep(0.5)
#
#     df = pd.DataFrame(data_list)
#     df = df.convert_dtypes()
#     print("\nFirst 10 rows (before sorting):")
#     print(df.head(10))
#
#     if not os.path.exists("output"):
#         os.makedirs("output")
#
#     df.to_csv("output/demographics_before_sort.csv", index=False)
#
#     df_sorted = df.sort_values("Country")
#     print("\nFirst 10 rows (after sorting):")
#     print(df_sorted.head(10))
#     df_sorted.to_csv("output/demographics_after_sort.csv", index=False)
#
#     print("\nAll data saved to:")
#     print(" - output/demographics_before_sort.csv")
#     print(" - output/demographics_after_sort.csv")
#     return df_sorted
#
# def main():
#     """
#     Main function to execute the full process:
#     - Crawl demographic data for all countries.
#     - Save the results to CSV files.
#     """
#     df_sorted = crawl_demographics()
#
#     print("First 5 rows of demographics data after sorting:")
#     print(df_sorted.head(5))
#
# if __name__ == "__main__":
#     main()

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from urllib.parse import urljoin
import os

def find_country_links(url: str, session) -> list[str]:
    links = []
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return links

    soup = BeautifulSoup(response.text, 'html.parser')
    pattern = re.compile(r"^https://www.worldometers.info/demographics/(?!world).*?-demographics/")
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])
        if pattern.match(full_url):
            links.append(full_url)
    return links

def extract_country_data(url: str, session, index: int = None, total: int = None) -> dict:
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    def extract_country_name():
        try:
            h1 = soup.find('h1')
            if h1:
                return h1.get_text(strip=True).replace(" Demographics", "")
        except:
            return None

    def extract_life_expectancy(label_text):
        try:
            card = soup.find('div', string=re.compile(label_text, re.IGNORECASE)).find_parent('div', class_='w-full')
            value = card.find('div', class_='text-2xl').get_text(strip=True)
            return float(value)
        except:
            return None

    def extract_urban_population_percentage():
        try:
            h2 = soup.find('h2', id='urb')
            p = h2.find_next('p')
            match = re.search(r'([\d.,]+)\s*%', p.text)
            if match:
                return float(match.group(1).replace(',', ''))
        except:
            return None

    def extract_urban_population_absolute():
        try:
            h2 = soup.find('h2', id='urb')
            p = h2.find_next('p')
            match = re.search(r'urban.*?([\d,]+)', p.text, re.IGNORECASE)
            if match:
                return int(match.group(1).replace(',', ''))
        except:
            return None

    def extract_population_density():
        try:
            h2 = soup.find('h2', id='population-density')
            p = h2.find_next('p')
            match = re.search(r'([\d,]+)\s*people per Km2', p.text)
            if match:
                return int(match.group(1).replace(',', ''))
        except:
            return None

    country_name = extract_country_name()
    if not country_name:
        print(f"[{index}/{total}] Could not extract country name from {url}")
        return None

    le_both = extract_life_expectancy("Both Sexes")
    le_female = extract_life_expectancy("Females")
    le_male = extract_life_expectancy("Males")
    urban_pct = extract_urban_population_percentage()
    urban_abs = extract_urban_population_absolute()
    pop_density = extract_population_density()

    print(f"[{index}/{total}] {country_name}")
    print(f"  LifeExpectancy - Both: {le_both}, Female: {le_female}, Male: {le_male}")
    print(f"  Urban Pop %: {urban_pct}, Urban Pop #: {urban_abs}, Density: {pop_density}")

    return {
        "Country": country_name,
        "LifeExpectancy Both": le_both,
        "LifeExpectancy Female": le_female,
        "LifeExpectancy Male": le_male,
        "UrbanPopulation Percentage": urban_pct,
        "UrbanPopulation Absolute": urban_abs,
        "PopulationDensity": pop_density,
        "ScrapeTimestamp": pd.Timestamp.now().isoformat()
    }

def load_and_process_gdp_and_population():
    df_gdp = pd.read_csv("gdp_per_capita_2021.csv", na_values=["None"])
    df_pop = pd.read_csv("population_2021.csv", na_values=["None"])

    assert "Country" in df_gdp.columns and "GDP_per_capita_PPP" in df_gdp.columns
    assert "Country" in df_pop.columns and "Population" in df_pop.columns

    df_gdp["GDP_per_capita_PPP"] = pd.to_numeric(df_gdp["GDP_per_capita_PPP"], errors='coerce')
    df_pop["Population"] = pd.to_numeric(df_pop["Population"], errors='coerce')

    df_gdp.to_csv("output/gdp before sort.csv", index=False)
    df_pop.to_csv("output/pop before sort.csv", index=False)

    df_gdp_sorted = df_gdp.sort_values("Country")
    df_pop_sorted = df_pop.sort_values("Country")

    df_gdp_sorted.to_csv("output/gdp after sort.csv", index=False)
    df_pop_sorted.to_csv("output/pop after sort.csv", index=False)

    df_gdp.describe().to_csv("output/gdp describe.csv")
    df_pop.describe().to_csv("output/pop describe.csv")

    print("\nGDP DataFrame:")
    print("Before sorting:")
    print(df_gdp.head(5))
    print("After sorting:")
    print(df_gdp_sorted.head(5))

    print("\nPopulation DataFrame:")
    print("Before sorting:")
    print(df_pop.head(5))
    print("After sorting:")
    print(df_pop_sorted.head(5))

    print("\nShapes and columns:")
    print("Demographics:", df_demographics.shape, list(df_demographics.columns))
    print("GDP:", df_gdp.shape, list(df_gdp.columns))
    print("Population:", df_pop.shape, list(df_pop.columns))

    return df_gdp_sorted, df_pop_sorted

def crawl_demographics():
    base_url = "https://www.worldometers.info/demographics/"
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    })

    print("Crawling base page...")
    country_links = find_country_links(base_url, session)
    print(f"Found {len(country_links)} country links")

    data_list = []
    for i, link in enumerate(country_links, start=1):
        data = extract_country_data(link, session, index=i, total=len(country_links))
        if data:
            data_list.append(data)
        time.sleep(0.5)

    df = pd.DataFrame(data_list)
    df = df.convert_dtypes()

    if not os.path.exists("output"):
        os.makedirs("output")

    df.to_csv("output/demographics before sort.csv", index=False)
    print("\nFirst 10 rows before sorting:")
    print(df.head(10))

    df_sorted = df.sort_values("Country")
    print("\nFirst 10 rows after sorting:")
    print(df_sorted.head(10))

    df_sorted.to_csv("output/demographics after sort.csv", index=False)
    df_sorted.to_csv("output/demographics data.csv", index=False)

    global df_demographics
    df_demographics = df_sorted  # So other functions can access it
    return df_sorted

def main():
    print("\n--- Part 1 preview ---")
    df_sorted = crawl_demographics()
    load_and_process_gdp_and_population()

if __name__ == "__main__":
    main()

