import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
from urllib.parse import urljoin

def find_country_links(url: str) -> list[str]:
    links = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
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


def extract_country_data(url: str) -> dict:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    def extract_life_expectancy(soup, label_text):
        try:
            card = soup.find('div', string=re.compile(label_text, re.IGNORECASE)).find_parent('div', class_='w-full')
            value = card.find('div', class_='text-2xl').get_text(strip=True)
            return float(value)
        except:
            return None

    def extract_urban_population_percentage(soup):
        try:
            h2 = soup.find('h2', id='urb')
            p = h2.find_next('p')
            match = re.search(r'([\d.,]+)\s*%', p.text)
            if match:
                return float(match.group(1).replace(',', ''))
        except:
            return None

    def extract_urban_population_absolute(soup):
        try:
            h2 = soup.find('h2', id='urb')
            p = h2.find_next('p')
            match = re.search(r'urban.*?([\d,]+)', p.text, re.IGNORECASE)
            if match:
                return int(match.group(1).replace(',', ''))
        except:
            return None

    def extract_population_density(soup):
        try:
            h2 = soup.find('h2', id='population-density')
            p = h2.find_next('p')
            match = re.search(r'([\d,]+)\s*people per Km2', p.text)
            if match:
                return int(match.group(1).replace(',', ''))
        except:
            return None

    # Extract data
    country_name = url.split("/")[-1].replace("-demographics", "").replace("-", " ").title()

    data = {
        "Country": country_name,
        "LifeExpectancy Both": extract_life_expectancy(soup, "Both Sexes"),
        "LifeExpectancy Female": extract_life_expectancy(soup, "Females"),
        "LifeExpectancy Male": extract_life_expectancy(soup, "Males"),
        "UrbanPopulation Percentage": extract_urban_population_percentage(soup),
        "UrbanPopulation Absolute": extract_urban_population_absolute(soup),
        "PopulationDensity": extract_population_density(soup)
    }

    return data



def crawl_demographics():
    base_url = "https://www.worldometers.info/demographics/"
    print("Crawling base page...")
    country_links = find_country_links(base_url)
    print(f"Found {len(country_links)} country links")

    data_list = []
    for link in country_links:
        data = extract_country_data(link)
        if data:
            data_list.append(data)

    df = pd.DataFrame(data_list)
    print("Sample result (first 10 rows):")
    print(df.head(10))

    df_sorted = df.sort_values("Country")
    df_sorted.to_csv("demographics_data.csv", index=False)
    print("Data saved to demographics_data.csv")
    return df_sorted
import pandas as pd

def extract_gdp_data() -> dict:
    gdp_data = {}
    gdp_df = pd.read_csv("gdp_per_capita_2021.csv")  # קרא את קובץ ה-GDP
    for _, row in gdp_df.iterrows():
        gdp_data[row["Country"]] = row["GDP per capita PPP"]  # שמור את המידע במילון
    return gdp_data

def extract_population_data() -> dict:
    population_data = {}
    pop_df = pd.read_csv("population_2021.csv")  # קרא את קובץ האוכלוסייה
    for _, row in pop_df.iterrows():
        population_data[row["Country"]] = row["Population"]  # שמור את המידע במילון
    return population_data

def merge_datasets(demographics_df, gdp_data, population_data):
    # חיבור נתוני GDP ואוכלוסייה עם נתוני הדמוגרפיה
    demographics_df["GDP per capita PPP"] = demographics_df["Country"].map(gdp_data)  # מיזוג GDP
    demographics_df["Population"] = demographics_df["Country"].map(population_data)  # מיזוג אוכלוסייה
    return demographics_df

def crawl_demographics():
    # קרוא ל-crawl ולייצא את הנתונים ממדינות
    base_url = "https://www.worldometers.info/demographics/"
    print("Crawling base page...")
    country_links = find_country_links(base_url)
    print(f"Found {len(country_links)} country links")

    data_list = []
    for link in country_links:
        data = extract_country_data(link)
        if data:
            data_list.append(data)

    # יצירת DataFrame עם הנתונים מהמדינות
    demographics_df = pd.DataFrame(data_list)
    print("Sample result (first 10 rows):")
    print(demographics_df.head(10))

    # חיבור הנתונים עם ה-GDP והאוכלוסייה
    gdp_data = extract_gdp_data()
    population_data = extract_population_data()
    final_df = merge_datasets(demographics_df, gdp_data, population_data)


    final_df.to_csv("final_demographics_with_gdp_population.csv", index=False)
    print("Data saved to final_demographics_with_gdp_population.csv")
    return final_df


def crawl_demographics():

    base_url = "https://www.worldometers.info/demographics/"
    print("Crawling base page...")
    country_links = find_country_links(base_url)
    print(f"Found {len(country_links)} country links")

    data_list = []
    for link in country_links:
        data = extract_country_data(link)
        if data:
            data_list.append(data)

    demographics_df = pd.DataFrame(data_list)

    numeric_cols = ["LifeExpectancy Both", "LifeExpectancy Female", "LifeExpectancy Male",
                    "UrbanPopulation Percentage", "UrbanPopulation Absolute", "PopulationDensity"]
    for col in numeric_cols:
        demographics_df[col] = pd.to_numeric(demographics_df[col], errors='coerce')

    print("Sample result (first 10 rows BEFORE sort):")
    print(demographics_df.head(10))


    os.makedirs("output", exist_ok=True)

    demographics_df.head(10).to_csv("output/demographics_before_sort.csv", index=False)

    df_sorted = demographics_df.sort_values("Country")

    print("Sample result (first 10 rows AFTER sort):")
    print(df_sorted.head(10))

    df_sorted.head(10).to_csv("output/demographics_after_sort.csv", index=False)

    df_sorted.to_csv("output/demographics_data.csv", index=False)

    print("Data saved to output/demographics_data.csv")
    return df_sorted

if __name__ == "__main__":
    crawl_demographics()

