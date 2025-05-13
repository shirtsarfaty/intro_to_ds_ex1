"""
demographics_scraper.py
=======================

Part 1 of the project.

This script performs the following tasks:
    - Crawls Worldometer demographics pages to extract country-level data.
    - Extracts demographic indicators including life expectancy (by gender), urban population (percentage and absolute), and population density.
    - Downloads and processes additional datasets: GDP per capita and population size.
    - Saves intermediate and cleaned data to CSV files.
    - Computes basic descriptive statistics.
    - Calculates the Pearson correlation between Life Expectancy (Both Sexes) and Population Density.

Outputs:
    output/demographics_before_sort.csv   (raw extracted demographics data)
    output/demographics_after_sort.csv    (sorted demographics data)
    output/demographics_data.csv          (cleaned demographics data)
    output/gdp_before_sort.csv
    output/gdp_after_sort.csv
    output/pop_before_sort.csv
    output/pop_after_sort.csv
    output/gdp_describe.csv
    output/pop_describe.csv
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from urllib.parse import urljoin
import os

def find_country_links(url: str, session) -> list[str]:
    """
    Crawl the main demographics page and find links to individual country demographics pages.

    Args:
        url (str): The URL of the Worldometer demographics page.
        session: A requests session object.

    Returns:
        list[str]: A list of country-specific demographics page URLs.
    """
    links = []
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return links

    soup = BeautifulSoup(response.text, 'html.parser')
    # Match URLs of the form https://www.worldometers.info/demographics/[country]-demographics/
    pattern = re.compile(r"^https://www.worldometers.info/demographics/(?!world).*?-demographics/")
    for link in soup.find_all('a', href=True):
        full_url = urljoin(url, link['href'])
        if pattern.match(full_url):
            links.append(full_url)
    return links


def extract_country_data(url: str, session, index: int = None, total: int = None) -> dict:
    """
    Extract demographic data from a single country's demographics page.

    Args:
        url (str): Country demographics page URL.
        session: A requests session object.
        index (int, optional): Current country index (for progress display).
        total (int, optional): Total number of countries.

    Returns:
        dict: Extracted data including life expectancy, urban population, and population density.
    """
    try:
        response = session.get(url)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Helper functions to extract specific fields from the page
    def extract_country_name():
        try:
            h1 = soup.find('h1')
            if h1:
                return h1.get_text(strip=True).replace(" Demographics", "")
        except:
            return None

    def extract_life_expectancy_males():
        try:
            cards = soup.find_all('div', class_='w-full mx-auto max-w-sm bg-white border rounded shadow-sm')
            for card in cards:
                header_span = card.find('span', class_='text-blue-400 font-bold')
                if header_span and 'males' in header_span.get_text(strip=True).lower():
                    value_div = card.find('div', class_='text-2xl font-bold mb-1.5')
                    if value_div:
                        value_text = value_div.get_text(strip=True).replace(',', '')
                        print(f"Found Life Expectancy 'Males': {value_text}")
                        return float(value_text)
        except Exception as e:
            print(f"[ERROR] extract_life_expectancy_males failed: {e}")
        return None

    def extract_life_expectancy_general(label_text):
        try:
            cards = soup.find_all('div', class_='w-full mx-auto max-w-sm bg-white border rounded shadow-sm')
            for card in cards:
                header_div = card.find('div',
                                       class_='bg-zinc-50 border-b px-4 py-3 rounded-t uppercase text-xl font-medium flex items-center justify-center gap-1.5')
                if header_div and label_text.lower() in header_div.get_text(strip=True).lower():
                    value_div = card.find('div', class_='text-2xl font-bold mb-1.5')
                    if value_div:
                        value_text = value_div.get_text(strip=True).replace(',', '')
                        print(f"Life Expectancy '{label_text}': {value_text}")
                        return float(value_text)
        except Exception as e:
            print(f"[ERROR] extract_life_expectancy_general failed for {label_text}: {e}")
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

    # Extract data for the country
    country_name = extract_country_name()
    if not country_name:
        print(f"[{index}/{total}] Could not extract country name from {url}")
        return None

    le_both = extract_life_expectancy_general("Both Sexes")
    le_female = extract_life_expectancy_general("Females")
    le_male = extract_life_expectancy_males()
    urban_pct = extract_urban_population_percentage()
    urban_abs = extract_urban_population_absolute()
    pop_density = extract_population_density()

    print(f"[{index}/{total}] {country_name}")
    print(f"  LifeExpectancy - Both: {le_both}, Female: {le_female}, Male: {le_male}")
    print(f"  Urban Pop %: {urban_pct}, Urban Pop #: {urban_abs}, Density: {pop_density}")

    return {
        "Country": country_name,
        "LifeExpectancy_Both": le_both,
        "LifeExpectancy_Female": le_female,
        "LifeExpectancy_Male": le_male,
        "UrbanPopulation_Percentage": urban_pct,
        "UrbanPopulation_Absolute": urban_abs,
        "PopulationDensity": pop_density
    }


def load_and_process_gdp_and_population():
    """
    Load GDP per capita and population datasets, process and save cleaned versions,
    and provide descriptive statistics.
    """
    df_gdp = pd.read_csv("gdp_per_capita_2021.csv", na_values=["None"])
    df_pop = pd.read_csv("population_2021.csv", na_values=["None"])

    assert "Country" in df_gdp.columns and "GDP_per_capita_PPP" in df_gdp.columns
    assert "Country" in df_pop.columns and "Population" in df_pop.columns

    # Ensure numeric types
    df_gdp["GDP_per_capita_PPP"] = pd.to_numeric(df_gdp["GDP_per_capita_PPP"], errors='coerce')
    df_pop["Population"] = pd.to_numeric(df_pop["Population"], errors='coerce')

    # Save sorted and descriptive files
    df_gdp_sorted = df_gdp.sort_values("Country")
    df_pop_sorted = df_pop.sort_values("Country")

    df_gdp_sorted.to_csv("output/gdp_after_sort.csv", index=False)
    df_pop_sorted.to_csv("output/pop_after_sort.csv", index=False)

    df_gdp.describe().to_csv("output/gdp_describe.csv")
    df_pop.describe().to_csv("output/pop_describe.csv")

    return df_gdp_sorted, df_pop_sorted


def crawl_demographics():
    """
    Crawl all country demographics pages, extract data, and save the result to CSV files.
    """
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
        time.sleep(0.5)  # polite delay

    df = pd.DataFrame(data_list)
    df = df.convert_dtypes()

    if not os.path.exists("output"):
        os.makedirs("output")

    df.to_csv("output/demographics_before_sort.csv", index=False)
    df_sorted = df.sort_values("Country")
    df_sorted.to_csv("output/demographics_data.csv", index=False)

    global df_demographics
    df_demographics = df_sorted
    return df_sorted


def main():
    """
    Main function to orchestrate the crawling, processing, and basic analysis.
    """
    df_sorted = crawl_demographics()
    load_and_process_gdp_and_population()

    df = pd.read_csv('output/demographics_data.csv')
    numeric_cols = df.select_dtypes(include='number')

    # Descriptive statistics
    stats = {
        'Count': numeric_cols.count(),
        'Mean': numeric_cols.mean(),
        'Std': numeric_cols.std(),
        'Min': numeric_cols.min(),
        'Median': numeric_cols.median(),
        'Max': numeric_cols.max(),
        'Missing Values': df.shape[0] - numeric_cols.count()
    }

    for stat_name, values in stats.items():
        display_df = pd.DataFrame(values).rename(columns={0: stat_name})
        display_df.index.name = 'Parameter'
        print(f"\n=== {stat_name} ===")
        print(display_df)

    # Correlation calculation
    df = pd.read_csv("output/demographics_data.csv", index_col="Country")
    correlation = df["LifeExpectancy_Both"].corr(df["PopulationDensity"])
    print(f"\nPearson correlation between LifeExpectancy_Both and PopulationDensity: {correlation}")


if __name__ == "__main__":
    main()
