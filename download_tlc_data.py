import os
import re
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

BASE_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
DOWNLOAD_DIR = "tlc_data"

# Définition de la plage de dates à récupérer
START_YEAR, START_MONTH = 2019, 1
END_YEAR, END_MONTH = 2024, 6

def is_valid_file(filename):
    """Vérifie si le fichier est un 'yellow_tripdata' et dans la plage de 2019-01 à 2024-06."""
    match = re.search(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet", filename)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return (START_YEAR <= year <= END_YEAR) and (year < END_YEAR or month <= END_MONTH)
    return False

def get_file_links():
    """Récupère les liens des fichiers 'yellow_tripdata' depuis la page officielle."""
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        filename = href.split("/")[-1]
        if filename.startswith("yellow_tripdata") and filename.endswith(".parquet") and is_valid_file(filename):
            full_url = href if href.startswith("http") else f"https://www.nyc.gov{href}"
            links.append(full_url)
    
    print(f"{len(links)} fichiers 'yellow_tripdata' trouvés entre 2019 et juin 2024.")
    print("Exemple de liens :", links[:5])  # Affiche les 5 premiers liens

    return links

def download_file(url, folder):
    """Télécharge un fichier avec affichage de la progression."""
    filename = os.path.join(folder, url.split("/")[-1])

    if os.path.exists(filename):
        print(f"Le fichier {filename} existe déjà. Téléchargement ignoré.")
        return

    response = requests.get(url, stream=True)
    response.raise_for_status()

    total_size = int(response.headers.get("content-length", 0))
    with open(filename, "wb") as file, tqdm(
        desc=filename, total=total_size, unit="B", unit_scale=True
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024):
            file.write(chunk)
            bar.update(len(chunk))

def main():
    """Télécharge automatiquement les fichiers 'yellow_tripdata' entre 2019 et juin 2024."""
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    print("Récupération des liens des fichiers...")
    file_links = get_file_links()
    
    if not file_links:
        print("Aucun fichier trouvé. Vérifie l'URL de la source.")
        return

    print(f"{len(file_links)} fichiers trouvés. Début du téléchargement...\n")
    
    for link in file_links:
        download_file(link, DOWNLOAD_DIR)

    print("\nTous les fichiers ont été téléchargés avec succès.")

if __name__ == "__main__":
    main()
