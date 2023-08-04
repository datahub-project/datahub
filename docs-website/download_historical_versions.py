import os
import tarfile

import requests


def download_file(url, destination):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)


def fetch_tar_urls(repo_url, folder_path):
    api_url = f"{repo_url}/contents/{folder_path}"
    response = requests.get(api_url)
    response.raise_for_status()
    data = response.json()
    tar_urls = [
        file["download_url"] for file in data if file["name"].endswith(".tar.gz")
    ]
    print(tar_urls)
    return tar_urls


def main():
    repo_url = "https://api.github.com/repos/datahub-project/static-assets"
    folder_path = "versioned_docs"

    destination_dir = "versioned_docs"
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    tar_urls = fetch_tar_urls(repo_url, folder_path)

    for url in tar_urls:
        filename = os.path.basename(url)
        destination_path = os.path.join(destination_dir, filename)

        try:
            download_file(url, destination_path)
            print(f"Downloaded {filename} to {destination_dir}")
            with tarfile.open(destination_path, "r:gz") as tar:
                tar.extractall()
            os.remove(destination_path)
        except requests.exceptions.RequestException as e:
            print(f"Error while downloading {filename}: {e}")
            continue


if __name__ == "__main__":
    main()
