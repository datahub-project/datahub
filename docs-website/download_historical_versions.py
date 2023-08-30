import json
import os
import tarfile
import urllib.request

repo_url = "https://api.github.com/repos/datahub-project/static-assets"


def download_file(url, destination):
    with urllib.request.urlopen(url) as response:
        with open(destination, "wb") as f:
            while True:
                chunk = response.read(8192)
                if not chunk:
                    break
                f.write(chunk)


def fetch_urls(repo_url: str, folder_path: str, file_format: str):
    api_url = f"{repo_url}/contents/{folder_path}"
    response = urllib.request.urlopen(api_url)
    data = response.read().decode("utf-8")
    urls = [
        file["download_url"]
        for file in json.loads(data)
        if file["name"].endswith(file_format)
    ]
    print(urls)
    return urls


def extract_tar_file(destination_path):
    with tarfile.open(destination_path, "r:gz") as tar:
        tar.extractall()
    os.remove(destination_path)


def download_versioned_docs(folder_path: str, destination_dir: str, file_format: str):
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    urls = fetch_urls(repo_url, folder_path, file_format)

    for url in urls:
        filename = os.path.basename(url)
        destination_path = os.path.join(destination_dir, filename)

        version = ".".join(filename.split(".")[:3])
        extracted_path = os.path.join(destination_dir, version)
        print("extracted_path", extracted_path)
        if os.path.exists(extracted_path):
            print(f"{extracted_path} already exists, skipping downloads")
            continue
        try:
            download_file(url, destination_path)
            print(f"Downloaded {filename} to {destination_dir}")
            if file_format == ".tar.gz":
                extract_tar_file(destination_path)
        except urllib.error.URLError as e:
            print(f"Error while downloading {filename}: {e}")
            continue


def main():
    download_versioned_docs(
        folder_path="versioned_docs",
        destination_dir="versioned_docs",
        file_format=".tar.gz",
    )
    download_versioned_docs(
        folder_path="versioned_sidebars",
        destination_dir="versioned_sidebars",
        file_format=".json",
    )


if __name__ == "__main__":
    main()
