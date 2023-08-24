import os
import re


def replace_link_pattern_in_file(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # pattern = r'\!\[(.*?)\]\(\.\/imgs\/(.*?)\.png\)'
    # pattern = r'\!\[(.*?)\]\(imgs\/(.*?)\.png\)'
    pattern = r'\!\[(.*?)\]\(\.\.\/imgs\/(.*?)\.png\)'
    pattern = r'\!\[(.*?)\]\(\.\.\/\.\.\/imgs\/(.*?)\.png\)'
    # pattern = r'\!\[(.*?)\]\(img\/(.*?)\.png\)' #sso
    # pattern = r'\!\[(.*?)\]\(\.\.\/managed-datahub\/imgs\/saas\/(.*?)\.png\)' # saas
    # pattern = r'\!\[(.*?)\]\(imgs\/saas\/(.*?)\.png\)' #saas
    # pattern = r'\!\[(.*?)\]\(\.\.\/\.\.\/imgs\/saas\/(.*?)\.png\)' #saas
    pattern = r'!\[(.*?)\]\(\.\.\/\.\.\/docs\/imgs\/(.*?)\.png\)' #airflow

    replacement = r'\n<p align="center">\n  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/\2.png"/>\n</p>\n'

    new_content = re.sub(pattern, replacement, content)

    with open(file_path, 'w') as file:
        file.write(new_content)


# Directory to search for files
directory_path = 'docker'

# Loop through all files in the directory
for root, dirs, files in os.walk(directory_path):
    for file_name in files:
        if file_name.endswith('.md'):  # Replace with the desired file extension
            file_path = os.path.join(root, file_name)
            replace_link_pattern_in_file(file_path)
            print(f"Modified: {file_path}")
