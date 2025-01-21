import re
import argparse

def update_pyproject_version(file_path: str, new_version: str) -> None:
    """Update version in pyproject.toml file.
    
    Args:
        file_path: Path to pyproject.toml file
        new_version: New version string to set
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    pattern = r'(version = ")[^"]+(")' 
    def replace(match):
        return f'{match.group(1)}{new_version}{match.group(2)}'
        
    new_content = re.sub(pattern, replace, content)
    print(f"Updating version in {file_path} to {new_version}")
    
    with open(file_path, 'w') as f:
        f.write(new_content)

def update_init_version(file_path: str, new_version: str) -> None:
    """Update __version__ in Python __init__.py file.
    
    Args:
        file_path: Path to __init__.py file
        new_version: New version string to set
    """
    with open(file_path, 'r') as f:
        content = f.read()
    
    pattern = r'(__version__ = ")[^"]+(")' 
    def replace(match):
        return f'{match.group(1)}{new_version}{match.group(2)}'
        
    new_content = re.sub(pattern, replace, content)
    print(f"Updating version in {file_path} to {new_version}")
    
    with open(file_path, 'w') as f:
        f.write(new_content)

def main():
    parser = argparse.ArgumentParser(description='Update version strings in project files')
    parser.add_argument('--version', help='New version string')
    parser.add_argument('--pyproject', help='Path to pyproject.toml file')
    parser.add_argument('--init', help='Path to __init__.py file')
    
    args = parser.parse_args()
    
    if args.pyproject:
        update_pyproject_version(args.pyproject, args.version)
    
    if args.init:
        update_init_version(args.init, args.version)

if __name__ == "__main__":
    main()