import re
import os
from typing import List, Tuple
import argparse

# This is a poor man's check for null dereference check for tsx files
# Use it like this

# python .github/scripts/typescript-null-checker.py --format text ./datahub-web-react/src --fix
# cd datahub-web-react
# yarn install
# yarn lint-fix

### Manually fix any problems created by this script (3-4 problems got created) and re-run the above steps

class TypeScriptNullChecker:
    def __init__(self):
        # Pattern to match chains that already have at least one optional chaining operator
        self.chain_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*(?:\?\.|\.)(?:[a-zA-Z_][a-zA-Z0-9_]*(?:\?\.|\.))*[a-zA-Z_][a-zA-Z0-9_]*(?:\[\d+\](?:\.[a-zA-Z_][a-zA-Z0-9_]*)*)*)'

    def find_property_chains(self, code: str) -> List[str]:
        """Find property chains that have at least one optional chaining operator but might be missing others."""
        chains = re.findall(self.chain_pattern, code)
        return [
            chain for chain in chains 
            if '?.' in chain  # Must have at least one optional chaining
            and ('.' in chain.replace('?.', ''))  # Must have at least one regular dot
        ]

    def analyze_chain(self, chain: str) -> Tuple[bool, str]:
        """Analyze a property chain that already has some optional chaining for missing operators."""
        parts = re.split(r'[\.\[\]]', chain)
        indices = [i for i, char in enumerate(chain) if char in '.[]']
        
        has_issue = False
        fixed_chain = list(chain)
        last_optional = -1
        
        for i, char in enumerate(chain):
            if char == '?':
                last_optional = i
                
        for i in indices:
            if i <= last_optional:
                continue
                
            if chain[i] == '.' and (i == 0 or chain[i-1] != '?'):
                if i > 0 and chain[i-1] != ']':
                    has_issue = True
                    fixed_chain.insert(i, '?')
                    indices = [idx + 1 if idx > i else idx for idx in indices]
            
            elif chain[i] == '[':
                next_dot = next((idx for idx in indices if idx > i and chain[idx] == '.'), None)
                if next_dot and (next_dot == 0 or chain[next_dot-1] != '?'):
                    has_issue = True
                    fixed_chain.insert(next_dot, '?')
                    indices = [idx + 1 if idx > next_dot else idx for idx in indices]
        
        return has_issue, ''.join(fixed_chain)
    
    def check_code(self, code: str, filename: str = "") -> List[dict]:
        """Check TypeScript code for potential null dereference issues."""
        issues = []
        chains = self.find_property_chains(code)
        
        for chain in chains:
            has_issue, fixed_chain = self.analyze_chain(chain)
            if has_issue:
                issues.append({
                    'filename': filename,
                    'original': chain,
                    'fixed': fixed_chain,
                    'message': f'Inconsistent null checking detected in chain: {chain}'
                })
        
        return issues

def scan_directory(directory: str, fix: bool = False) -> List[dict]:
    """Recursively scan a directory for TypeScript files and check for null dereference issues."""
    checker = TypeScriptNullChecker()
    all_issues = []
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(('.ts', '.tsx')):
                file_path = os.path.join(root, file)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    issues = checker.check_code(content, file_path)
                    if issues:
                        all_issues.extend(issues)
                        
                        if fix:
                            # Apply fixes directly
                            fixed_content = content
                            for issue in issues:
                                fixed_content = fixed_content.replace(issue['original'], issue['fixed'])
                            
                            # Write fixed content back to file
                            with open(file_path, 'w', encoding='utf-8') as f:
                                f.write(fixed_content)
                            
                except Exception as e:
                    print(f"Error processing {file_path}: {str(e)}")
    
    return all_issues

def main():
    parser = argparse.ArgumentParser(description='Detect inconsistent null checking in TypeScript files')
    parser.add_argument('directory', help='Directory to scan')
    parser.add_argument('--fix', action='store_true', help='Automatically fix issues')
    parser.add_argument('--format', choices=['text', 'json'], default='text', help='Output format')
    args = parser.parse_args()
    
    issues = scan_directory(args.directory, args.fix)
    
    if args.format == 'json':
        import json
        print(json.dumps(issues, indent=2))
    else:
        if not issues:
            print("No inconsistent null checking found.")
        else:
            print(f"\nFound {len(issues)} instances of inconsistent null checking:")
            for issue in issues:
                print(f"\nFile: {issue['filename']}")
                print(f"Original: {issue['original']}")
                print(f"Fixed to: {issue['fixed']}")
            
            if args.fix:
                print(f"\nApplied fixes to {len(issues)} locations.")

if __name__ == "__main__":
    main()
