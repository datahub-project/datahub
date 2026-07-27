import argparse
import json
import sys
from random import choice, randint, uniform


def generate_user_data(count: int) -> list:
    """Generate varying user data for testing"""
    first_names = ["Jane", "John", "Bob", "Alice", "Charlie"]
    last_names = ["Doe", "Smith", "Johnson", "Williams", "Brown"]
    domains = ["example.com", "test.com", "email.com"]

    data = []
    for i in range(count):
        # Sometimes generate null values
        has_null = i % 5 == 0
        record = {
            "email": None
            if has_null
            else f"{first_names[i % len(first_names)].lower()}_{i}@{choice(domains)}",
            "firstName": first_names[i % len(first_names)],
            "lastName": None if (i % 7 == 0) else last_names[i % len(last_names)],
        }
        data.append(record)
    return data


def generate_numeric_data(count: int) -> list:
    """Generate numeric data with known statistical properties"""
    data = []
    for i in range(count):
        record = {"id": randint(1, 1000), "value": uniform(0, 100), "count": i * 10}
        data.append(record)
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["user", "numeric"], required=True)
    parser.add_argument("--count", type=int, default=100)
    args = parser.parse_args()

    if args.type == "user":
        data = generate_user_data(args.count)
    else:
        data = generate_numeric_data(args.count)

    json.dump(data, sys.stdout, indent=2)


if __name__ == "__main__":
    main()
