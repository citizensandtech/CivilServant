import argparse
import json
import random


def clear_descriptions(data):
    print("-- Clearing description field")
    for obj in data:
        if "description" in obj:
            obj["description"] = ""
    return data


def clear_subreddits(data):
    print("-- Clearing subreddit fields")
    for obj in data:
        if "subreddit" in obj:
            obj["subreddit"] = "anonymized"
        if "subreddit_name_prefixed" in obj:
            obj["subreddit_name_prefixed"] = "r/anonymized"
    return data


def add_random_to_timestamp(data):
    print("-- Adding random seconds to timestamp")
    for obj in data:
        if "created_utc" in obj:
            obj["created_utc"] += random.randint(-60, 60)
    return data


def filter_banuser(data):
    print("-- Filtering only for banuser actions")
    return [obj for obj in data if obj.get("action") == "banuser"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process JSON file with optional features."
    )
    parser.add_argument("input_file", type=str, help="Input JSON file")
    parser.add_argument("output_file", type=str, help="Output JSON file")

    args = parser.parse_args()

    with open(args.input_file, "r") as infile:
        data = json.load(infile)


    data = filter_banuser(data)

    data = clear_descriptions(data)

    data = clear_subreddits(data)

    data = add_random_to_timestamp(data)

    with open(args.output_file, "w") as outfile:
        json.dump(data, outfile, indent=4)

    print(f"Data processed and saved to {args.output_file}")
