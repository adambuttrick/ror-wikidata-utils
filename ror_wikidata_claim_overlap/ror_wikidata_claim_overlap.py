import os
import csv
import json
import logging
import argparse
import requests
import multiprocessing
from functools import partial


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Generate CSV files mapping ROR IDs to Wikidata claim values.")
    parser.add_argument("-i", "--input_file", required=True,
                        help="Path to JSON file containing claim IDs and names")
    parser.add_argument("-d", "--output_directory", default="ror_wikidata_claims",
                        help="Directory to store output CSV files")
    parser.add_argument(
        "-e", "--endpoint", default="https://query.wikidata.org/sparql", help="SPARQL endpoint URL")
    parser.add_argument("-l", "--limit", type=int,
                        default=10000, help="LIMIT value for SPARQL query")
    parser.add_argument("-o", "--offset", type=int, default=0,
                        help="Initial OFFSET value for SPARQL query")
    parser.add_argument(
        "--email", help="Email address to include in request header")
    return parser.parse_args()


def parse_json_claims(file_path):
    try:
        with open(file_path, 'r') as f:
            claims = json.load(f)
        return claims
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file: {e}")
        raise
    except IOError as e:
        logger.error(f"Error reading file: {e}")
        raise


def generate_sparql_query(claims):
    select_clause = "SELECT ?item ?rorID"
    where_clause = "WHERE {\n  ?item wdt:P6782 ?rorID ."
    for claim_id, claim_name in claims.items():
        select_clause += f" ?{claim_name}"
        where_clause += f"\n  OPTIONAL {{ ?item wdt:{claim_id} ?{claim_name} }}"
    where_clause += "\n}"

    return f"{select_clause}\n{where_clause}"


def execute_sparql_query(endpoint_url, query, limit, offset, email=None):
    full_query = f"{query} LIMIT {limit} OFFSET {offset}"
    headers = {"User-Agent": "ror_wikidata_claim_overlap"}

    if email:
        headers["From"] = email

    try:
        response = requests.get(
            endpoint_url,
            params={"query": full_query, "format": "json"},
            headers=headers
        )
        response.raise_for_status()
        return response.json()["results"]["bindings"]
    except requests.RequestException as e:
        logger.error(f"Error executing SPARQL query: {e}")
        raise


def process_wikidata_results(results, claims):
    processed_data = {}
    for result in results:
        ror_id = result["rorID"]["value"]
        wikidata_id = result["item"]["value"].split("/")[-1]
        processed_data[ror_id] = {
            "wikidata_id": wikidata_id,
            **{claim_name: result.get(claim_name, {}).get("value")
               for claim_name in claims.values()}
        }
    return processed_data


def generate_csv_files(data, output_directory, claims):
    os.makedirs(output_directory, exist_ok=True)
    for claim_id, claim_name in claims.items():
        output_file = os.path.join(output_directory, f"{claim_name}_mapping.csv")
        with open(output_file, "w", encoding="utf-8") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(["ROR ID", "Wikidata ID", claim_name])
            for ror_id, values in data.items():
                if values[claim_name]:
                    writer.writerow(
                        [ror_id, values["wikidata_id"], values[claim_name]])
        logger.info(f"Generated CSV file: {output_file}")


def worker(endpoint_url, query, claims, limit, offset, email):
    try:
        results = execute_sparql_query(
            endpoint_url, query, limit, offset, email)
        return process_wikidata_results(results, claims)
    except Exception as e:
        logger.error(f"Error in worker (offset {offset}): {e}")
        return {}


def main():
    args = parse_arguments()
    try:
        claims = parse_json_claims(args.input_file)
        query = generate_sparql_query(claims)
        pool = multiprocessing.Pool(5)
        worker_func = partial(worker, args.endpoint, query,
                              claims, args.limit, email=args.email)
        offsets = range(args.offset, args.offset + args.limit * 20, args.limit)
        results = pool.map(worker_func, offsets)
        all_data = {}
        for result in results:
            all_data.update(result)
        generate_csv_files(all_data, args.output_directory, claims)
        logger.info(f"CSV files have been generated in {args.output_directory}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        if 'pool' in locals():
            pool.close()
            pool.join()


if __name__ == "__main__":
    main()
