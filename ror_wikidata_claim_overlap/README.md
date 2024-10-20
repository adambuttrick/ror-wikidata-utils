# ROR Wikidata Claim Overlap

Generates CSV files mapping ROR IDs to Wikidata claim values for specified properties, including the corresponding Wikidata ID for each entry.

## Installation

```
pip install -r requirements.txt
```

## Usage

```
python ror_wikidata_claim_overlap.py -i <input_file.json> -d <output_directory> [-e <endpoint_url>] [-l <limit>] [-o <offset>] [--email <email_address>]
```

Arguments:
- `-i`, `--input_file`: Path to JSON file containing claim IDs and names (required)
- `-d`, `--output_directory`: Directory to store output CSV files (default: 'ror_wikidata_claims')
- `-e`, `--endpoint`: SPARQL endpoint URL (default: 'https://query.wikidata.org/sparql')
- `-l`, `--limit`: LIMIT value for SPARQL query (default: 10000)
- `-o`, `--offset`: Initial OFFSET value for SPARQL query (default: 0)
- `--email`: Email address to include in request header (optional)

## Input

The input JSON file should contain Wikidata property IDs and their corresponding human-readable names. For example:

```json
{
    "P281": "postalCode",
    "P3548":"australianBusinessNumber"
}
```

## Output

The script generates multiple CSV files in the specified output directory, one for each claim in the input JSON. Each CSV file contains three columns:
- ROR ID
- Wikidata ID
- The corresponding claim value (e.g., appleMapID, facebookLocID, etc.)


