import requests
import json
import argparse
# Define the API Gateway endpoint URL
API_GATEWAY_URL = 'https://5c91p08g7b.execute-api.us-east-1.amazonaws.com/prod/execute-sql'
# Define the SQL query
parser = argparse.ArgumentParser(description='Execute SQL query via API Gateway and Lambda.')
parser.add_argument('sql_query', type=str, help='The SQL query to execute')
args = parser.parse_args()
sql_query = args.sql_query
# Create the request payload
payload = {
    "sql_query": sql_query
}
try:
    # Send the POST request to the API Gateway
    print(f"Applying query '{sql_query}' ...")
    response = requests.post(API_GATEWAY_URL, json=payload)
    
    # Check if the request was successful
    if response.status_code == 200:
        # Print the JSON response from the Lambda function
        print(f"--\nResponse:\n")
        response_str = json.dumps(response.json(), indent=4)
        print(eval(response_str))
    else:
        print(f"Failed to execute query. Status code: {response.status_code}")
        print(response.text)
except requests.exceptions.RequestException as e:
    print(f"An error occurred: {e}")

