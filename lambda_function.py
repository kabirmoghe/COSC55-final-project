import json
import boto3
import pymysql
import re
from botocore.exceptions import ClientError

# Basic MySQL query validation (can be expanded for more comprehensive checks)
def validate_sql_query(query):
    """
    Validate the SQL query to prevent SQL injection and ensure it's a valid SQL statement.
    This is a basic example; more comprehensive validation may be necessary for production use.
    """
    # Example regex to allow only SELECT, INSERT, UPDATE, and DELETE statements (CRUD as discussed in class)
    allowed_sql = re.compile(r'^\s*(SELECT|INSERT|UPDATE|DELETE)\s', re.IGNORECASE)
    
    if not allowed_sql.match(query):
        raise ValueError("Invalid or potentially dangerous SQL query provided.")

# Actual Lambda function
def lambda_handler(event, context):

    ### Retrieving DB credentials
    
    # Secret name
    secret_name = "rds!db-bd1e6aca-590d-4ac8-b748-2dc4b22e331b"
    region_name = "us-east-1"

    # Create a Secrets Manager client to retrieve secret 
    print("Creating Secrets Manager client")
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        print(f"Retrieved secret {secret}")

        # Parse the secret string (assuming it's in JSON format)
        db_credentials = json.loads(secret)
        db_host = "cosc55-project-instance.cbltjuc2kpbl.us-east-1.rds.amazonaws.com"
        db_user = db_credentials['username']
        db_password = db_credentials['password']
        db_port = 3306  # MySQL default port
        
    except ClientError as e:
        print(f"Error retrieving secret: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error retrieving secret: {e}")
        }
    except json.JSONDecodeError as e:
        print(f"Error parsing secret JSON: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error parsing secret JSON.')
        }

    ### Connecting to RDS MySQL DB
    
    # Current DB name
    demo_db_name = "demo_company_info"

    # Establish a connection to the MySQL database
    print("Establishing connection")
    
    try:
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=demo_db_name,
            port=db_port,
            connect_timeout=5
        )
    except pymysql.MySQLError as e:
        print(f"Error connecting to the MySQL database: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error connecting to the MySQL database: {e}")
        }
    
    ### Applying SQL query to RDS DB and returning response

    # Parsing query
    
    try:
        # The body is a string, so we need to parse it to JSON
        body = json.loads(event['body'])
        sql_query = body.get('sql_query')
        
        print(f"Retrieved query: {sql_query}")

        # No query passed in
        if not sql_query:
            return {
                'statusCode': 400,
                'body': json.dumps('No SQL query provided in the request.')
            }

        # Validate the SQL query
        validate_sql_query(sql_query)
    
    except json.JSONDecodeError as e:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid JSON input.')
        }
    except ValueError as e:
        return {
            'statusCode': 400,
            'body': json.dumps(str(e))
        }

    # Applying query
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)

            # Check if the SQL query is an INSERT, DELETE, or UPDATE to commit the transaction
            if re.match(r'^\s*(INSERT|DELETE|UPDATE)', sql_query, re.IGNORECASE):
                connection.commit()
                # Updated info on query execution
                result = f"Query executed successfully! {cursor.rowcount} row(s) affected."
            else:
                # For SELECT queries or queries that return data
                result = cursor.fetchall()
                # Convert the result to a list of dictionaries for visualization
                column_names = [desc[0] for desc in cursor.description]
                data = [dict(zip(column_names, row)) for row in result]

                # Visualizing data for output (currently hardcoded for following database and table)
                demo_db_name = "demo_company_info"
                table_name = "employees"
                data_str = f"Database: {demo_db_name}\n\tTable: {table_name}"

                for row in data:
                    data_str += f"\n\t\t{row}"

                print(data_str)

                return {
                    'statusCode': 200,
                    'body': json.dumps(data_str, default=str)
                }

        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
    
    except pymysql.MySQLError as e:
        print(f"Error executing SQL query: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing SQL query: {e}")
        }
    finally:
        # Ensure the connection is closed properly
        try:
            if connection:
                connection.close()
                print("Database connection closed.")
        except Exception as e:
            print(f"Error closing connection: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f"Error closing connection: {e}")
            }

