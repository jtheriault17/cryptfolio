from main import connect
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values
import requests
from datetime import datetime
import time

def safe_float(value):
    return float(value) if value is not None else 0.0

def populate_market_data(ids, connection):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {"x-cg-demo-api-key": "CG-U3VbGJ3KKNE5dVgvttoKb1dv"}
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": "250",
        "page": "1",
        "sparkline": "false",
        "price_change_percentage": "1h,24h,7d,14d,30d,200d,1y",
        "locale": "en",
        "precision": "18",
        "ids": ids
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        json_data = response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return

    cursor = connection.cursor()

    # Clear existing data
    cursor.execute("DELETE FROM market.data")
    connection.commit()
    print("Existing data in market.data cleared.")

    # Insert query for all fields
    insert_query = """
        INSERT INTO market.data (
            market_cap_rank, id, symbol, name, image, current_price, market_cap, 
            fully_diluted_valuation, total_volume, 
            high_24h, low_24h, price_change_24h, price_change_percentage_24h, 
            market_cap_change_24h, market_cap_change_percentage_24h, 
            circulating_supply, total_supply, max_supply, ath, 
            ath_change_percentage, ath_date, atl, atl_change_percentage, 
            atl_date, roi, last_updated,
            price_change_percentage_1h_in_currency,
            price_change_percentage_7d_in_currency,
            price_change_percentage_14d_in_currency,
            price_change_percentage_30d_in_currency,
            price_change_percentage_200d_in_currency,
            price_change_percentage_1y_in_currency
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
    """

    for coin in json_data:
        # Convert timestamp strings to datetime objects for TIMESTAMP fields
        ath_date = datetime.fromisoformat(coin.get("ath_date", "").replace("Z", "")) if coin.get("ath_date") else None
        atl_date = datetime.fromisoformat(coin.get("atl_date", "").replace("Z", "")) if coin.get("atl_date") else None
        last_updated = datetime.fromisoformat(coin.get("last_updated", "").replace("Z", "")) if coin.get("last_updated") else None

        # Retrieve and ensure numeric fields are properly cast as None or their appropriate type
        cursor.execute(insert_query, (
            coin.get("market_cap_rank"),
            coin["id"],
            coin["symbol"],
            coin["name"],
            coin["image"],
            safe_float(coin.get("current_price")),
            safe_float(coin.get("market_cap")),
            safe_float(coin.get("fully_diluted_valuation")),
            safe_float(coin.get("total_volume")),
            safe_float(coin.get("high_24h")),
            safe_float(coin.get("low_24h")),
            safe_float(coin.get("price_change_24h")),
            safe_float(coin.get("price_change_percentage_24h")),
            safe_float(coin.get("market_cap_change_24h")),
            safe_float(coin.get("market_cap_change_percentage_24h")),
            safe_float(coin.get("circulating_supply")),
            safe_float(coin.get("total_supply")),
            safe_float(coin.get("max_supply")),
            safe_float(coin.get("ath")),
            safe_float(coin.get("ath_change_percentage")),
            coin.get("ath_date"),
            safe_float(coin.get("atl")),
            safe_float(coin.get("atl_change_percentage")),
            coin.get("atl_date"),
            None if coin.get("roi") is None else safe_float(coin.get("roi", {}).get("percentage")),
            coin.get("last_updated"),
            safe_float(coin.get("price_change_percentage_1h_in_currency")),
            safe_float(coin.get("price_change_percentage_7d_in_currency")),
            safe_float(coin.get("price_change_percentage_14d_in_currency")),
            safe_float(coin.get("price_change_percentage_30d_in_currency")),
            safe_float(coin.get("price_change_percentage_200d_in_currency")),
            safe_float(coin.get("price_change_percentage_1y_in_currency"))
        ))

    connection.commit()
    print("Market data populated successfully.")

def populate_coin_list(connection):
    url = "https://api.coingecko.com/api/v3/coins/list"
    headers = {"x-cg-demo-api-key": "CG-U3VbGJ3KKNE5dVgvttoKb1dv"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        json_data = response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return

    cursor = connection.cursor()
    # Clear existing data
    cursor.execute("DELETE FROM coins.coin_list")
    connection.commit()
    print("Existing data in coins.coin_list cleared.")

    # Insert query for all fields
    insert_query = """
        INSERT INTO coins.coin_list (
            id, symbol, name
        ) VALUES (
            %s, %s, %s
        )
    """

    for coin in json_data:
        # Convert timestamp strings to datetime objects for TIMESTAMP fields
        ath_date = datetime.fromisoformat(coin.get("ath_date", "").replace("Z", "")) if coin.get("ath_date") else None
        atl_date = datetime.fromisoformat(coin.get("atl_date", "").replace("Z", "")) if coin.get("atl_date") else None
        last_updated = datetime.fromisoformat(coin.get("last_updated", "").replace("Z", "")) if coin.get("last_updated") else None

        # Retrieve and ensure numeric fields are properly cast as None or their appropriate type
        cursor.execute(insert_query, (
            coin["id"],
            coin["symbol"],
            coin["name"]
        ))

    connection.commit()
    print("Coin list populated successfully.")

def get_coin_id(symbol, connection):
    """
    Description:
    Retrieves the coin ID for a given symbol from the coins.coin_list table.
    If there are multiple matches, prompts the user to choose.

    Parameters:
    - symbol (str): The symbol of the coin.
    - connection: The PostgreSQL connection object.

    Returns:
    str or None: The coin ID if found, or None if not found.
    """
    # Initialize a cursor and execute query    
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        # Check if the coin ID is already cached in user_coins
        query = "SELECT id FROM coins.user_coins WHERE symbol = %s"
        cursor.execute(query, (symbol, ))
        result = cursor.fetchone()

        if result:
            return result['id']
        
        # If not cached, look up in coins list for matching symbols
        cursor.execute("SELECT id, name FROM coins.coin_list WHERE symbol = %s", (symbol,))
        matching_coins = cursor.fetchall()
        if not matching_coins:
            print(f"No id found for {symbol}")
            return None # No coins found with this symbol
        
        elif len(matching_coins) == 1:
            # Single match found, add to user_coins and return
            id = matching_coins[0]['id']
            cursor.execute(
                "INSERT INTO coins.user_coins  (symbol, id) VALUES (%s, %s)",
                (symbol, id)
            )
            connection.commit()
            return id

        else:
            # Multiple matches found, ask user to select
            print(f"Multiple coins found for symbol '{symbol}':")
            for i, (id, name) in enumerate(matching_coins, 1):
                print(f"{i}. {matching_coins[i-1]['name']}")
                
            choice = input("Enter the number corresponding to the desired coin: ")
            while not choice.isdigit() or int(choice) < 1 or int(choice) > len(matching_coins):
                choice = input("Invalid input. Please enter a valid number: ")
                
            selected_id = matching_coins[int(choice) - 1]['id']
            
            # Cache the selected coin ID in the user_coins
            cursor.execute(
                "INSERT INTO coins.user_coins (symbol, id) VALUES (%s, %s) ON CONFLICT (symbol) DO UPDATE SET id = EXCLUDED.id",
                (symbol, selected_id)
            )
            connection.commit()
            cursor.close()
            return selected_id
        
def populate_user_coins(connection):
    """
    Description:
    Populates the coins.user_coins table with unique symbols from the 
    transactions.transactions table and their corresponding IDs from 
    coins.coin_list. Each unique symbol in transactions is 
    matched with its ID, and both are inserted as rows in user_coins.

    Parameters:
    - connection: The PostgreSQL connection object.
    """
    try:
        cursor = connection.cursor()
        # string for market data parameter
        ids = ""

        # Fetch unique symbols from transactions
        cursor.execute("SELECT DISTINCT LOWER(received_currency) AS symbol FROM transactions.transactions")
        symbols = [row[0] for row in cursor.fetchall()]

        # Insert each symbol into user_coins with its corresponding ID
        for symbol in symbols:
            if symbol == None:
                continue
            id = get_coin_id(symbol, connection)  # Retrieve the ID using the get_coin_id function and insert into coins.user_coins
            if id:
                ids = ids + f"{id},"
        if ids.endswith(','):
            ids = ids[:-1]

    # Commit the transaction
        connection.commit()
        print("User coins populated successfully.")
        return ids
    except Exception as e:
        print(f"An error occurred while populating user coins: {e}")
        connection.rollback()
    finally:
        # Close the cursor and connection
        cursor.close()

def fetch_and_store_coin_data(coin_id, symbol, connection):
    """
    Description:
    Fetches historical data for a given coin from an API and stores it in the SQL database.

    Parameters:
    - connection (psycopg2.connection): Active connection to the PostgreSQL database.
    - coin_id (str): The ID of the coin.
    """
    # Fetch data from the API with retry for rate limiting
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=usd&days=365&interval=daily&precision=8"
    headers = {"x-cg-demo-api-key": "CG-U3VbGJ3KKNE5dVgvttoKb1dv"}
    json_data = None
    retries = 3  # Number of retries in case of throttling
    for attempt in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            try:
                json_data = response.json()
                break  # Exit loop on successful retrieval
            except requests.exceptions.JSONDecodeError:
                print("Error: Unable to decode JSON. Response content:", response.text)
                return  # Exit function on JSON decode error
        elif response.status_code == 429:
            print(f"Received status code 429 (Throttled), retrying in {60 * (attempt + 1)} seconds...")
            time.sleep(60 * (attempt + 1))  # 1 min, 2 min, 3 min
        else:
            print(f"Error: Received status code {response.status_code} from API")
            print("Response content:", response.text)
            return  # Exit function on non-429 error

    # If json_data is still None, exit the function
    if json_data is None:
        print(f"Failed to fetch data for {coin_id} after {retries} attempts.")
        return

    # Extract data into lists for easy handling
    prices = json_data.get("prices", [])
    market_caps = json_data.get("market_caps", [])
    total_volumes = json_data.get("total_volumes", [])

    # Create the table for the coin if it doesn't already exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS historical_data.{symbol} (
        date TIMESTAMP PRIMARY KEY,
        price NUMERIC,
        market_cap NUMERIC,
        total_volume NUMERIC
    );
    """
    with connection.cursor() as cursor:
        cursor.execute(create_table_query)

    # Prepare the insert query
    insert_query = f"""
    INSERT INTO historical_data.{symbol} (date, price, market_cap, total_volume)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (date) DO UPDATE
    SET price = EXCLUDED.price, 
        market_cap = EXCLUDED.market_cap, 
        total_volume = EXCLUDED.total_volume;
    """

    # Process and insert data row by row
    with connection.cursor() as cursor:
        for i in range(len(prices)):
            timestamp = datetime.fromtimestamp(prices[i][0] / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
            price = prices[i][1]
            market_cap = market_caps[i][1] if i < len(market_caps) else None
            total_volume = total_volumes[i][1] if i < len(total_volumes) else None

            cursor.execute(insert_query, (timestamp, price, market_cap, total_volume))

    # Commit changes to the database
    connection.commit()
    print(f"Data for {coin_id} has been successfully stored.")


def fetch_and_store_data_for_all_coins(connection):
    """
    Fetches and stores data for all coins in the user_coins table.
    
    Parameters:
    - conn: The PostgreSQL database connection object.
    """
    cursor = connection.cursor()
    # Retrieve all coin_id from user_coins
    cursor.execute("SELECT symbol, id FROM coins.user_coins;")
    coins = cursor.fetchall()

    for symbol, coin_id in coins:
        print(f"Fetching and storing data for {symbol} (ID: {coin_id})...")
        fetch_and_store_coin_data(coin_id, symbol, connection)

    cursor.close()

def main():
    connection = connect()

    populate_coin_list(connection)
    ids = populate_user_coins(connection)
    populate_market_data(ids, connection)
    fetch_and_store_data_for_all_coins(connection)

    connection.close

if __name__ == "__main__":
    main()