import pandas as pd
from datetime import datetime, timedelta
from main import connect
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.extras import execute_values
import requests
from datetime import datetime

def get_dates(date, days):

    """
    Description:
    Generates a range of dates ending at a specified date.

    Parameters:
    - date (datetime): The end date of the date range.
    - days (int): number of days

    Returns:
    pd.DatetimeIndex: A pandas DatetimeIndex object containing the generated dates.
    """
    dates = pd.date_range(end=date, periods=days, freq = 'D')
    return dates

def calculate_symbol_value(symbol, quantity, date, connection):
    """
    Description:
    Calculates the value of a symbol in the portfolio on a specific date.

    Parameters:
    - symbol (str): Symbol of the cryptocurrency.
    - quantity (float): Quantity of the cryptocurrency.
    - date (datetime): Date of calculation.
    - connection: The PostgreSQL connection object.

    Returns:
    float: Value of the symbol.
    """
    cursor = connection.cursor()
    
    # Convert the date to a plain date for querying historical data
    date_only = date.date()
    current_date = datetime.now().date()
    value = 0

    try:
        if date_only == current_date:
            # Query the current price from the `coins.coin_list` table
            query = """
            SELECT current_price
            FROM market.data
            WHERE symbol = %s
            """
            cursor.execute(query, (symbol,))
            result = cursor.fetchone()
            if result:
                current_price = result[0]
                value = current_price * quantity

        else:
            # Query historical data for the specific date from the `historical_data.{symbol}` table
            query = f"""
            SELECT price
            FROM historical_data.{symbol}
            WHERE date::date = %s
            """
            cursor.execute(query, (date_only,))
            result = cursor.fetchone()
            if result:
                historical_price = result[0]
                value =float(historical_price) * quantity

    except Exception as e:
        print(f"An error occurred: {e}")
    
    return value

def get_portfolio_on_date(date, connection):
    """
    Retrieves portfolio data for a specific date using SQL.

    Parameters:
    - date (datetime): The date and time to retrieve portfolio data up to.
    - connection: The PostgreSQL connection object.

    Returns:
    dict: Portfolio data for the specified timestamp.
    """
    cursor = connection.cursor()
    
    # SQL query to get transactions data up to the specified timestamp
    query = """
        WITH received_data AS (
        SELECT
            received_currency AS symbol,
            SUM(CASE WHEN received_currency IS NOT NULL THEN received_quantity ELSE 0 END) AS total_received_quantity,
            SUM(CASE WHEN received_currency IS NOT NULL THEN received_cost_basis ELSE 0 END) AS total_received_cost_basis
        FROM transactions.transactions
        WHERE date <= %s
        GROUP BY received_currency
    ),
    sent_data AS (
        SELECT
            sent_currency AS symbol,
            SUM(CASE WHEN sent_currency IS NOT NULL THEN sent_quantity ELSE 0 END) AS total_sent_quantity,
            SUM(CASE WHEN sent_currency IS NOT NULL THEN sent_cost_basis ELSE 0 END) AS total_sent_cost_basis
        FROM transactions.transactions
        WHERE date <= %s
        GROUP BY sent_currency
    )
    SELECT
        COALESCE(received_data.symbol, sent_data.symbol) AS symbol,
        COALESCE(total_received_quantity, 0) - COALESCE(total_sent_quantity, 0) AS remaining_quantity,
        COALESCE(total_received_cost_basis, 0) - COALESCE(total_sent_cost_basis, 0) AS remaining_cost_basis
    FROM received_data
    FULL OUTER JOIN sent_data ON received_data.symbol = sent_data.symbol
    WHERE COALESCE(total_received_quantity, 0) - COALESCE(total_sent_quantity, 0) > 0
    AND COALESCE(total_received_cost_basis, 0) - COALESCE(total_sent_cost_basis, 0) > 1;
    """
    
    # Execute the query with the specified timestamp
    cursor.execute(query, (date, date))
    
    # Fetch the results
    results = cursor.fetchall()
    
    # Close the cursor, connection should be managed outside this function
    cursor.close()
    
    # Process the results to construct the portfolio data
    symbol_data = {}
    for row in results:
        symbol, remaining_quantity, remaining_cost_basis = row
        # Assuming calculate_symbol_value uses remaining_quantity to determine the value
        value = calculate_symbol_value(symbol.lower(), remaining_quantity, date, connection)
        if value > 0:
            symbol_data[symbol] = {
                "quantity": remaining_quantity,
                "value": value,
                "cost_basis": remaining_cost_basis
            }
    
    return symbol_data if symbol_data else None

def populate_portfolio(dates, connection):
    """
    Populates the portfolio table with data for each given date.

    Parameters:
    - dates (list of datetime): A list of dates for which the portfolio data is to be populated.
    - connection: The PostgreSQL connection object.

    Returns:
    None
    """
    cursor = connection.cursor()
    cursor.execute("DELETE FROM portfolio.portfolio")
    connection.commit()
    print("Existing data in portfolio.portfolio cleared.")
    
    try:
        for date in dates:
            # Retrieve the portfolio data for the current date
            portfolio_data = get_portfolio_on_date(date, connection)

            if portfolio_data:
                # For each symbol in the portfolio data, insert the data into the portfolio table
                for symbol, data in portfolio_data.items():
                    quantity = data["quantity"]
                    value = data["value"]
                    cost_basis = data["cost_basis"]
                    
                    # Insert data into the portfolio table
                    query = """
                        INSERT INTO portfolio.portfolio (date, coin, quantity, value, cost_basis)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (date, coin) DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            value = EXCLUDED.value,
                            cost_basis = EXCLUDED.cost_basis;
                    """
                    cursor.execute(query, (date.date(), symbol, quantity, value, cost_basis))
            
            # Commit after processing each date
            connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()  # Rollback in case of error to maintain data integrity
    
    finally:
        cursor.close()

def populate_value(connection):
    """
    Populates the portfolio.value table with the sum of values for each date.
    
    Parameters:
    - connection: The PostgreSQL connection object.
    
    Returns:
    None
    """
    cursor = connection.cursor()
    
    try:
        # Query to sum the total value for each date
        query = """
        SELECT date, SUM(value) AS total_value
        FROM portfolio.portfolio
        GROUP BY date
        ORDER BY date;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Insert the summed values into the portfolio.value table
        for row in results:
            date, total_value = row
            insert_query = """
            INSERT INTO portfolio.value (date, value)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET value = EXCLUDED.value;
            """
            cursor.execute(insert_query, (date, total_value))
        
            connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()
    
    finally:
        cursor.close()

def populate_cost_basis(connection):
    """
    Populates the portfolio.cost_basis table with the sum of cost_basis for each date.
    
    Parameters:
    - connection: The PostgreSQL connection object.
    
    Returns:
    None
    """
    cursor = connection.cursor()
    
    try:
        # Query to sum the total cost_basis for each date
        query = """
        SELECT date, SUM(cost_basis) AS total_cost_basis
        FROM portfolio.portfolio
        GROUP BY date
        ORDER BY date;
        """
        cursor.execute(query)
        results = cursor.fetchall()
        
        # Insert the summed cost_basis into the portfolio.cost_basis table
        for row in results:
            date, total_cost_basis = row
            insert_query = """
            INSERT INTO portfolio.cost_basis (date, cost_basis)
            VALUES (%s, %s)
            ON CONFLICT (date) DO UPDATE SET cost_basis = EXCLUDED.cost_basis;
            """
            cursor.execute(insert_query, (date, total_cost_basis))
        
        connection.commit()

    except Exception as e:
        print(f"An error occurred: {e}")
        connection.rollback()
    
    finally:
        cursor.close()



def main():
    connection = connect()

    # dates = get_dates(datetime.today(), 365)
    # populate_portfolio(dates, connection)
    # populate_cost_basis(connection)
    populate_value(connection)


    connection.close

if __name__ == "__main__":
    main()