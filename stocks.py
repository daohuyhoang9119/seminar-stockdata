import streamlit as st
import yfinance as yf
import pandas as pd
import pymysql


def init_connection():
    config = st.secrets["tidb"]
    ssl_ca = config.get("ssl_ca", None)
    return pymysql.connect(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config["password"],
        database=config["database"],
        ssl_verify_cert=True,
        ssl_verify_identity=True,
        ssl_ca= ssl_ca
    )

conn = init_connection()

# Set up the Streamlit app
st.set_page_config(page_title="Historical Stock Data", page_icon="📈")

# Define the function to retrieve data from Yahoo Finance
def get_stock_data(symbol, start_date, end_date):
    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date, end=end_date)
    return data


# # Define function to fetch data from TiDB database
# def fetch_data(symbol1, symbol2, start_date, end_date):

#     cursor = conn.cursor()
#     query = f"""
#     SELECT Ticker, YEAR(Market_Date) AS Year, ROUND(SUM(Dividends), 2) AS Total_Dividends, CAST(ROUND(AVG(Volume), 2) AS DOUBLE) AS Avg_Volume
#     FROM stock_price_history
#     WHERE Ticker IN ('{symbol1}', '{symbol2}') AND Market_Date BETWEEN '{start_date}' AND '{end_date}'
#     GROUP BY Ticker, YEAR(Market_Date)
#     ORDER BY Ticker, YEAR(Market_Date) ASC;
#     """
#     cursor.execute(query)
#     data = cursor.fetchall()
#     cols = ['Ticker', 'Year', 'Total_Dividends', 'Avg_Volume']
#     df = pd.DataFrame(data, columns=cols)
#     return df

def save_data(data, symbol):
    data["Date"] = data.index
    data["Ticker"] = symbol
    data.reset_index(drop=True, inplace=True)

    df = data.loc[:, ['Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Date', 'Ticker']]

    cur = conn.cursor()

    data = [tuple(row) for row in df.itertuples(index=False)]

    query = 'INSERT INTO stock_price_history (Opening_Price, High, Low, Closing_Price,Volume, Dividends , Market_Date, Ticker) VALUES (  %s,  %s,  %s,  %s, %s,  %s, %s, %s);'
    cur.executemany(query, data)

    rows_upserted = cur.rowcount

    # commit the changes
    conn.commit()
    cur.close()
    conn.close()
    st.success( str(rows_upserted) +  " data saved successfully!")
    del st.session_state['data']


# Define the Streamlit app
def app():
    # Set up the tabs
    tabs = ["Collect Trade Data","About app"]
   

    st.sidebar.header("Menu")
    page = st.sidebar.radio(" ", tabs)


    # Collect Data tab  
    if page == "Collect Trade Data":
        st.header(":earth_americas: Collect Historical Stock Data")

        # Define the inputs
        symbol = st.text_input("Stock Name").upper()
        start_date = st.date_input("Start Date", value=pd.to_datetime("today").floor("D") - pd.offsets.DateOffset(years=10))
        end_date = st.date_input("End Date", value=pd.to_datetime("today").floor("D"))

        # Create two columns
        col1, col2, col3 = st.columns(3)

        # Define the button to retrieve the data
        if col1.button("Get Data"):
            data = get_stock_data(symbol, start_date, end_date)
            st.write(data)
            col3.write(f"Total Rows: {len(data)}")
            st.session_state.data = data

        # Define the button to save the data to TiDB
        if col2.button("Save Data"):
            if st.session_state.get("data") is None:
                st.write("No data to save.")
                return
            data = st.session_state.data
            save_data(data, symbol)

        

    elif page == "About app":
        st.header("⭐Tech stack")
        st.markdown('''
                  ✏️:red[Language:] Python.\n\n 📖:red[Libraries:] Pandas, JFinance, PyMysql.\n\n🤝:red[Database:] TiDB''')
       


app()
