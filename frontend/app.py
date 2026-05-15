import streamlit as st

from api_client import check_server_health, get_available_coins, get_top5_coins
from components import render_coin_card


def main() -> None:
    st.set_page_config(
        page_title="Crypto Direction Prediction",
        layout="wide",
    )
    

    if not check_server_health():
        st.error("Backend server is not running or unhealthy.")
        st.stop()

    st.title("Crypto Direction Prediction App")

    st.markdown(
        """
        Welcome to the crypto direction prediction dashboard.  
        The app displays the most actively traded coins based on current trading volume
        and allows you to request a 24-hour short-term price direction prediction for any available coin.
        """
    )

    st.divider()

    st.subheader("Top 5 Most Traded Coins in the last 24 hours")

    top_5_coins = get_top5_coins()

    for coin in top_5_coins:
        render_coin_card(coin=coin, n_days=30)

    st.divider()

    st.subheader("Explore another coin")

    coins = get_available_coins()

    selected_coin = st.selectbox(
        "Choose a coin",
        options=coins,
        index=0,
    )

    if st.button("Open prediction page"):
        st.session_state["selected_coin"] = selected_coin
        st.switch_page("pages/prediction.py")


if __name__ == "__main__":
    main()