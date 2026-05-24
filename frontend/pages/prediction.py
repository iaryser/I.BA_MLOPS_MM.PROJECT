import streamlit as st
from api_client import (
    check_server_health,
    get_available_coins,
    get_coin_context,
    predict_coin,
)
from components import render_prediction_card, render_price_chart


def main() -> None:

    st.set_page_config(
        page_title="Prediction",
        layout="wide",
    )

    if not check_server_health():
        st.error("Backend server is not running or unhealthy.")
        st.stop()

    st.title("Coin Prediction")

    coins = get_available_coins()

    default_coin = st.session_state.get("selected_coin")

    selected_coin = st.selectbox(
        "Select coin",
        options=coins,
        index=coins.index(default_coin) if default_coin in coins else 0,
    )

    st.session_state["selected_coin"] = selected_coin

    st.subheader(f"Recent price movement: {selected_coin.capitalize()}")

    df = get_coin_context(coin_id=selected_coin, n_days=7)

    if not df.empty:
        render_price_chart(df=df, n_days=7, height=320)
    else:
        st.warning("No chart data available.")

    st.divider()

    if st.button("Predict direction"):
        prediction = predict_coin(selected_coin)

        render_prediction_card(prediction)


if __name__ == "__main__":
    main()
