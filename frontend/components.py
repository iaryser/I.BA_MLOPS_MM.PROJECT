import altair as alt
import pandas as pd
import streamlit as st
from api_client import get_coin_context

from inference.schemas import PredictionResponse, TopCoin


def render_volume_badge(volume: float) -> None:
    st.markdown(
        f"""
        <div style="
            display: inline-block;
            background-color: rgba(255, 229, 229, 0.12);
            color: #ff6b6b;
            border: 1px solid rgba(255, 107, 107, 0.35);
            padding: 0.35rem 0.65rem;
            border-radius: 999px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-bottom: 0.5rem;
        ">
            traded a lot · volume: ${volume:,.0f}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_price_chart(
    df: pd.DataFrame,
    *,
    n_days: int,
    height: int = 220,
) -> None:
    if df.empty:
        st.warning("No chart data available.")
        return

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    if n_days <= 2:
        x_format = "%H:%M"
        tick_count = 6
    elif n_days <= 7:
        x_format = "%d %b"
        tick_count = 7
    else:
        x_format = "%d %b"
        tick_count = 6

    chart = (
        alt.Chart(df)
        .mark_line(
            color="#9aa0a6",
            strokeWidth=2.4,
            interpolate="linear",
        )
        .encode(
            x=alt.X(
                "timestamp:T",
                title=None,
                axis=alt.Axis(
                    format=x_format,
                    labelAngle=0,
                    labelColor="#b8c7d9",
                    tickColor="#3c4043",
                    domain=False,
                    grid=False,
                    tickCount=tick_count,
                ),
            ),
            y=alt.Y(
                "price:Q",
                title=None,
                scale=alt.Scale(zero=False),
                axis=alt.Axis(
                    labelColor="#b8c7d9",
                    tickColor="#3c4043",
                    domain=False,
                    grid=True,
                    gridColor="#2f3338",
                    gridOpacity=0.8,
                    format=",.4f",
                ),
            ),
            tooltip=[
                alt.Tooltip("timestamp:T", title="Time", format="%d %b %Y, %H:%M"),
                alt.Tooltip("price:Q", title="Price", format=",.6f"),
            ],
        )
        .properties(height=height)
        .configure_view(strokeWidth=0)
        .configure_axis(labelFontSize=12)
    )

    st.altair_chart(chart, use_container_width=True)


def render_coin_card(coin: TopCoin, n_days: int) -> None:
    with st.container(border=True):
        st.markdown(f"### {coin.coin_id.capitalize()}")

        render_volume_badge(coin.volume)

        df = get_coin_context(coin_id=coin.coin_id, n_days=n_days)
        render_price_chart(df, n_days=n_days, height=220)

        if st.button(
            f"Get prediction for {coin.coin_id.capitalize()}",
            key=f"predict_{coin.coin_id}",
        ):
            st.session_state["selected_coin"] = coin.coin_id
            st.switch_page("pages/prediction.py")


def render_prediction_card(prediction: PredictionResponse) -> None:
    col1, col2, col3, col4 = st.columns(4)

    timestamp = pd.to_datetime(prediction.timestamp)

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")

    timestamp = timestamp.tz_convert("Europe/Zurich")
    timestamp = timestamp.strftime("%d %b %Y %H:%M %Z")

    with col1:
        st.metric("Coin", prediction.coin_id.upper())

    with col2:
        st.metric("Direction over next 24h", prediction.direction)

    with col3:
        if prediction.direction == "up":
            st.metric("Probability up", f"{prediction.probability_up:.1%}")
        else:
            st.metric("Probability down", f"{1 - prediction.probability_up:.1%}")

    with col4:
        st.metric(
            "Model version", f"{prediction.model_alias}-{prediction.model_version}"
        )

    st.caption(f"Prediction generated at {timestamp}")
