import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ─── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="SocialPulse",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Base ── */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ── Background ── */
    .stApp {
        background: #0a0e1a;
        color: #e2e8f0;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background: #0f1628 !important;
        border-right: 1px solid #1e2a45;
    }
    [data-testid="stSidebar"] * {
        color: #94a3b8 !important;
    }
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #e2e8f0 !important;
    }

    /* ── Metric cards ── */
    [data-testid="metric-container"] {
        background: #0f1628;
        border: 1px solid #1e2a45;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="metric-container"] label {
        color: #64748b !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    [data-testid="metric-container"] [data-testid="stMetricValue"] {
        color: #f1f5f9 !important;
        font-family: 'Space Grotesk', sans-serif !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }

    /* ── Section headers ── */
    h2, h3 {
        font-family: 'Space Grotesk', sans-serif !important;
        color: #f1f5f9 !important;
        font-weight: 700 !important;
    }

    /* ── Divider ── */
    hr {
        border-color: #1e2a45 !important;
        margin: 1.5rem 0 !important;
    }

    /* ── Dataframe ── */
    [data-testid="stDataFrame"] {
        border: 1px solid #1e2a45;
        border-radius: 12px;
        overflow: hidden;
    }

    /* ── Pill badge ── */
    .pill {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 999px;
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        text-transform: uppercase;
    }
    .pill-hot      { background: #ff4b4b22; color: #ff4b4b; border: 1px solid #ff4b4b44; }
    .pill-trending { background: #ff9f4322; color: #ff9f43; border: 1px solid #ff9f4344; }
    .pill-active   { background: #1dd1a122; color: #1dd1a1; border: 1px solid #1dd1a144; }
    .pill-normal   { background: #a29bfe22; color: #a29bfe; border: 1px solid #a29bfe44; }

    /* ── Title area ── */
    .dash-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2rem;
        font-weight: 700;
        color: #f1f5f9;
        line-height: 1.1;
    }
    .dash-subtitle {
        font-size: 0.85rem;
        color: #475569;
        margin-top: 4px;
    }
    .accent { color: #6366f1; }
</style>
""", unsafe_allow_html=True)

# ─── DB Connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost", port=5432,
        dbname="socialpulse",
        user="socialpulse", password="socialpulse123",
    )

@st.cache_data(ttl=30)
def query(sql):
    return pd.read_sql(sql, get_connection())

# ─── Load Data ─────────────────────────────────────────────────────────────────
hashtags_df   = query("SELECT * FROM mart_trending_topics ORDER BY rank")
engagement_df = query("SELECT * FROM raw_user_engagement ORDER BY user_rank")
trends_df     = query("SELECT * FROM raw_hourly_trends ORDER BY hour_of_day")
kpi_df        = query("""
    SELECT
        COUNT(*)                                    AS total_users,
        SUM(total_posts)                            AS total_posts,
        SUM(viral_posts)                            AS viral_posts,
        ROUND(AVG(avg_engagement_score)::numeric,1) AS avg_engagement
    FROM raw_user_engagement
""")

# ─── Sidebar Filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ SocialPulse")
    st.markdown("---")

    st.markdown("### 🎛️ Filters")

    # Engagement tier filter
    all_tiers = hashtags_df["engagement_tier"].unique().tolist()
    selected_tiers = st.multiselect(
        "Engagement Tier",
        options=all_tiers,
        default=all_tiers,
    )

    # Top N hashtags
    top_n = st.slider("Top N Hashtags", min_value=3, max_value=18, value=10, step=1)

    # Min mentions filter
    min_mentions = st.number_input(
        "Min Mentions",
        min_value=0,
        max_value=int(hashtags_df["total_mentions"].max()),
        value=0,
        step=100,
    )

    st.markdown("---")
    st.markdown("### 👤 User Filters")

    # Location filter
    all_locations = sorted(engagement_df["primary_location"].dropna().unique().tolist())
    selected_locations = st.multiselect(
        "Location",
        options=all_locations,
        default=[],
        placeholder="All locations",
    )

    # Min engagement
    min_engagement = st.slider(
        "Min Engagement Score",
        min_value=0,
        max_value=int(engagement_df["total_engagement_score"].max()),
        value=0,
        step=1000,
    )

    st.markdown("---")
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(f"<div style='color:#334155;font-size:0.7rem;margin-top:8px'>Updated {datetime.now().strftime('%H:%M:%S')}</div>", unsafe_allow_html=True)

# ─── Apply Filters ─────────────────────────────────────────────────────────────
filtered_hashtags = hashtags_df[
    (hashtags_df["engagement_tier"].isin(selected_tiers)) &
    (hashtags_df["total_mentions"] >= min_mentions)
].head(top_n)

filtered_users = engagement_df[
    engagement_df["total_engagement_score"] >= min_engagement
]
if selected_locations:
    filtered_users = filtered_users[filtered_users["primary_location"].isin(selected_locations)]
filtered_users = filtered_users.head(20)

# ─── Plotly theme ──────────────────────────────────────────────────────────────
PLOT_BG    = "#0a0e1a"
PAPER_BG   = "#0f1628"
GRID_COLOR = "#1e2a45"
TEXT_COLOR = "#94a3b8"
FONT       = "Inter"

def base_layout(height=380):
    return dict(
        height=height,
        plot_bgcolor=PLOT_BG,
        paper_bgcolor=PAPER_BG,
        font=dict(family=FONT, color=TEXT_COLOR, size=12),
        margin=dict(l=10, r=10, t=20, b=10),
        xaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR),
        yaxis=dict(gridcolor=GRID_COLOR, zerolinecolor=GRID_COLOR),
    )

# ─── Header ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="dash-title">
    Social<span class="accent">Pulse</span> &nbsp;⚡
</div>
<div class="dash-subtitle">
    Real-time social media analytics · Spark + Kafka + Delta Lake + dbt + Airflow
</div>
""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─── KPIs ──────────────────────────────────────────────────────────────────────
k = kpi_df.iloc[0]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("👤 Users",          f"{int(k['total_users']):,}")
c2.metric("📝 Posts",          f"{int(k['total_posts']):,}")
c3.metric("🔥 Viral Posts",    f"{int(k['viral_posts']):,}")
c4.metric("⚡ Avg Engagement", f"{k['avg_engagement']:,}")
c5.metric("🏷️ Hashtags",       f"{len(filtered_hashtags)}")

st.divider()

# ─── Row 1: Hashtag Bar + Tier Donut ──────────────────────────────────────────
col1, col2 = st.columns([3, 2])

with col1:
    st.markdown("### 🏷️ Trending Hashtags")
    if filtered_hashtags.empty:
        st.info("No hashtags match your filters.")
    else:
        fig = go.Figure(go.Bar(
            x=filtered_hashtags["total_mentions"],
            y=filtered_hashtags["hashtag"],
            orientation="h",
            marker=dict(
                color=filtered_hashtags["avg_engagement"],
                colorscale=[[0, "#312e81"], [0.5, "#6366f1"], [1, "#a78bfa"]],
                showscale=False,
            ),
            text=filtered_hashtags["total_mentions"],
            textposition="outside",
            textfont=dict(color=TEXT_COLOR, size=11),
        ))
        _layout = base_layout()
        _layout["yaxis"] = dict(autorange="reversed", gridcolor=GRID_COLOR)
        fig.update_layout(**_layout)
        st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### 🔥 Engagement Tiers")
    tier_counts = filtered_hashtags["engagement_tier"].value_counts().reset_index()
    tier_counts.columns = ["tier", "count"]
    color_map = {"HOT": "#ff4b4b", "TRENDING": "#ff9f43", "ACTIVE": "#1dd1a1", "NORMAL": "#a29bfe"}
    colors = [color_map.get(t, "#6366f1") for t in tier_counts["tier"]]

    fig2 = go.Figure(go.Pie(
        labels=tier_counts["tier"],
        values=tier_counts["count"],
        hole=0.55,
        marker=dict(colors=colors, line=dict(color=PLOT_BG, width=3)),
        textinfo="label+percent",
        textfont=dict(size=12, color="#e2e8f0"),
    ))
    fig2.update_layout(
        **base_layout(),
        showlegend=False,
        annotations=[dict(
            text="Tiers", x=0.5, y=0.5,
            font=dict(size=14, color="#94a3b8", family="Space Grotesk"),
            showarrow=False
        )],
    )
    st.plotly_chart(fig2, use_container_width=True)

st.divider()

# ─── Row 2: Viral Rate + Hourly Trends ────────────────────────────────────────
col3, col4 = st.columns([2, 3])

with col3:
    st.markdown("### 💫 Viral Rate %")
    fig3 = go.Figure(go.Bar(
        x=filtered_hashtags["hashtag"],
        y=filtered_hashtags["viral_rate_pct"],
        marker=dict(
            color=filtered_hashtags["viral_rate_pct"],
            colorscale=[[0, "#1e1b4b"], [1, "#f43f5e"]],
            showscale=False,
        ),
        text=filtered_hashtags["viral_rate_pct"].apply(lambda x: f"{x}%"),
        textposition="outside",
        textfont=dict(color=TEXT_COLOR, size=10),
    ))
    _layout3 = base_layout(350)
    _layout3["xaxis"] = dict(tickangle=-40, gridcolor=GRID_COLOR)
    fig3.update_layout(**_layout3)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown("### 📈 Hourly Event Volume")
    if trends_df.empty:
        st.info("Let the simulator run across multiple hours to see trends.")
    else:
        event_colors = {
            "post":    "#6366f1",
            "like":    "#1dd1a1",
            "share":   "#ff9f43",
            "comment": "#a78bfa",
            "repost":  "#ff4b4b",
        }
        fig4 = go.Figure()
        for etype in trends_df["event_type"].unique():
            df_e = trends_df[trends_df["event_type"] == etype]
            fig4.add_trace(go.Bar(
                x=df_e["hour_of_day"],
                y=df_e["event_count"],
                name=etype.capitalize(),
                marker_color=event_colors.get(etype, "#6366f1"),
            ))
        _layout4 = base_layout(350)
        _layout4["barmode"] = "group"
        _layout4["legend"] = dict(
            orientation="h", y=-0.25,
            font=dict(color=TEXT_COLOR),
            bgcolor="rgba(0,0,0,0)",
        )
        fig4.update_layout(**_layout4)
        st.plotly_chart(fig4, use_container_width=True)

st.divider()

# ─── Row 3: Leaderboard ────────────────────────────────────────────────────────
st.markdown("### 👤 User Engagement Leaderboard")

if filtered_users.empty:
    st.info("No users match your filters.")
else:
    display = filtered_users[[
        "user_rank", "username", "total_posts",
        "total_engagement_score", "viral_posts",
        "primary_location", "devices_used"
    ]].rename(columns={
        "user_rank":              "Rank",
        "username":               "Username",
        "total_posts":            "Posts",
        "total_engagement_score": "Engagement Score",
        "viral_posts":            "🔥 Viral",
        "primary_location":       "Location",
        "devices_used":           "Devices",
    })

    st.dataframe(
        display,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Rank": st.column_config.NumberColumn(format="%d"),
            "Engagement Score": st.column_config.ProgressColumn(
                min_value=0,
                max_value=int(display["Engagement Score"].max()),
                format="%.0f",
            ),
            "🔥 Viral": st.column_config.NumberColumn(format="%d"),
        }
    )

st.divider()
st.markdown(
    "<div style='color:#1e293b;font-size:0.72rem;text-align:center'>"
    "SocialPulse · Apache Spark · Kafka · Delta Lake · dbt · Airflow · Streamlit"
    "</div>",
    unsafe_allow_html=True
)