import streamlit as st
import pandas as pd
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import to_date, datediff, current_date, col, when, count, date_format
import os
from datetime import datetime, timezone

st.set_page_config(page_title="Event Horizon Dashboard", page_icon="🎟️", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700&display=swap');

    /* General Body Styling */
    html, body, [class*="st-"] {
        font-family: 'Poppins', sans-serif;
    }
    .kpi-container {
        display: flex;
        justify-content: space-around;
        padding: 1.5rem;
        background-color: 
        border-radius: 15px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 30px -15px rgba(0,0,0,0.5);
    }
    .kpi-container .stMetric {
        background-color: rgba(255, 255, 255, 0.05);
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }
    .event-card {
        background-color: 
        border: 1px solid 
        border-radius: 15px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    .event-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 30px rgba(0, 0, 0, 0.4);
        border-color: 
    }
    .event-card h3 {
        color: 
        margin-top: 0;
        margin-bottom: 0.5rem;
        font-weight: 600;
    }
    .event-card .details-bar {
        font-size: 0.9rem;
        color: 
        margin-top: 0.5rem;
        margin-bottom: 1rem;
    }
    .demand-tag {
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.8rem;
        color: white;
        display: inline-block;
    }
    .demand-high { background-color: 
    .demand-medium { background-color: 
    .demand-low { background-color: 
            
    .notification-item {
        background-color: rgba(255, 255, 255, 0.08);
        padding: 0.75rem;
        border-radius: 8px;
        margin-bottom: 0.5rem;
        border-left: 4px solid 
    }
    .notification-item .event-name {
        font-weight: 600;
        color: 
    }
    .notification-item .message {
        font-size: 0.85rem;
        color: 
        font-style: italic;
    }

</style>
""", unsafe_allow_html=True)

if not st.session_state.get("authentication_status"):
    st.error("You must be logged in to view this page. Please return to the Home page to log in.")
    st.stop()

@st.cache_resource
def get_spark_session():
    
    jar_filename = "mongo-spark-connector_2.12-10.1.0-all.jar"
    spark_jars_path = r"C:\spark\spark-3.5.6-bin-hadoop3\jars"
    full_jar_path = os.path.join(spark_jars_path, jar_filename)
    if not os.path.exists(full_jar_path):
        st.error(f"FATAL ERROR: Spark JAR not found at {full_jar_path}")
        st.stop()
    spark = SparkSession.builder.appName("EventHorizonDashboard").master("local[*]").config("spark.jars", full_jar_path).config("spark.memory.offHeap.enabled", "true").config("spark.memory.offHeap.size", "4g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

@st.cache_resource
def load_models():
    
    try:
        popularity_model = PipelineModel.load("models/event_popularity_model")
    except Exception:
        st.warning("Could not load the Popularity Model.", icon="⚠️")
        popularity_model = None
    try:
        demand_model = PipelineModel.load("models/event_demand_model")
    except Exception:
        st.warning("Could not load the Demand Model.", icon="⚠️")
        demand_model = None
    return popularity_model, demand_model

@st.cache_resource
def get_mongo_client():
    
    return MongoClient("mongodb://127.0.0.1:27017/")

@st.cache_data(ttl=300)
def load_and_predict_all(_spark, _pop_model, _demand_model):
    
    client = get_mongo_client()
    db = client["event_pulse_db"]
    events_list = list(db["events"].find({}, {"_id": 0}))
    if not events_list: return pd.DataFrame()

    df = pd.DataFrame(events_list)
    df['avg_price'] = pd.to_numeric(df['avg_price'], errors='coerce')
    spark_df = _spark.createDataFrame(df)
    name_counts = spark_df.groupBy("name").agg(count("id").alias("event_name_count"))
    df_base_featured = spark_df.join(name_counts, on="name", how="left").na.fill(1, ["event_name_count"])
    df_base_featured = df_base_featured.withColumn("event_date_dt", to_date(col("event_date"), "yyyy-MM-dd")).withColumn("days_until_event", datediff(col("event_date_dt"), current_date())).withColumn("day_of_week", date_format(col("event_date_dt"), "E")).withColumn("is_weekend", when(col("day_of_week").isin(["Sat", "Sun"]), 1).otherwise(0))
    
    df_with_predictions = df_base_featured
    if _pop_model:
        pop_predictions_df = _pop_model.transform(df_base_featured)
        pop_results = pop_predictions_df.select("id", col("prediction").alias("popularity_prediction"))
        df_with_predictions = df_with_predictions.join(pop_results, "id", "left")
    else:
        df_with_predictions = df_with_predictions.withColumn("popularity_prediction", when(col("id").isNotNull(), 0.0))
    if _demand_model:
        demand_predictions_df = _demand_model.transform(df_base_featured)
        demand_results = demand_predictions_df.select("id", col("prediction").alias("demand_prediction"))
        df_with_predictions = df_with_predictions.join(demand_results, "id", "left")
    else:
        df_with_predictions = df_with_predictions.withColumn("demand_prediction", when(col("id").isNotNull(), 0.0))

    final_cols = ["id", "name", "segment", "genre", "venue_name", "city", "state", "event_date", "avg_price", "url", "popularity_prediction", "demand_prediction"]
    return df_with_predictions.select(*final_cols).toPandas()

@st.cache_data(ttl=300)
def load_recommendations_lookup(_spark):
    
    try:
        recs_df = _spark.read.parquet("models/event_recommender_data")
        recs_list = recs_df.select("id", col("recommendations.similar_id").alias("similar_ids")).collect()
        return {row['id']: row['similar_ids'] for row in recs_list}
    except Exception:
        st.warning("Recommendation data not found.", icon="⚠️")
        return {}

spark = get_spark_session()
popularity_model, demand_model = load_models()
client = get_mongo_client()
db = client["event_pulse_db"]
users_collection = db["users"]
notifications_collection = db["notifications"]

user_data = users_collection.find_one({"username": st.session_state["username"]})
user_favorites = user_data.get("favorite_event_ids", []) if user_data else []

df_predictions = load_and_predict_all(spark, popularity_model, demand_model)
recommendation_lookup = load_recommendations_lookup(spark)

def toggle_favorite(event_id, event_name, is_high_demand):
    
    username = st.session_state["username"]
    if event_id in user_favorites:
        users_collection.update_one({"username": username}, {"$pull": {"favorite_event_ids": event_id}})
        st.toast(f"💔 '{event_name}' removed from favorites.")
        user_favorites.remove(event_id)
    else:
        users_collection.update_one({"username": username}, {"$addToSet": {"favorite_event_ids": event_id}}, upsert=True)
        st.toast(f"❤️ '{event_name}' added to favorites!")
        user_favorites.append(event_id)
        if is_high_demand:
            notification_message = f"Heads up! '{event_name}' is a high-demand event."
            notifications_collection.insert_one({"username": username, "event_id": event_id, "event_name": event_name, "message": notification_message, "timestamp": datetime.now(timezone.utc), "is_read": False, "notification_type": "high_demand_favorite"})
            st.toast("🔥 Reminder sent for this high-demand event!")

def get_demand_tag(score):
    
    if score >= 60: return '<span class="demand-tag demand-high">High Risk</span>'
    if score >= 35: return '<span class="demand-tag demand-medium">Medium Demand</span>'
    return '<span class="demand-tag demand-low">Low Demand</span>'

with st.sidebar:
    st.header(f"Welcome, {st.session_state['name']}!")
    st.divider()
    st.header("Search & Filters ⚙️")
    
    search_term = st.text_input("Search by Event, City, or Venue", key="search_term", placeholder="e.g., 'Concert' or 'New York'")
    price_range = st.slider("Filter by Price ($)", 0, 1000, (0, 1000), 10, key="price_filter")
    
    if not df_predictions.empty:
        available_segments = sorted(df_predictions['segment'].unique())
        segment_filter = st.multiselect("Filter by Segment", available_segments, default=available_segments, key="segment_filter")
    else:
        segment_filter = []

    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.header("Notifications 🔔")
    user_notifications = list(notifications_collection.find({"username": st.session_state["username"], "is_read": False}).sort("timestamp", -1))
    if not user_notifications:
        st.info("No new notifications.")
    else:
        st.success(f"You have {len(user_notifications)} unread notifications!")
        for notif in user_notifications:
            st.markdown(f"""
            <div class="notification-item">
                <div class="event-name">{notif['event_name']}</div>
                <div class="message">{notif['message']}</div>
            </div>
            """, unsafe_allow_html=True)
        if st.button("Mark all as read", use_container_width=True, type="primary"):
            notifications_collection.update_many({"username": st.session_state["username"], "is_read": False}, {"$set": {"is_read": True}})
            st.toast("Notifications cleared!")
            st.rerun()

st.title("🎟️ Event Horizon Dashboard")
st.markdown("Your personalized compass for trending events. We predict demand so you never miss out.")

if df_predictions.empty:
    st.warning("No event data found. Please ensure your data ingestion pipeline is running.")
    st.stop()

user_prefs = users_collection.find_one({"username": st.session_state["username"]})
preferred_segments = user_prefs.get('favorite_segments', []) if user_prefs else []
recommended_event_ids = set()
if not preferred_segments:
    st.info("Go to the Profile page to set your preferences for personalized recommendations!", icon="👤")
    recommended_event_ids.update(df_predictions.nlargest(20, 'demand_prediction')['id'].tolist())
else:
    direct_match_ids = set(df_predictions[df_predictions['segment'].isin(preferred_segments)]['id'])
    recommended_event_ids.update(direct_match_ids)
    for event_id in direct_match_ids:
        if event_id in recommendation_lookup:
            recommended_event_ids.update(recommendation_lookup[event_id])

df_personalized = df_predictions[df_predictions['id'].isin(recommended_event_ids)].copy()

df_filtered = df_personalized[
    (df_personalized['avg_price'].between(price_range[0], price_range[1])) &
    (df_personalized['segment'].isin(segment_filter))
]
if search_term:
    df_filtered = df_filtered[df_filtered.apply(lambda row: search_term.lower() in str(row['name']).lower() or search_term.lower() in str(row['city']).lower() or search_term.lower() in str(row['venue_name']).lower(), axis=1)]

df_display = df_filtered.drop_duplicates(subset=['id']).copy()

df_display.loc[:, 'demand_prediction'] = pd.to_numeric(df_display['demand_prediction'], errors='coerce').fillna(0)

st.markdown('<div class="kpi-container">', unsafe_allow_html=True)
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Events For You", f"{len(df_display)}")
with col2:
    st.metric("🔥 High-Demand", f"{len(df_display[df_display['popularity_prediction'] == 1.0])}")
with col3:
    top_segment = df_display['segment'].mode()[0] if not df_display.empty else "N/A"
    st.metric("Your Top Segment", top_segment)
st.markdown('</div>', unsafe_allow_html=True)

if df_display.empty:
    st.info("No events match your current filters. Try adjusting your search!")
else:
    tab1, tab2 = st.tabs(["🔥 High-Demand Events", "⭐ Other Recommendations"])

    def render_event_card(row_data, card_type_prefix):
        is_favorited = row_data['id'] in user_favorites
        demand_score = row_data['demand_prediction']
        is_priority = row_data['popularity_prediction'] == 1.0
        
        st.markdown('<div class="event-card">', unsafe_allow_html=True)
        c1, c2 = st.columns([3, 1])
        with c1:
            st.markdown(f"<h3>{row_data['name']}</h3>", unsafe_allow_html=True)
            st.markdown(f"🗓️ **Date:** {row_data['event_date']} | 📍 **Location:** {row_data['city']}, {row_data['state']}")
            price = row_data.get('avg_price')
            price_text = f"${price:.2f}" if pd.notna(price) and price > 0 else "Not Available"
            st.markdown(f"""
                <div class="details-bar">
                    🎭 {row_data['segment']} | 🎵 {row_data['genre']} | 💵 {price_text}
                </div>
            """, unsafe_allow_html=True)

        with c2:
            st.metric(label="Demand Score", value=f"{demand_score:.0f}/100")
            st.markdown(get_demand_tag(demand_score), unsafe_allow_html=True)
        
        st.progress(int(demand_score))
        
        b1, b2, b3 = st.columns([1.5, 2.5, 4])
        with b1:
            button_text = "Unfavorite 💔" if is_favorited else "Favorite ❤️"
            unique_key = f"{card_type_prefix}_fav_{row_data['id']}"
            if st.button(button_text, key=unique_key, use_container_width=True):
                toggle_favorite(row_data['id'], row_data['name'], is_high_demand=is_priority)
                st.rerun()
        with b2:
            if isinstance(row_data.get('url'), str) and row_data['url'].startswith('http'):
                button_type = "primary" if is_priority else "secondary"
                st.link_button("Book Now 🎟️", row_data['url'], use_container_width=True, type=button_type)
        
        st.markdown('</div>', unsafe_allow_html=True)

    with tab1:
        df_priority = df_display[df_display['popularity_prediction'] == 1.0].sort_values("demand_prediction", ascending=False)
        if df_priority.empty:
            st.info("No high-demand events match your current filters.")
        else:
            st.subheader("Top Priority Events")
            st.write("These popular events are predicted to sell out quickly. Act fast!")
            for _, row in df_priority.iterrows():
                render_event_card(row, card_type_prefix="priority")

    with tab2:
        df_recommended = df_display[df_display['popularity_prediction'] != 1.0].sort_values("demand_prediction", ascending=False)
        
        if df_recommended.empty:
            st.info("No other recommended events match your current filters.")
        else:
            st.subheader("Other Personalized Recommendations")
            st.write("A list of other events tailored to your preferences.")
            for _, row in df_recommended.iterrows():
                render_event_card(row, card_type_prefix="rec")