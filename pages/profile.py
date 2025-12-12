import streamlit as st
from pymongo import MongoClient
import os
from PIL import Image
import pandas as pd

# --- Page Configuration ---
st.set_page_config(page_title="User Profile", page_icon="👤", layout="wide")

# --- Custom CSS for Enhanced UI ---
st.markdown("""
<style>
    /* --- Dark Theme Base --- */
    body {
        color: #e0e0e0; /* Default text color for the dark theme */
    }

    /* Set the main background for the app */
    [data-testid="stAppViewContainer"] > .main {
        background-color: #0E1117; /* Dark background color */
    }

    /* Main container styling */
    .main > div {
        padding-top: 2rem;
    }
    
    /* Profile header banner */
    .profile-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Stats cards */
    .stat-card {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        padding: 1.5rem;
        border-radius: 12px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .stat-number {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0;
    }
    
    .stat-label {
        font-size: 0.9rem;
        opacity: 0.9;
        margin-top: 0.5rem;
    }
    
    /* Section headers */
    .section-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 1rem 1.5rem;
        border-radius: 10px;
        color: white;
        margin: 2rem 0 1rem 0;
        font-size: 1.3rem;
        font-weight: 600;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    /* Event cards enhancement */
    div[data-testid="stContainer"][border="true"] {
        background-color: #1c1f2b;
        transition: transform 0.2s, box-shadow 0.2s;
        border: 1px solid #3c4257;
    }
    
    div[data-testid="stContainer"][border="true"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2);
    }
    
    /* Profile picture frame */
    .profile-pic-frame {
        border: 4px solid #667eea;
        border-radius: 50%;
        padding: 5px;
        background: #1c1f2b; /* Dark background for the frame */
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.3);
    }
    
    /* Form styling */
    .stForm {
        background: #1c1f2b; /* Dark background for form */
        padding: 1.5rem;
        border-radius: 12px;
        border: 1px solid #3c4257;
    }
    
    /* Button enhancement */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        padding: 0.75rem 2rem;
        font-weight: 600;
        border-radius: 8px;
        transition: all 0.3s;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }
    
    /* Empty state styling */
    .empty-state {
        text-align: center;
        padding: 3rem 2rem;
        background: rgba(44, 51, 73, 0.5); /* Translucent dark card color */
        border-radius: 15px;
        border: 1px solid #414a66;
        color: #ffffff;
    }
    
    .empty-state-icon {
        font-size: 4rem;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# --- Authentication Check ---
if not st.session_state.get("authentication_status"):
    st.error("You must be logged in to view this page. Please go to the main page to log in.")
    st.stop()

# --- MongoDB Connection ---
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://127.0.0.1:27017/")

client = get_mongo_client()
db = client["event_pulse_db"]
users_collection = db["users"]
events_collection = db["events"]

# --- Data Loading (Cached for Performance) ---
@st.cache_data(ttl=600)
def load_distinct_options():
    """Loads all unique preference options from the events collection."""
    try:
        segments = sorted(events_collection.distinct("segment"))
        cities = sorted(events_collection.distinct("city", {"city": {"$ne": ""}}))
        states = sorted(events_collection.distinct("state", {"state": {"$ne": ""}}))
        genres = sorted(events_collection.distinct("genre", {"genre": {"$ne": ""}}))
        return segments, cities, states, genres
    except Exception as e:
        st.error(f"Could not connect to database to fetch options. Error: {e}")
        return ["Music", "Sports", "Arts & Theatre"], ["New York"], ["NY"], ["Rock", "Pop"]

# --- Load Data ---
available_segments, available_cities, available_states, available_genres = load_distinct_options()
current_user = st.session_state["username"]
user_data = users_collection.find_one({"username": current_user}) or {}

# --- Profile Header Banner ---
st.markdown(f"""
<div class="profile-header">
    <h1 style="margin:0; font-size: 2.5rem;">👤 {st.session_state['name']}'s Profile</h1>
    <p style="margin-top: 0.5rem; opacity: 0.9; font-size: 1.1rem;">Manage your preferences and view your favorited events</p>
</div>
""", unsafe_allow_html=True)

# --- Stats Overview ---
favorited_ids = user_data.get("favorite_event_ids", [])
saved_prefs = len(user_data.get("favorite_segments", [])) + len(user_data.get("favorite_genres", []))
fav_cities = len(user_data.get("favorite_cities", []))

stat_col1, stat_col2, stat_col3 = st.columns(3)

with stat_col1:
    st.markdown(f"""
    <div class="stat-card">
        <p class="stat-number">{len(favorited_ids)}</p>
        <p class="stat-label">❤️ Favorited Events</p>
    </div>
    """, unsafe_allow_html=True)

with stat_col2:
    st.markdown(f"""
    <div class="stat-card" style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);">
        <p class="stat-number">{saved_prefs}</p>
        <p class="stat-label">🎯 Saved Preferences</p>
    </div>
    """, unsafe_allow_html=True)

with stat_col3:
    st.markdown(f"""
    <div class="stat-card" style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);">
        <p class="stat-number">{fav_cities}</p>
        <p class="stat-label">📍 Favorite Cities</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# --- Main Content Layout ---
col1, col2 = st.columns([1, 2.5])

# --- COLUMN 1: Profile Picture ---
with col1:
    st.markdown('<div class="section-header">📸 Profile Picture</div>', unsafe_allow_html=True)
    
    img_dir = "user_images"
    os.makedirs(img_dir, exist_ok=True)
    profile_pic_path = os.path.join(img_dir, f"{current_user}.png")

    pic_container = st.container()
    with pic_container:
        if os.path.exists(profile_pic_path):
            col_pic1, col_pic2, col_pic3 = st.columns([0.5, 2, 0.5])
            with col_pic2:
                st.markdown('<div class="profile-pic-frame">', unsafe_allow_html=True)
                st.image(profile_pic_path, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="empty-state">
                <div class="empty-state-icon">🖼️</div>
                <h3>No Profile Picture Yet</h3>
                <p>Upload your first picture below!</p>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    uploaded_file = st.file_uploader("Choose a new picture", type=["png", "jpg", "jpeg"], label_visibility="collapsed")
    
    if uploaded_file is not None:
        try:
            img = Image.open(uploaded_file)
            img.save(profile_pic_path)
            st.success("✅ Picture updated successfully!")
            st.rerun()
        except Exception as e:
            st.error(f"❌ Could not save image. Error: {e}")

    # --- Quick Actions ---
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-header">⚡ Quick Actions</div>', unsafe_allow_html=True)
    
    if st.button("🏠 Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")


# --- COLUMN 2: Preferences Form ---
with col2:
    st.markdown('<div class="section-header">⚙️ Your Preferences</div>', unsafe_allow_html=True)
    st.markdown("**Customize your experience** by selecting your interests for personalized recommendations.")

    with st.form("preferences_form"):
        st.subheader("🎵 Entertainment Interests")
        
        selected_segments = st.multiselect(
            "Favorite Segments",
            options=available_segments,
            default=user_data.get("favorite_segments", []),
            help="Select the types of events you're interested in"
        )
        
        if any(s in selected_segments for s in ["Music", "Film", "Arts & Theatre"]):
            selected_genres = st.multiselect(
                "Favorite Genres",
                options=available_genres,
                default=user_data.get("favorite_genres", []),
                help="Choose specific genres within your selected segments"
            )
        else:
            selected_genres = []
            if not selected_segments:
                st.info("💡 Select segments above to enable genre selection")

        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("📍 Location Preferences")
        
        loc_col1, loc_col2 = st.columns(2)
        with loc_col1:
            selected_cities = st.multiselect(
                "Favorite Cities",
                options=available_cities,
                default=user_data.get("favorite_cities", []),
                help="Events in these cities will be prioritized"
            )
        
        with loc_col2:
            selected_states = st.multiselect(
                "Favorite States",
                options=available_states,
                default=user_data.get("favorite_states", []),
                help="Select states you're interested in"
            )
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("💰 Budget Settings")
        
        saved_price = user_data.get("max_price", 500)
        selected_max_price = st.slider(
            "Maximum average ticket price",
            min_value=0,
            max_value=1000,
            value=saved_price,
            step=10,
            format="$%d",
            help="Only show events with average prices below this amount"
        )

        st.markdown("<br>", unsafe_allow_html=True)
        
        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 1])
        with col_btn2:
            submitted = st.form_submit_button("💾 Save All Preferences", use_container_width=True)
        
        if submitted:
            try:
                update_data = {
                    "favorite_segments": selected_segments,
                    "favorite_genres": selected_genres,
                    "favorite_cities": selected_cities,
                    "favorite_states": selected_states,
                    "max_price": selected_max_price
                }
                
                users_collection.update_one(
                    {"username": current_user},
                    {"$set": update_data},
                    upsert=True
                )
                st.success("✅ Your preferences have been saved successfully!")
                st.balloons()
            except Exception as e:
                st.error(f"❌ An error occurred while saving: {e}")

# ==============================================================================
# === FAVORITED EVENTS SECTION =================================================
# ==============================================================================

st.markdown("<br>", unsafe_allow_html=True)
st.markdown('<div class="section-header">❤️ Your Favorited Events</div>', unsafe_allow_html=True)

if not favorited_ids:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-state-icon">💔</div>
        <h2>No Favorites Yet</h2>
        <p style="font-size: 1.1rem; margin: 1rem 0;">Start building your collection by clicking the ❤️ button on events you love!</p>
        <p style="opacity: 0.8;">Visit the Dashboard or Browse Events to discover exciting happenings near you.</p>
    </div>
    """, unsafe_allow_html=True)
    
    col_empty1, col_empty2, col_empty3 = st.columns([1, 1, 1])
    with col_empty2:
        if st.button("🚀 Explore Events Now", use_container_width=True, type="primary"):
            st.switch_page("pages/dashboard.py")
else:
    favorited_events = list(events_collection.find({"id": {"$in": favorited_ids}}))
    
    if not favorited_events:
        st.warning("⚠️ Could not find details for your favorited events. They may no longer be available.")
    else:
        df_favs = pd.DataFrame(favorited_events)
        df_favs['event_date_dt'] = pd.to_datetime(df_favs['event_date'])
        df_favs = df_favs.sort_values('event_date_dt')

        st.markdown(f"**You have {len(df_favs)} saved event{'s' if len(df_favs) != 1 else ''}** 🎉")
        st.markdown("<br>", unsafe_allow_html=True)
        
        for idx, event in df_favs.iterrows():
            with st.container(border=True):
                event_col1, event_col2 = st.columns([3, 1])
                
                with event_col1:
                    st.markdown(f"### {event.get('name', 'N/A')}")
                    
                    date_str = event['event_date_dt'].strftime('%A, %B %d, %Y')
                    segment = event.get('segment', 'N/A')
                    venue = event.get('venue_name', 'N/A')
                    
                    st.markdown(f"**{segment}** • 📍 {venue}")
                    st.markdown(f"📅 {date_str}")
                    
                    genre = event.get('genre', '')
                    if genre:
                        st.caption(f"🎭 Genre: {genre}")
                
                with event_col2:
                    price = event.get('avg_price', 0)
                    price_text = f"${price:.2f}" if price > 0 else "Free"
                    st.metric("Avg. Price", price_text)
                    
                    url = event.get('url')
                    if isinstance(url, str) and url.startswith('http'):
                        st.link_button("🎟️ Get Tickets", url, use_container_width=True)
                    
                    # Remove from favorites button
                    if st.button("💔 Remove", key=f"remove_{event.get('id')}", use_container_width=True):
                        try:
                            users_collection.update_one(
                                {"username": current_user},
                                {"$pull": {"favorite_event_ids": event.get('id')}}
                            )
                            st.success("Removed from favorites!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error removing favorite: {e}")

# --- Footer ---
st.markdown("<br><br>", unsafe_allow_html=True)
st.divider()
st.markdown("""
<div style="text-align: center; opacity: 0.7; padding: 2rem 0;">
    <p>🎭 EventPulse • Your Personal Event Companion</p>
</div>
""", unsafe_allow_html=True)