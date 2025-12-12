import streamlit as st
import yaml
from yaml.loader import SafeLoader

# 1. PAGE CONFIGURATION
st.set_page_config(
    page_title="Event Horizon",
    page_icon="🎟️",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# --- CUSTOM STYLING ---
st.markdown("""
<style>
/* General Body Styles */
body {
    background-color: #f0f2f6;
    color: #333;
}

/* Login Page Specific Styles */
.login-container {
    background-image: linear-gradient(to top, #a18cd1 0%, #fbc2eb 100%);
    padding: 3rem;
    border-radius: 10px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
    color: white;
}

/* Feature Cards */
.feature-card {
    background-color: rgba(255, 255, 255, 0.9);
    padding: 1.5rem;
    border-radius: 10px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    margin-bottom: 1rem;
    color: #333;
    display: flex;
    align-items: center;
}

.feature-icon {
    font-size: 2rem;
    margin-right: 1rem;
    color: #8a2be2; /* A shade of purple */
}

/* Login Form Styling */
.login-form {
    background-color: white;
    padding: 2rem;
    border-radius: 10px;
    box-shadow: 0 4px 8px rgba(0,0,0,0.1);
}

/* Button Styling */
div.stButton > button:first-child {
    background-color: #8a2be2;
    color: white;
    border-radius: 5px;
    border: none;
    padding: 0.75rem 1.5rem;
    font-size: 1rem;
    font-weight: bold;
}

div.stButton > button:hover {
    background-color: #7a1fb8;
}

/* Welcome Message */
.welcome-message {
    background-color: #e0d4ff;
    padding: 1.5rem;
    border-radius: 10px;
    border: 2px solid #8a2be2;
}
</style>
""", unsafe_allow_html=True)

# Font Awesome for icons
st.markdown('<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/5.15.4/css/all.min.css">', unsafe_allow_html=True)


# 2. INITIALIZE SESSION STATE
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None
if 'name' not in st.session_state:
    st.session_state['name'] = None
if 'username' not in st.session_state:
    st.session_state['username'] = None

# 3. LOAD USER CREDENTIALS
try:
    with open('config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)
        credentials = config['credentials']['usernames']
except FileNotFoundError:
    st.error("config.yaml not found. Please create it to manage user credentials.")
    st.stop()
except Exception as e:
    st.error(f"Error loading config.yaml: {e}")
    st.stop()

# 4. DISPLAY LOGIN/WELCOME UI
st.title("Welcome to Event Horizon 🎟️")
st.markdown("Your compass for trending events.")
st.divider()

# --- LOGIN SCREEN ---
if not st.session_state['authentication_status']:
    col1, col2 = st.columns([1.2, 1], gap="large")

    with col1:
        st.header("Unlock Event Insights")
        
        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-search"></i></div>
            <div>
                <strong>Discover</strong>
                <p>Find events tailored to your preferences.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-chart-line"></i></div>
            <div>
                <strong>Predict</strong>
                <p>Analyze sell-out risk and event demand.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown("""
        <div class="feature-card">
            <div class="feature-icon"><i class="fas fa-bell"></i></div>
            <div>
                <strong>Get Notified</strong>
                <p>Receive alerts about new, exciting opportunities.</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        with st.container():
             st.markdown('<div class="login-container">', unsafe_allow_html=True)
             with st.container():
                st.subheader("🔐 Secure Login")
                login_username = st.text_input("Username", key="login_user")
                login_password = st.text_input("Password", type="password", key="login_pass")

                if st.button("Login", use_container_width=True, type="primary"):
                    if login_username in credentials and credentials[login_username]['password'] == login_password:
                        st.session_state['authentication_status'] = True
                        st.session_state['name'] = credentials[login_username]['name']
                        st.session_state['username'] = login_username
                        st.balloons()
                        st.rerun()
                    else:
                        st.error("😕 Username or password is incorrect.")
             st.markdown('</div>', unsafe_allow_html=True)
else:
    st.markdown(f"""
    <div class="welcome-message">
        <h3>Welcome back, {st.session_state['name']}! 🎉</h3>
        <p>👈 Select a page from the sidebar to dive into your personalized dashboard or manage your profile.</p>
    </div>
    """, unsafe_allow_html=True)