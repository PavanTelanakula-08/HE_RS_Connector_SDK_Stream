import streamlit as st
import json
import time
from connector import redshift_to_snowflake_for_ui
from schema import table_list

st.set_page_config(layout="wide")

# Sidebar Navigation
st.sidebar.title("Navigation")
navigation = st.sidebar.radio("Go to", ["Overview", "Connections", "Transformations", "Settings"])

# Sidebar Configuration
st.sidebar.header("Configuration")
config_path = st.sidebar.text_input("Path to configuration.json", "configuration.json")
batch_size = st.sidebar.number_input("Batch Size per Cycle", min_value=10, max_value=10000, step=100, value=1000)
auto_refresh = st.sidebar.checkbox("Auto-Refresh Every 60 Sec", value=False)

# Initialize session state
if "is_syncing" not in st.session_state:
    st.session_state.is_syncing = False
if "cancel_requested" not in st.session_state:
    st.session_state.cancel_requested = False

# Main Content
st.title("📡 Redshift to Snowflake Sync Dashboard")

if navigation == "Overview":
    st.subheader("Overview")
    st.write("Display overall sync status, recent activity, and key metrics here.")

elif navigation == "Connections":
    st.subheader("Connections")
    available_tables = [t["table"] for t in table_list]
    selected_tables = st.multiselect("Select Tables to Sync", available_tables, default=available_tables)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("🚀 Start Sync"):
            st.session_state.is_syncing = True
            st.session_state.cancel_requested = False

    with col2:
        if st.button("❌ Cancel Sync"):
            st.session_state.cancel_requested = True

    st.markdown("---")

    log_placeholder = st.empty()
    progress_bar = st.progress(0)
    status_text = st.empty()

    if st.session_state.is_syncing and not st.session_state.cancel_requested:
        try:
            with open(config_path, "r") as f:
                config = json.load(f)

            config["only_tables"] = selected_tables

            st.success("Configuration loaded successfully!")
            log_placeholder.code("Starting sync...", language="text")

            synced_tables = 0
            total_records = 0

            def should_continue():
                return not st.session_state.cancel_requested and (auto_refresh or synced_tables < len(selected_tables))

            while should_continue():
                synced_tables = 0
                total_records = 0
                for result in redshift_to_snowflake_for_ui(config, batch_size=batch_size):
                    if st.session_state.cancel_requested:
                        log_placeholder.code("❌ Sync cancelled by user.", language="text")
                        break

                    if result.get("type") == "log":
                        log_placeholder.code(result["message"], language="text")
                    elif result.get("type") == "progress":
                        synced_tables += 1
                        total_records += result.get("total_records", 0)
                        progress = min(synced_tables / len(selected_tables), 1.0)
                        progress_bar.progress(progress)
                        status_text.text(f"🔄 Synced batch #{result['batch']} | Total rows: {total_records}")

                if auto_refresh and not st.session_state.cancel_requested:
                    time.sleep(60)
                else:
                    break

            if not st.session_state.cancel_requested:
                st.success(f"✅ Sync complete. Total records transferred: {total_records}")
            else:
                st.warning("⚠️ Sync cancelled.")

        except Exception as e:
            st.error(f"❌ Error during sync: {str(e)}")
        finally:
            st.session_state.is_syncing = False
            st.session_state.cancel_requested = False

elif navigation == "Transformations":
    st.subheader("Transformations")
    st.write("Display transformation statuses, logs, and controls here.")

elif navigation == "Settings":
    st.subheader("Settings")
    st.write("Display and edit configuration settings here.")
