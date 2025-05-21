import streamlit as st
import json
import time
import pandas as pd
from datetime import datetime
from connector import redshift_to_snowflake_for_ui
from schema import table_list

st.set_page_config(layout="wide", page_title="Redshift to Snowflake Sync")

# --- Session State Init ---
if "is_syncing" not in st.session_state:
    st.session_state.is_syncing = False
if "cancel_requested" not in st.session_state:
    st.session_state.cancel_requested = False
if "metrics" not in st.session_state:
    st.session_state.metrics = []
if "row_counts" not in st.session_state:
    st.session_state.row_counts = {}

# --- Sidebar ---
st.sidebar.header("🔧 Configuration")
config_path = st.sidebar.text_input("Path to configuration.json", "configuration.json")
batch_size = st.sidebar.number_input("Batch Size per Cycle", min_value=10, max_value=10000, step=100, value=1000)
auto_refresh = st.sidebar.checkbox("Auto-Refresh Every 60 Sec", value=False)

# --- Table Selector ---
available_tables = [t["table"] for t in table_list]
selected_tables = st.multiselect("Select Tables to Sync", available_tables, default=available_tables)

# --- Control Buttons ---
col1, col2 = st.columns(2)
with col1:
    if st.button("🚀 Start Sync"):
        st.session_state.is_syncing = True
        st.session_state.cancel_requested = False
        st.session_state.metrics = []
        st.session_state.row_counts = {}
with col2:
    if st.button("❌ Cancel Sync"):
        st.session_state.cancel_requested = True

st.markdown("## 📊 Sync Dashboard")

# --- Top KPIs ---
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Records Synced", f"{sum(st.session_state.row_counts.values()):,}")
with col2:
    st.metric("Tables Synced", f"{len(st.session_state.row_counts)} / {len(selected_tables)}")
with col3:
    if st.session_state.metrics:
        latest = st.session_state.metrics[-1]
        st.metric("Last Sync Time", latest["timestamp"].split("T")[-1].split("Z")[0])
    else:
        st.metric("Last Sync Time", "--")

st.markdown("---")

# --- Visualization Placeholders ---
chart_col1, chart_col2 = st.columns([2, 1])
with chart_col1:
    if st.session_state.metrics:
        df = pd.DataFrame(st.session_state.metrics)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        st.line_chart(df.set_index("timestamp")["total_records"], use_container_width=True)
    else:
        st.info("Throughput graph will appear here after sync starts.")

with chart_col2:
    if st.session_state.row_counts:
        df_bar = pd.DataFrame.from_dict(st.session_state.row_counts, orient="index", columns=["Rows"])
        df_bar = df_bar.sort_values("Rows", ascending=False)
        st.bar_chart(df_bar)
    else:
        st.info("Row count per table will show here.")

# --- Sync Execution ---
log_placeholder = st.empty()
progress_bar = st.progress(0)
status_text = st.empty()

def cancel_flag():
    return st.session_state.cancel_requested

if st.session_state.is_syncing and not st.session_state.cancel_requested:
    try:
        with open(config_path, "r") as f:
            config = json.load(f)

        config["only_tables"] = selected_tables
        st.success("✅ Configuration loaded successfully")
        log_placeholder.code("Starting sync...", language="text")

        synced_tables = 0
        total_records = 0

        def should_continue():
            return not st.session_state.cancel_requested and (auto_refresh or synced_tables < len(selected_tables))

        while should_continue():
            synced_tables = 0
            total_records = 0

            for result in redshift_to_snowflake_for_ui(
                config,
                batch_size=batch_size,
                only_tables=selected_tables,
                cancel_flag=cancel_flag  # ← LIVE CANCEL FLAG
            ):
                if st.session_state.cancel_requested:
                    log_placeholder.code("❌ Sync cancelled by user.", language="text")
                    break

                if result.get("type") == "log":
                    log_placeholder.code(result["message"], language="text")

                elif "batch" in result:
                    synced_tables += 1
                    total_records = result["total_records"]
                    table_counts = result.get("checkpoint", {})
                    for key, value in table_counts.items():
                        table_name = key.replace("_cursor", "")
                        st.session_state.row_counts[table_name] = total_records
                    st.session_state.metrics.append({
                        "timestamp": result["timestamp"],
                        "total_records": total_records,
                        "batch": result["batch"]
                    })

                    progress = min(synced_tables / len(selected_tables), 1.0)
                    progress_bar.progress(progress)
                    status_text.text(f"🔄 Batch #{result['batch']} | Rows this batch: {total_records}")

            if auto_refresh and not st.session_state.cancel_requested:
                time.sleep(60)
            else:
                break

        if not st.session_state.cancel_requested:
            st.success(f"✅ Sync complete. Total records transferred: {total_records}")
        else:
            st.warning("⚠️ Sync cancelled.")
            progress_bar.progress(0)
            status_text.text("❌ Sync cancelled by user.")

    except Exception as e:
        st.error(f"❌ Error during sync: {str(e)}")
    finally:
        st.session_state.is_syncing = False
        st.session_state.cancel_requested = False
