"""
N-BeMod — Streamlit UI
Pantallas: Upload/DQ | Calibrate | Run + Results + Download
"""
import os
import time
import requests
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

API = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(
    page_title="N-BeMod",
    page_icon="⬛",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Premium Dark Theme (Palantir-inspired) ───────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
    background-color: #080808;
    color: #E8E8E8;
}
.stApp { background-color: #080808; }
.main .block-container { padding: 2rem 2.5rem; max-width: 1400px; }

/* Sidebar */
[data-testid="stSidebar"] {
    background-color: #0D0D0D;
    border-right: 1px solid #1A1A1A;
}
[data-testid="stSidebar"] .stSelectbox label,
[data-testid="stSidebar"] p { color: #A0A0A0; font-size: 0.8rem; }

/* Headers */
h1 { font-family: 'IBM Plex Sans', sans-serif; font-weight: 700; font-size: 1.6rem;
     color: #FFFFFF; letter-spacing: -0.5px; }
h2 { font-family: 'IBM Plex Sans', sans-serif; font-weight: 600; font-size: 1.1rem;
     color: #C8C8C8; border-bottom: 1px solid #1E1E1E; padding-bottom: 0.5rem; margin-top: 1.5rem; }
h3 { font-size: 0.9rem; color: #A0A0A0; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }

/* Buttons */
.stButton > button {
    background-color: #00C2FF;
    color: #000000;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 600;
    font-size: 0.8rem;
    letter-spacing: 0.5px;
    border: none;
    border-radius: 2px;
    padding: 0.6rem 1.5rem;
    transition: all 0.15s ease;
}
.stButton > button:hover { background-color: #33CFFF; transform: translateY(-1px); }

/* Metrics */
[data-testid="metric-container"] {
    background: #0F0F0F;
    border: 1px solid #1E1E1E;
    border-radius: 2px;
    padding: 1rem;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] { color: #00C2FF; font-size: 1.4rem; font-weight: 700; }
[data-testid="metric-container"] [data-testid="stMetricLabel"] { color: #606060; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 1px; }

/* Inputs */
.stTextInput input, .stSelectbox select, .stNumberInput input {
    background-color: #111111 !important;
    border: 1px solid #2A2A2A !important;
    color: #E8E8E8 !important;
    border-radius: 2px !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.85rem !important;
}

/* Tables */
.stDataFrame { border: 1px solid #1E1E1E; border-radius: 2px; }
thead th { background-color: #111111 !important; color: #00C2FF !important;
           font-family: 'IBM Plex Mono', monospace !important; font-size: 0.75rem !important; }

/* Status badges */
.badge-ok    { background: #0A2A1A; color: #00FF88; padding: 2px 10px; border-radius: 2px;
               font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; font-weight: 600; }
.badge-warn  { background: #2A2000; color: #FFB800; padding: 2px 10px; border-radius: 2px;
               font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; font-weight: 600; }
.badge-ko    { background: #2A0A0A; color: #FF4444; padding: 2px 10px; border-radius: 2px;
               font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; font-weight: 600; }
.badge-pend  { background: #1A1A2A; color: #8888FF; padding: 2px 10px; border-radius: 2px;
               font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; font-weight: 600; }

/* Panel */
.panel {
    background: #0D0D0D;
    border: 1px solid #1A1A1A;
    border-radius: 2px;
    padding: 1.5rem;
    margin: 0.5rem 0;
}
.mono { font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; color: #606060; }

/* Dividers */
hr { border-color: #1A1A1A; }

/* Logo area */
.logo-text {
    font-family: 'IBM Plex Sans', sans-serif;
    font-weight: 700;
    font-size: 1.1rem;
    color: #FFFFFF;
    letter-spacing: 2px;
}
.logo-sub {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.65rem;
    color: #404040;
    letter-spacing: 1px;
    text-transform: uppercase;
}
</style>
""", unsafe_allow_html=True)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def badge(status: str) -> str:
    classes = {"OK": "badge-ok", "WARN": "badge-warn", "KO": "badge-ko",
               "SUCCEEDED": "badge-ok", "FAILED": "badge-ko",
               "QUEUED": "badge-pend", "RUNNING": "badge-pend", "PENDING": "badge-pend"}
    cls = classes.get(status, "badge-pend")
    return f'<span class="{cls}">{status}</span>'


def api_get(path: str):
    try:
        r = requests.get(f"{API}{path}", timeout=10)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
    return None


def dark_plotly():
    return dict(
        paper_bgcolor="#080808",
        plot_bgcolor="#0D0D0D",
        font=dict(family="IBM Plex Mono", color="#A0A0A0", size=10),
        xaxis=dict(gridcolor="#1A1A1A", linecolor="#2A2A2A"),
        yaxis=dict(gridcolor="#1A1A1A", linecolor="#2A2A2A"),
        margin=dict(l=40, r=20, t=40, b=40),
    )


# ─── Sidebar ──────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown('<div class="logo-text">N-BeMod</div>', unsafe_allow_html=True)
    st.markdown('<div class="logo-sub">Behavioural Model Platform</div>', unsafe_allow_html=True)
    st.markdown("---")

    page = st.selectbox(
        "MODULE",
        ["01 · Upload & DQ", "02 · Calibrate", "03 · Run & Results", "04 · Backtesting"],
        label_visibility="visible"
    )
    st.markdown("---")
    health = api_get("/health")
    api_status = "● ONLINE" if health else "● OFFLINE"
    api_color = "#00FF88" if health else "#FF4444"
    st.markdown(f'<div style="font-family:IBM Plex Mono;font-size:0.7rem;color:{api_color}">{api_status}</div>', unsafe_allow_html=True)
    st.markdown('<div class="mono">v0.1.0 · MVP</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: Upload & Data Quality
# ══════════════════════════════════════════════════════════════════════════════

if page == "01 · Upload & Data Quality":
    st.markdown("## Upload & Data Quality")
    st.markdown('<div class="mono">loans dataset → normalization → DQ checks → versioning</div>', unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([1, 1.5], gap="large")

    with col1:
        st.markdown("### ENTITY")
        entities = api_get("/entities") or []
        if entities:
            entity_opts = {e["name"]: e["id"] for e in entities}
            selected_entity_name = st.selectbox("Select Entity", list(entity_opts.keys()))
            selected_entity_id = entity_opts[selected_entity_name]
        else:
            st.warning("No entities found. Create one below.")
            selected_entity_id = None
            selected_entity_name = None

        with st.expander("+ New Entity"):
            new_name = st.text_input("Entity Name")
            new_desc = st.text_input("Description (optional)")
            if st.button("Create Entity"):
                try:
                    r = requests.post(f"{API}/entities", json={"name": new_name, "description": new_desc})
                    if r.status_code == 201:
                        st.success(f"Entity '{new_name}' created")
                        st.rerun()
                    else:
                        st.error(r.json().get("detail", "Error"))
                except Exception as e:
                    st.error(str(e))

        st.markdown("### UPLOAD")
        uploaded_file = st.file_uploader("Loans dataset (CSV or Excel)", type=["csv", "xlsx", "xls"])
        as_of_date = st.text_input("As-of Date (YYYY-MM-DD)", value="2024-12-31")

        if st.button("Upload & Process") and uploaded_file and selected_entity_id:
            with st.spinner("Uploading..."):
                try:
                    r = requests.post(
                        f"{API}/datasets/loans/upload",
                        files={"file": (uploaded_file.name, uploaded_file.getvalue())},
                        data={"entity_id": selected_entity_id, "as_of_date": as_of_date},
                    )
                    if r.status_code == 202:
                        st.success(f"✓ Queued | dataset_version_id: `{r.json()['dataset_version_id']}`")
                    else:
                        st.error(r.text)
                except Exception as e:
                    st.error(str(e))

    with col2:
        st.markdown("### DATASET VERSIONS")
        filter_entity = selected_entity_id if selected_entity_id else None
        dvs = api_get(f"/datasets?entity_id={filter_entity}" if filter_entity else "/datasets") or []

        if dvs:
            for dv in dvs[:10]:
                with st.container():
                    c1, c2, c3 = st.columns([2, 1, 1])
                    c1.markdown(f'<div class="mono">{dv["source_name"]}</div>', unsafe_allow_html=True)
                    c2.markdown(f'<div class="mono">{dv["as_of_date"]}</div>', unsafe_allow_html=True)
                    c3.markdown(badge(dv["status"]), unsafe_allow_html=True)

                    if dv.get("dq_summary") and dv["status"] in ("OK", "WARN", "KO"):
                        dqs = dv["dq_summary"]
                        m1, m2, m3, m4 = st.columns(4)
                        m1.metric("Rows", dv.get("row_count", "—"))
                        m2.metric("Balance", f"{dv.get('total_balance', 0):,.0f}" if dv.get("total_balance") else "—")
                        m3.metric("DQ Warns", dqs.get("warn_count", 0))
                        m4.metric("DQ KOs", dqs.get("ko_count", 0))
                    st.markdown('<hr style="margin:0.5rem 0;border-color:#1A1A1A">', unsafe_allow_html=True)
        else:
            st.markdown('<div class="panel mono">No datasets found.</div>', unsafe_allow_html=True)

        if st.button("↻ Refresh"):
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: Calibrate
# ══════════════════════════════════════════════════════════════════════════════

elif page == "02 · Calibrate":
    st.markdown("## Model Calibration")
    st.markdown('<div class="mono">prepay_curve · simple_average CPR/SMM by segment</div>', unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([1, 1.5], gap="large")

    with col1:
        st.markdown("### DATASET")
        dvs = api_get("/datasets") or []
        valid_dvs = [d for d in dvs if d["status"] in ("OK", "WARN")]

        if not valid_dvs:
            st.warning("No valid datasets (OK/WARN). Upload and process a dataset first.")
        else:
            dv_opts = {f"{d['source_name']} · {d['as_of_date']} [{d['status']}]": d["id"] for d in valid_dvs}
            sel_dv_label = st.selectbox("Select Dataset Version", list(dv_opts.keys()))
            sel_dv_id = dv_opts[sel_dv_label]

            st.markdown("### MODEL CONFIG")
            curve_method = st.selectbox("Curve Method", ["simple_average"], help="Cohort method available in P1")
            horizon_months = st.slider("Horizon (months)", 12, 240, 60, step=12)
            min_segment_size = st.number_input("Min Segment Size", min_value=1, value=10)
            smoothing = st.toggle("Smoothing (rolling avg)", value=False)

            if st.button("▶  Launch Calibration"):
                with st.spinner("Queuing calibration job..."):
                    try:
                        r = requests.post(f"{API}/models/prepay_curve/calibrate", json={
                            "dataset_version_id": sel_dv_id,
                            "curve_method": curve_method,
                            "horizon_months": horizon_months,
                            "min_segment_size": min_segment_size,
                            "smoothing": smoothing,
                        })
                        if r.status_code == 202:
                            mv_id = r.json()["model_version_id"]
                            st.success(f"✓ Queued | model_version_id: `{mv_id}`")
                        else:
                            st.error(r.text)
                    except Exception as e:
                        st.error(str(e))

    with col2:
        st.markdown("### MODEL VERSIONS")
        mvs = api_get("/models/versions") or []

        if mvs:
            for mv in mvs[:8]:
                status_badge = badge(mv["status"])
                with st.container():
                    c1, c2 = st.columns([3, 1])
                    c1.markdown(f'<div class="mono">mv · {mv["id"][:16]}…</div>', unsafe_allow_html=True)
                    c2.markdown(status_badge, unsafe_allow_html=True)

                    if mv["status"] == "SUCCEEDED" and mv.get("summary_metrics"):
                        m = mv["summary_metrics"].get("global", {})
                        mc1, mc2, mc3 = st.columns(3)
                        mc1.metric("Segments", m.get("total_segments", "—"))
                        mc2.metric("Horizon", f"{m.get('horizon_months', '—')}m")
                        mc3.metric("Contracts", m.get("total_contracts", "—"))

                        # Plot curves if available
                        if st.button(f"View Curves", key=f"curves_{mv['id']}"):
                            st.session_state["selected_mv_id"] = mv["id"]

                    elif mv["status"] == "FAILED":
                        st.markdown(f'<div class="mono" style="color:#FF4444">{mv.get("error_message", "Unknown error")}</div>', unsafe_allow_html=True)

                    st.markdown('<hr style="margin:0.4rem 0;border-color:#1A1A1A">', unsafe_allow_html=True)
        else:
            st.markdown('<div class="panel mono">No model versions yet.</div>', unsafe_allow_html=True)

        if st.button("↻ Refresh"):
            st.rerun()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: Run & Results
# ══════════════════════════════════════════════════════════════════════════════

elif page == "03 · Run & Results":
    st.markdown("## Scenario Run & Results")
    st.markdown('<div class="mono">execute · cashflows · export</div>', unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([1, 2], gap="large")

    with col1:
        st.markdown("### MODEL VERSION")
        mvs = api_get("/models/versions") or []
        succeeded_mvs = [m for m in mvs if m["status"] == "SUCCEEDED"]

        if not succeeded_mvs:
            st.warning("No succeeded model versions. Calibrate a model first.")
        else:
            mv_opts = {f"mv · {m['id'][:16]}… | {m['params_json'].get('horizon_months', '?')}m": m["id"] for m in succeeded_mvs}
            sel_mv_label = st.selectbox("Select Model Version", list(mv_opts.keys()))
            sel_mv_id = mv_opts[sel_mv_label]

            st.markdown("### SCENARIO")
            scenario = st.selectbox("Scenario", ["Base", "Shock+100", "Shock-100"])

            if st.button("▶  Execute Run"):
                with st.spinner("Launching run..."):
                    try:
                        r = requests.post(f"{API}/runs", json={"model_version_id": sel_mv_id, "scenario_name": scenario})
                        if r.status_code == 202:
                            run_id = r.json()["run_id"]
                            st.success(f"✓ Run queued | run_id: `{run_id}`")
                        else:
                            st.error(r.text)
                    except Exception as e:
                        st.error(str(e))

    with col2:
        st.markdown("### RESULTS")

        # List recent runs (simplified: get all artifacts from recent runs)
        # In production: paginated run listing endpoint
        run_id_input = st.text_input("Run ID (paste to inspect)", placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")

        if run_id_input:
            run = api_get(f"/runs/{run_id_input}")
            if run:
                rc1, rc2, rc3 = st.columns(3)
                rc1.markdown(badge(run["status"]), unsafe_allow_html=True)
                if run.get("started_at"):
                    rc2.markdown(f'<div class="mono">Started: {run["started_at"][:19]}</div>', unsafe_allow_html=True)
                if run.get("finished_at"):
                    rc3.markdown(f'<div class="mono">Finished: {run["finished_at"][:19]}</div>', unsafe_allow_html=True)

                if run["status"] == "FAILED":
                    st.error(run.get("error_message", "Unknown error"))

                elif run["status"] == "SUCCEEDED":
                    artifacts = api_get(f"/runs/{run_id_input}/artifacts") or []

                    for a in artifacts:
                        if a["artifact_type"] == "excel":
                            dl_url = f"{API}/runs/artifacts/{a['id']}/download"
                            st.markdown(f"""
                            <a href="{dl_url}" target="_blank">
                                <div class="panel" style="cursor:pointer;border-color:#00C2FF22;text-align:center">
                                    <span style="font-family:IBM Plex Mono;font-size:0.85rem;color:#00C2FF">
                                        ⬇ Download Excel Export
                                    </span>
                                </div>
                            </a>
                            """, unsafe_allow_html=True)

                        if a.get("summary_metrics_json"):
                            sm = a["summary_metrics_json"]
                            sm1, sm2, sm3, sm4 = st.columns(4)
                            sm1.metric("Total Prepayment", f"{sm.get('total_prepayment', 0):,.0f}")
                            sm2.metric("Avg CPR", f"{sm.get('avg_cpr', 0):.2%}")
                            sm3.metric("Segments", sm.get("segments", "—"))
                            sm4.metric("Months", sm.get("months", "—"))

                elif run["status"] in ("QUEUED", "RUNNING"):
                    st.info("Run in progress... refresh in a few seconds.")
                    time.sleep(2)
                    st.rerun()

        if st.button("↻ Refresh"):
            st.rerun()
elif page == "04 · Backtesting":
    st.markdown("## Backtesting & Model Validation")
    st.markdown('<div class="mono">rolling validation · MAPE · WAPE · segment performance</div>', unsafe_allow_html=True)
    st.markdown("---")

    col1, col2 = st.columns([1, 1.5], gap="large")

    with col1:
        st.markdown("### MODEL VERSION")
        mvs = api_get("/models/versions") or []
        succeeded_mvs = [m for m in mvs if m["status"] == "SUCCEEDED"]

        if not succeeded_mvs:
            st.warning("No succeeded model versions. Calibrate a model first.")
        else:
            mv_opts = {f"mv · {m['id'][:16]}… | {m['params_json'].get('horizon_months', '?')}m": m["id"] for m in succeeded_mvs}
            sel_mv_label = st.selectbox("Select Model Version", list(mv_opts.keys()))
            sel_mv_id = mv_opts[sel_mv_label]

            st.markdown("### CONFIG")
            noise_std = st.slider("Noise std (simulation)", 0.01, 0.10, 0.02, step=0.01,
                                  help="Desviación estándar del ruido simulado. En producción: datos reales.")

            if st.button("▶ Run Backtesting"):
                with st.spinner("Running backtesting..."):
                    try:
                        r = requests.post(f"{API}/backtesting/run", json={
                            "model_version_id": sel_mv_id,
                            "noise_std": noise_std,
                        })
                        if r.status_code == 200:
                            result = r.json()
                            st.session_state["bt_result"] = result
                            st.success("✓ Backtesting complete")
                        else:
                            st.error(r.text)
                    except Exception as e:
                        st.error(str(e))

    with col2:
        st.markdown("### RESULTS")

        if "bt_result" in st.session_state:
            result = st.session_state["bt_result"]
            gm = result["global_metrics"]
            sm = result["segment_metrics"]

            # Global metrics
            m1, m2, m3 = st.columns(3)
            mape_color = "normal" if gm["mape"] < 10 else "inverse"
            m1.metric("Global MAPE", f"{gm['mape']:.2f}%")
            m2.metric("Global WAPE", f"{gm['wape']:.2f}%")
            m3.metric("Segments", gm["total_segments"])

            # Interpretation
            interp = gm.get("interpretation", "")
            color = "#00E87A" if "EXCELLENT" in interp else "#FFB800" if "GOOD" in interp else "#FF4444"
            st.markdown(f'<div style="font-family:var(--mono);font-size:0.75rem;color:{color};margin:0.5rem 0">{interp}</div>', unsafe_allow_html=True)

            st.markdown("---")

            # Segment metrics table
            st.markdown("### SEGMENT BREAKDOWN")
            seg_rows = []
            for seg, metrics in sm.items():
                seg_rows.append({
                    "Segment": seg,
                    "MAPE (%)": round(metrics["mape"], 2),
                    "WAPE (%)": round(metrics["wape"], 2),
                    "CPR Predicted": f"{metrics['avg_cpr_predicted']:.2%}",
                    "CPR Observed": f"{metrics['avg_cpr_observed']:.2%}",
                    "Balance (€)": f"{metrics['balance']:,.0f}",
                })
            seg_df = pd.DataFrame(seg_rows)
            st.dataframe(seg_df, use_container_width=True, hide_index=True)

        else:
            st.markdown('<div class="panel mono">Run backtesting to see results.</div>', unsafe_allow_html=True)
