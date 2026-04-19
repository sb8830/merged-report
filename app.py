"""
app.py  —  Invesmate Analytics Dashboard  (Streamlit)
Single merged app — Online + Offline + Integrated dashboards
Live MS365 data OR manual file upload.
"""
import streamlit as st
import streamlit.components.v1 as components
import json, os, hashlib, secrets
from pathlib import Path
from PIL import Image
from data_processor import process_all
from ms365_connector import fetch_excel_files, check_secrets_configured, check_share_urls_configured

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
def _get_page_icon():
    for p in [Path(__file__).parent/'logo.png', Path(os.getcwd())/'logo.png']:
        if p.exists():
            try: return Image.open(p)
            except: pass
    return "📊"

st.set_page_config(page_title="Invesmate Analytics", page_icon=_get_page_icon(),
                   layout="wide", initial_sidebar_state="collapsed")
st.markdown("""<style>
  #MainMenu,footer,header{visibility:hidden}
  .block-container{padding:0!important;max-width:100%!important}
  .stApp{background:#060910}
  div[data-testid="stToolbar"]{display:none}
  section[data-testid="stSidebar"]{display:none}
  div[data-testid="stDecoration"]{display:none}
  div[data-testid="stStatusWidget"]{display:none}
  button[kind="header"]{display:none}
</style>""", unsafe_allow_html=True)

# ─── LOGO ─────────────────────────────────────────────────────────────────────
LOGO_B64 = "/9j/4AAQSkZJRgABAQAAAQABAAD/4gHYSUNDX1BST0ZJTEUAAQEAAAHIAAAAAAQwAABtbnRyUkdCIFhZWiAH4AABAAEAAAAAAABhY3NwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAQAA9tYAAQAAAADTLQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlkZXNjAAAA8AAAACRyWFlaAAABFAAAABRnWFlaAAABKAAAABRiWFlaAAABPAAAABR3dHB0AAABUAAAABRyVFJDAAABZAAAAChnVFJDAAABZAAAAChiVFJDAAABZAAAAChjcHJ0AAABjAAAADxtbHVjAAAAAAAAAAEAAAAMZW5VUwAAAAgAAAAcAHMAUgBHAEI="
LOGO_SRC = "data:image/png;base64," + LOGO_B64
LOGO_IMG  = f'<img src="{LOGO_SRC}" style="width:36px;height:36px;border-radius:50%;object-fit:cover;border:2px solid rgba(79,206,143,.5);flex-shrink:0">'

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def _hash(pw): return hashlib.sha256(pw.encode()).hexdigest()
_HERE = Path(__file__).resolve().parent

def _load_template(name):
    for p in [_HERE/f'template_{name}.html', Path(os.getcwd())/f'template_{name}.html']:
        if p.exists(): return p.read_text(encoding='utf-8')
    for p in Path(os.getcwd()).rglob(f'template_{name}.html'):
        return p.read_text(encoding='utf-8')
    return None

# ─── SESSION STATE ────────────────────────────────────────────────────────────
def _init():
    defaults = {
        'logged_in': False, 'username': '', 'role': '', 'user_name': '',
        'page': 'home', 'dashboards': None, 'active_dash': 'integrated',
        'refresh_counter': 0, 'pending': [],
    }
    for k, v in defaults.items():
        if k not in st.session_state: st.session_state[k] = v

    if 'users' not in st.session_state:
        st.session_state.users = {
            "admin":   {"hash":_hash("invesmate@2024"),"role":"admin","name":"Admin","suspended":False,"reset_token":"","is_main_admin":True},
            "analyst": {"hash":_hash("analyst@123"),   "role":"viewer","name":"Analyst","suspended":False,"reset_token":"","is_main_admin":False},
            "manager": {"hash":_hash("manager@123"),   "role":"viewer","name":"Manager","suspended":False,"reset_token":"","is_main_admin":False},
        }
    if 'ms365_enabled' not in st.session_state:
        try: ms_ok, _ = check_secrets_configured()
        except: ms_ok = False
        st.session_state.ms365_enabled = ms_ok

_init()

# ─── TEMPLATES ────────────────────────────────────────────────────────────────
TEMPLATES = {}
for _n in ['online', 'offline', 'integrated']:
    _t = _load_template(_n)
    if _t: TEMPLATES[_n] = _t
    else:
        st.error(f"❌ template_{_n}.html not found. Commit it to your repo.")
        st.stop()

# ─── DATA INJECTION ───────────────────────────────────────────────────────────
def _j(o): return json.dumps(o, ensure_ascii=False, default=str)

def build_data_js(data, mode):
    b    = _j(data.get('bcmb', []))
    i    = _j(data.get('insg', []))
    off  = _j(data.get('offline', []))
    # Offline student-level data (from Seminar Updated + Conversion + Leads)
    stu  = _j(data.get('students', []))
    ord_ = _j(data.get('orders', []))
    agg  = _j(data.get('offline_agg', {}))
    # Legacy / integrated
    sm   = _j(data.get('seminar', []))
    att  = _j(data.get('att_summary', {}))
    ct   = _j(data.get('ct_stats', {}))
    sr   = _j(data.get('sr_stats', {}))
    lc   = _j(data.get('loc_stats', {}))

    sb_js = "...BCMB_DATA.map(r=>({...r,course:'BCMB'}))"
    si_js = "...INSG_DATA.map(r=>({...r,course:'INSIGNIA'}))"
    so_js = "...OFFLINE_DATA.map(r=>({...r,course:'OFFLINE'}))"

    if mode == 'online':
        return (
            "const BCMB_DATA="+b+";const INSG_DATA="+i+";const OFFLINE_DATA=[];"
            "const ALL_DATA=["+sb_js+","+si_js+"];"
            "const STUDENTS=[];const ORDERS=[];const OFFLINE_AGG={};"
            "const SEMINAR_DATA=[];const ATTENDEE_SUMMARY={};const SALES_REP_STATS={};"
            "const COURSE_TYPE_STATS={};const LOCATION_STATS_ATT={};"
        )
    if mode == 'offline':
        return (
            "const BCMB_DATA=[];const INSG_DATA=[];const OFFLINE_DATA=[];const ALL_DATA=[];"
            "const STUDENTS="+stu+";const ORDERS="+ord_+";const OFFLINE_AGG="+agg+";"
            "const SEMINAR_DATA="+sm+";const ATTENDEE_SUMMARY="+att+";"
            "const SALES_REP_STATS="+sr+";const COURSE_TYPE_STATS="+ct+";"
            "const LOCATION_STATS_ATT="+lc+";"
        )
    # integrated
    return (
        "const BCMB_DATA="+b+";const INSG_DATA="+i+";const OFFLINE_DATA="+off+";"
        "const ALL_DATA=["+sb_js+","+si_js+","+so_js+"];"
        "const STUDENTS="+stu+";const ORDERS="+ord_+";const OFFLINE_AGG="+agg+";"
        "const SEMINAR_DATA="+sm+";const ATTENDEE_SUMMARY="+att+";"
        "const SALES_REP_STATS="+sr+";const COURSE_TYPE_STATS="+ct+";"
        "const LOCATION_STATS_ATT="+lc+";"
    )

def inject_data(tmpl, js): return tmpl.replace('// @@DATA@@', js, 1)
def build_all(data):
    return {n: inject_data(TEMPLATES[n], build_data_js(data, n)) for n in ['online','offline','integrated']}

# ─── SHARED CSS ───────────────────────────────────────────────────────────────
def inject_fonts():
    st.markdown('<link href="https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">', unsafe_allow_html=True)

# ─── NAVBAR ───────────────────────────────────────────────────────────────────
def render_navbar(active='home'):
    inject_fonts()
    users     = st.session_state.users
    is_admin  = st.session_state.role == 'admin'
    is_main   = users.get(st.session_state.username, {}).get('is_main_admin', False)
    user_name = st.session_state.user_name
    pending_n = len(st.session_state.pending)

    if is_main:
        role_badge = '<span style="background:rgba(247,201,72,.12);border:1px solid rgba(247,201,72,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#f7c948;text-transform:uppercase">Main Admin</span>'
    elif is_admin:
        role_badge = '<span style="background:rgba(180,79,231,.12);border:1px solid rgba(180,79,231,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#b44fe7;text-transform:uppercase">Admin</span>'
    else:
        role_badge = '<span style="background:rgba(79,142,247,.12);border:1px solid rgba(79,142,247,.25);border-radius:8px;padding:2px 8px;font-size:9px;font-weight:700;color:#4f8ef7;text-transform:uppercase">Viewer</span>'

    pending_badge = (f'<span style="background:#f76f4f;color:#fff;border-radius:50%;width:16px;height:16px;display:inline-flex;align-items:center;justify-content:center;font-size:9px;font-weight:800;margin-left:4px">{pending_n}</span>' if pending_n > 0 else '')

    st.markdown(f"""
<style>
.im-nav{{background:linear-gradient(180deg,#0d1119 0%,#080b12 100%);border-bottom:1px solid rgba(255,255,255,.07);
  padding:0 24px;height:60px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:9999}}
.im-brand{{font-family:'Syne',sans-serif;font-size:16px;font-weight:800;color:#eceef5;letter-spacing:-.3px;line-height:1.1}}
.im-brand-sub{{font-size:9px;color:#4a5068;text-transform:uppercase;letter-spacing:.9px}}
.im-user-pill{{background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);border-radius:20px;
  padding:4px 12px 4px 8px;display:flex;align-items:center;gap:7px;font-size:12px;color:#8a90aa}}
.im-dot{{width:7px;height:7px;background:#4fce8f;border-radius:50%;animation:imdot 2s infinite;flex-shrink:0}}
@keyframes imdot{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
</style>
<div class="im-nav">
  <div style="display:flex;align-items:center;gap:11px">
    {LOGO_IMG}
    <div><div class="im-brand">Invesmate</div><div class="im-brand-sub">Analytics Hub</div></div>
  </div>
  <div style="display:flex;align-items:center;gap:10px">
    <div class="im-user-pill"><div class="im-dot"></div><span>{user_name}</span></div>
    {role_badge}
  </div>
</div>""", unsafe_allow_html=True)

    if is_admin:
        cols = st.columns([2,1,1,1,1,2])
        btn_map = [(1,'🏠 Home','home'),(2,'📊 Dashboard','dashboard'),(3,f'⚙️ Admin{pending_badge}','admin'),(4,'🚪 Logout','logout')]
    else:
        cols = st.columns([2,1,1,1,2])
        btn_map = [(1,'🏠 Home','home'),(2,'📊 Dashboard','dashboard'),(3,'🚪 Logout','logout')]

    for ci, label, action in btn_map:
        with cols[ci]:
            if st.button(label, key=f'nb_{action}', use_container_width=True,
                         type="primary" if active==action else "secondary"):
                if action == 'logout':
                    _u = st.session_state.get('users',{}); _p = st.session_state.get('pending',[])
                    for k in list(st.session_state.keys()): del st.session_state[k]
                    st.session_state.users = _u; st.session_state.pending = _p
                else:
                    st.session_state.page = action
                st.rerun()

# ─── LOGIN ────────────────────────────────────────────────────────────────────
def show_login():
    inject_fonts()
    st.markdown(f"""
<style>body,.stApp{{background:#060910}}
.lshell{{min-height:100vh;display:flex;align-items:center;justify-content:center;
  background:radial-gradient(ellipse at 25% 25%,rgba(79,142,247,.1) 0%,transparent 55%),
             radial-gradient(ellipse at 75% 75%,rgba(79,206,143,.07) 0%,transparent 55%),#060910;padding:40px 20px}}
.lcard{{background:linear-gradient(145deg,#0c1018,#090d14);border:1px solid rgba(255,255,255,.08);
  border-radius:22px;padding:40px 46px;width:100%;max-width:400px;box-shadow:0 32px 100px rgba(0,0,0,.7)}}
.lt{{font-family:'Syne',sans-serif;font-size:24px;font-weight:800;color:#eceef5;text-align:center;margin:14px 0 4px;letter-spacing:-.5px}}
.ls{{font-size:11px;color:#4a5068;text-align:center;margin-bottom:30px;text-transform:uppercase;letter-spacing:.8px}}</style>
<div class="lshell"><div class="lcard">
  <div style="text-align:center"><img src="{LOGO_SRC}" style="width:76px;height:76px;border-radius:50%;object-fit:cover;border:3px solid rgba(79,206,143,.4)"></div>
  <div class="lt">Invesmate Analytics</div><div class="ls">Sign in to continue</div>
</div></div>""", unsafe_allow_html=True)

    c1,c2,c3 = st.columns([1,2,1])
    with c2:
        st.markdown("<div style='margin-top:-300px'>", unsafe_allow_html=True)
        username = st.text_input("", placeholder="👤  Username", key="lu")
        st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
        password = st.text_input("", placeholder="🔑  Password", type="password", key="lp")
        st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
        if st.button("Sign In  →", use_container_width=True, type="primary", key="lbtn"):
            u = st.session_state.users.get((username or '').strip().lower())
            if u and u['hash'] == _hash(password or ''):
                if u.get('suspended', False):
                    st.error("🚫 Your account has been suspended.")
                else:
                    st.session_state.logged_in = True
                    st.session_state.username  = username.strip().lower()
                    st.session_state.role      = u['role']
                    st.session_state.user_name = u['name']
                    st.session_state.page      = 'home'
                    st.rerun()
            else:
                st.error("❌ Invalid credentials.")
        st.markdown("</div>", unsafe_allow_html=True)

# ─── HOME ─────────────────────────────────────────────────────────────────────
def show_home():
    render_navbar('home')
    inject_fonts()
    ms_on = st.session_state.ms365_enabled

    st.markdown(f"""
<style>
.hh{{text-align:center;padding:48px 20px 32px}}
.hh1{{font-family:'Syne',sans-serif;font-size:38px;font-weight:800;color:#eceef5;margin:14px 0 8px;letter-spacing:-1px}}
.hsub{{color:#4a5068;font-size:12px;text-transform:uppercase;letter-spacing:.8px}}
.dprow{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;max-width:780px;margin:28px auto 0}}
.dp{{border-radius:12px;padding:14px 18px;font-size:12px;font-weight:700;color:#fff;text-align:center;border:1px solid}}
.dpo{{background:linear-gradient(135deg,rgba(79,142,247,.2),rgba(180,79,231,.1));border-color:rgba(79,142,247,.3)}}
.dpf{{background:linear-gradient(135deg,rgba(247,111,79,.2),rgba(180,79,231,.1));border-color:rgba(247,111,79,.3)}}
.dpi{{background:linear-gradient(135deg,rgba(79,206,143,.15),rgba(79,142,247,.1));border-color:rgba(79,206,143,.25)}}
.ibox{{background:rgba(79,142,247,.05);border:1px solid rgba(79,142,247,.12);border-radius:14px;padding:16px 20px;
  margin:20px auto;max-width:900px;color:#8a90aa;font-size:13px;line-height:1.8}}
.ibox strong{{color:#eceef5}}
.live-badge{{display:inline-flex;align-items:center;gap:5px;background:rgba(79,206,143,.1);border:1px solid rgba(79,206,143,.25);
  border-radius:20px;padding:4px 12px;font-size:11px;color:#4fce8f;font-weight:600;margin:10px auto;width:fit-content}}
.live-dot{{width:6px;height:6px;background:#4fce8f;border-radius:50%;animation:ldot 2s infinite}}
@keyframes ldot{{0%,100%{{opacity:1}}50%{{opacity:.2}}}}
@media(max-width:700px){{.dprow{{grid-template-columns:1fr}}}}</style>
<div class="hh">
  <img src="{LOGO_SRC}" style="width:88px;height:88px;border-radius:50%;object-fit:cover;border:3px solid rgba(79,206,143,.4);box-shadow:0 0 40px rgba(79,206,143,.18)">
  <div class="hh1">Invesmate Analytics Hub</div>
  <div class="hsub">{'Live Microsoft 365 data' if ms_on else 'Upload your Excel files'} · 3 interactive dashboards</div>
</div>
<div class="dprow">
  <div class="dp dpo">🎥 Online Dashboard<br><small style="font-weight:400;opacity:.8">BCMB + INSIGNIA webinars</small></div>
  <div class="dp dpf">🏢 Offline Dashboard<br><small style="font-weight:400;opacity:.8">Seminar · Students · Sales</small></div>
  <div class="dp dpi">📊 Integrated Dashboard<br><small style="font-weight:400;opacity:.8">Everything combined</small></div>
</div>""", unsafe_allow_html=True)

    mc1, mc2, mc3 = st.columns([2,2,2])
    with mc2:
        c1, c2 = st.columns(2)
        with c1:
            if st.button("☁️ Live Data"+" ●"*ms_on, key="mode_live", use_container_width=True,
                         type="primary" if ms_on else "secondary"):
                st.session_state.ms365_enabled = True; st.rerun()
        with c2:
            if st.button("📁 Upload Files"+" ●"*(not ms_on), key="mode_upload", use_container_width=True,
                         type="primary" if not ms_on else "secondary"):
                st.session_state.ms365_enabled = False; st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    if ms_on:
        st.markdown("""<div style="text-align:center"><div class="live-badge"><div class="live-dot"></div>Connected to Microsoft 365</div></div>""", unsafe_allow_html=True)
        st.markdown("""<div class="ibox">
  <strong>Live mode — 4 files fetched automatically:</strong><br>
  🔵 <strong>Free Class Lead Report</strong> — BCMB &amp; INSIGNIA webinar data<br>
  🟢 <strong>Seminar Updated Sheet</strong> — Attendance, seat bookings, student info<br>
  🟡 <strong>Conversion List</strong> — Orders, payments, course purchases<br>
  🔴 <strong>Leads Report</strong> — Lead source, campaign, stage, owner<br><br>
  Click <strong>🔄 Refresh &amp; Build</strong> to load latest data from SharePoint.
</div>""", unsafe_allow_html=True)

        if st.session_state.get('last_refresh'):
            st.markdown(f'<div style="text-align:center;font-size:11px;color:#4a5068;margin-bottom:10px">Last refreshed: {st.session_state.last_refresh}</div>', unsafe_allow_html=True)

        _, cb, _ = st.columns([1,2,1])
        with cb:
            if st.button("🔄  Refresh & Build Dashboards", use_container_width=True, type="primary", key="live_refresh"):
                with st.spinner("Fetching files from Microsoft 365…"):
                    try:
                        st.session_state.refresh_counter += 1
                        files = fetch_excel_files(st.session_state.refresh_counter)
                        with st.spinner("Parsing & building dashboards…"):
                            data = process_all(
                                webinar_file=files.get('webinar'),
                                seminar_updated_file=files.get('seminar_updated'),
                                conversion_file=files.get('conversion'),
                                leads_file=files.get('leads'),
                                sem_name='seminar_updated.xlsx',
                                conv_name='conversion.xlsx',
                                leads_name='leads.xlsx',
                            )
                        if data['errors']:
                            for e in data['errors']: st.warning(f"⚠️ {e}")
                        st.session_state.dashboards  = build_all(data)
                        st.session_state.active_dash = 'integrated'
                        from datetime import datetime
                        st.session_state.last_refresh = datetime.now().strftime("%d %b %Y, %H:%M:%S")
                        s = data['stats']
                        st.success(f"✅ Done — BCMB:{s['bcmb_count']} · INSIGNIA:{s['insg_count']} · Students:{s['students']:,} · Conversions:{s['conversions']}")
                        st.session_state.page = 'dashboard'; st.rerun()
                    except (ConnectionError, PermissionError, FileNotFoundError, ValueError) as e:
                        st.error(str(e))
                    except Exception as e:
                        st.error(f"❌ Unexpected error: {e}")
                        import traceback; st.code(traceback.format_exc())

            if st.session_state.role == 'admin':
                with st.expander("⚙️ Microsoft 365 Configuration", expanded=False):
                    _show_ms365_setup()
    else:
        st.markdown("""<div class="ibox">
  <strong>Manual upload — 4 files (1 required + 3 for offline):</strong><br>
  🔵 <strong>Free_Class_Lead_Report.xlsx</strong> — BCMB &amp; INSIGNIA sheets <em>(online dashboard)</em><br>
  🟢 <strong>Seminar_Updated_Sheet.xlsx</strong> — Attendance, seat bookings, student names<br>
  🟡 <strong>Conversion_List.xlsx</strong> — Orders, payment_received, due, course names<br>
  🔴 <strong>Leads_Report.xlsx</strong> — Lead source, campaign, stage, owner, attempted
</div>""", unsafe_allow_html=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🔵</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Free Class Lead Report</div><div style="font-size:10px;color:#4a5068">BCMB &amp; INSIGNIA online webinars</div></div>', unsafe_allow_html=True)
            wf = st.file_uploader("wf", type=['xlsx','xls','csv'], key='wf', label_visibility='collapsed')
        with c2:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🟢</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Seminar Updated Sheet</div><div style="font-size:10px;color:#4a5068">Student attendance &amp; seat bookings</div></div>', unsafe_allow_html=True)
            suf = st.file_uploader("suf", type=['xlsx','xls','csv'], key='suf', label_visibility='collapsed')

        c3, c4 = st.columns(2)
        with c3:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🟡</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Conversion List</div><div style="font-size:10px;color:#4a5068">Orders, payments, courses</div></div>', unsafe_allow_html=True)
            cvf = st.file_uploader("cvf", type=['xlsx','xls','csv'], key='cvf', label_visibility='collapsed')
        with c4:
            st.markdown('<div style="background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:12px;padding:14px;margin-bottom:8px"><span style="font-size:20px">🔴</span><div style="font-family:Syne,sans-serif;font-size:11px;font-weight:700;color:#eceef5;margin:5px 0 2px">Leads Report</div><div style="font-size:10px;color:#4a5068">Lead source, campaign, stage, owner</div></div>', unsafe_allow_html=True)
            ldf = st.file_uploader("ldf", type=['xlsx','xls','csv'], key='ldf', label_visibility='collapsed')

        st.markdown("<br>", unsafe_allow_html=True)
        _, cb, _ = st.columns([1,2,1])
        with cb:
            ready_offline = suf and cvf  # ldf optional
            ready_any = wf or ready_offline
            if ready_any:
                if st.button("🚀  Generate All 3 Dashboards", use_container_width=True, type="primary"):
                    with st.spinner("Parsing files and building dashboards…"):
                        try:
                            data = process_all(
                                webinar_file=wf,
                                seminar_updated_file=suf,
                                conversion_file=cvf,
                                leads_file=ldf,
                                sem_name=suf.name if suf else '',
                                conv_name=cvf.name if cvf else '',
                                leads_name=ldf.name if ldf else '',
                            )
                            if data['errors']:
                                for e in data['errors']: st.warning(f"⚠️ {e}")
                            st.session_state.dashboards  = build_all(data)
                            st.session_state.active_dash = 'integrated'
                            s = data['stats']
                            st.success(f"✅ Done — BCMB:{s['bcmb_count']} · INSIGNIA:{s['insg_count']} · Students:{s['students']:,} · Conversions:{s['conversions']}")
                            st.session_state.page = 'dashboard'; st.rerun()
                        except Exception as e:
                            st.error(f"❌ {e}")
                            import traceback; st.code(traceback.format_exc())
            else:
                st.markdown('<div style="text-align:center;padding:14px;background:rgba(255,255,255,.02);border:1px solid rgba(255,255,255,.05);border-radius:10px;color:#4a5068;font-size:13px">Upload at least <strong style="color:#8a90aa">Seminar Updated Sheet + Conversion List</strong> for offline dashboard</div>', unsafe_allow_html=True)


def _show_ms365_setup():
    share_status = check_share_urls_configured()
    ok, missing  = check_secrets_configured()
    st.markdown("""
**Add to Streamlit Cloud → App Settings → Secrets:**
```toml
MS_EMAIL    = "admin@admininvesmate360.onmicrosoft.com"
MS_PASSWORD = "your-password"

SHARE_URL_WEBINAR        = "https://..."
SHARE_URL_SEMINAR_UPDATE = "https://..."   # Seminar Updated Sheet
SHARE_URL_CONVERSION     = "https://..."   # Conversion List
SHARE_URL_LEADS          = "https://..."   # Leads Report
```
""")
    if ok: st.success("✅ MS_EMAIL + MS_PASSWORD configured.")
    else:  st.error(f"❌ Missing: {', '.join(missing)}")
    st.markdown("**Share URL status:**")
    cols = st.columns(2)
    for i, (secret, configured) in enumerate(share_status.items()):
        with cols[i % 2]:
            st.markdown(f"`{'✅' if configured else '❌'} {secret}`")

# ─── DASHBOARD ────────────────────────────────────────────────────────────────
def show_dashboard():
    render_navbar('dashboard')
    if not st.session_state.dashboards:
        st.markdown("<div style='padding:40px;text-align:center;color:#4a5068'>No dashboards yet. Go to Home to upload files.</div>", unsafe_allow_html=True)
        _, cb, _ = st.columns([1,2,1])
        with cb:
            if st.button("← Go Home", use_container_width=True):
                st.session_state.page = 'home'; st.rerun()
        return

    active = st.session_state.active_dash
    DASH = {'online':'🎥 Online','offline':'🏢 Offline','integrated':'📊 Integrated'}

    st.markdown("<div style='background:#0a0e16;border-bottom:1px solid rgba(255,255,255,.06);padding:8px 22px'></div>", unsafe_allow_html=True)
    tc = st.columns([1,1,1,4,1])
    for idx, (key, label) in enumerate(DASH.items()):
        with tc[idx]:
            if st.button(label, key=f'dt_{key}', use_container_width=True,
                         type="primary" if key==active else "secondary"):
                st.session_state.active_dash = key; st.rerun()
    with tc[4]:
        if st.button("← New Files", use_container_width=True):
            st.session_state.dashboards = None; st.session_state.active_dash = 'integrated'
            st.session_state.page = 'home'; st.rerun()
    components.html(st.session_state.dashboards[active], height=950, scrolling=True)

# ─── ADMIN ────────────────────────────────────────────────────────────────────
def show_admin():
    if st.session_state.role != 'admin':
        st.error("⛔ Access denied."); return
    render_navbar('admin')
    inject_fonts()
    users   = st.session_state.users
    me      = st.session_state.username
    is_main = users.get(me, {}).get('is_main_admin', False)
    pending = st.session_state.pending

    st.markdown(f"""<style>
.aw{{max-width:1060px;margin:0 auto;padding:26px 22px 60px}}
.asec{{background:#0c1018;border:1px solid rgba(255,255,255,.07);border-radius:14px;padding:20px 22px;margin-bottom:16px}}
.asec-t{{font-family:'Syne',sans-serif;font-size:10px;font-weight:700;color:#f7c948;margin-bottom:14px;text-transform:uppercase;letter-spacing:.9px}}
.sg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:11px}}
.sc{{background:#111520;border:1px solid rgba(255,255,255,.06);border-radius:12px;padding:14px 16px;position:relative;overflow:hidden}}
.sc::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--c,#4f8ef7)}}
.sv{{font-family:'Syne',sans-serif;font-size:26px;font-weight:800;color:#eceef5;line-height:1}}
.sl{{font-size:10px;color:#4a5068;text-transform:uppercase;letter-spacing:.5px;margin-top:5px}}
.ut-grid{{display:grid;grid-template-columns:1.6fr 0.9fr 0.7fr 2fr;gap:8px;align-items:center}}
.ut-row{{padding:9px 10px;border-bottom:1px solid rgba(255,255,255,.04)}}
.badg{{border-radius:8px;padding:2px 8px;font-size:10px;font-weight:700;display:inline-block}}
.ba{{background:rgba(247,201,72,.1);border:1px solid rgba(247,201,72,.2);color:#f7c948}}
.bm{{background:rgba(180,79,231,.1);border:1px solid rgba(180,79,231,.2);color:#b44fe7}}
.bv{{background:rgba(79,142,247,.1);border:1px solid rgba(79,142,247,.2);color:#4f8ef7}}
.bs{{background:rgba(247,111,79,.1);border:1px solid rgba(247,111,79,.2);color:#f76f4f}}
.bok{{background:rgba(79,206,143,.1);border:1px solid rgba(79,206,143,.2);color:#4fce8f}}
.tok-box{{background:#060910;border:1px solid rgba(79,206,143,.2);border-radius:8px;padding:10px 14px;margin-top:8px;font-family:monospace;font-size:12px;color:#4fce8f;word-break:break-all}}</style>
<div class="aw"><h2 style="font-family:Syne,sans-serif;font-size:18px;font-weight:800;margin-bottom:18px">⚙️ Admin Panel — {'Main Admin' if is_main else 'Admin'}</h2></div>""", unsafe_allow_html=True)

    st.markdown('<div class="aw">', unsafe_allow_html=True)

    # Stats
    total_u  = len(users); active_u = sum(1 for u in users.values() if not u.get('suspended')); admin_u = sum(1 for u in users.values() if u['role']=='admin')
    st.markdown(f'''<div class="asec"><div class="asec-t">📊 System Overview</div><div class="sg">
      <div class="sc" style="--c:#4f8ef7"><div class="sv">{total_u}</div><div class="sl">Total Users</div></div>
      <div class="sc" style="--c:#4fce8f"><div class="sv">{active_u}</div><div class="sl">Active</div></div>
      <div class="sc" style="--c:#f7c948"><div class="sv">{admin_u}</div><div class="sl">Admins</div></div>
      <div class="sc" style="--c:#4fd8f7"><div class="sv">{len(pending)}</div><div class="sl">Pending</div></div>
    </div></div>''', unsafe_allow_html=True)

    # User table
    st.markdown('<div class="asec"><div class="asec-t">👥 User Management</div>', unsafe_allow_html=True)
    for uname, ud in list(users.items()):
        is_self=uname==me; is_susp=ud.get('suspended',False); is_main_u=ud.get('is_main_admin',False); role=ud['role']
        rbadge = '<span class="badg ba">Main Admin</span>' if is_main_u else ('<span class="badg bm">Admin</span>' if role=='admin' else '<span class="badg bv">Viewer</span>')
        sbadge = '<span class="badg bs">Suspended</span>' if is_susp else '<span class="badg bok">Active</span>'
        you_tag = ' <span style="font-size:10px;color:#4fce8f">(you)</span>' if is_self else ''
        st.markdown(f'<div class="ut-row"><b>{ud["name"]}{you_tag}</b> @{uname} &nbsp;{rbadge}&nbsp;{sbadge}</div>', unsafe_allow_html=True)
        if not is_self and not is_main_u:
            a1,a2,a3,a4,_ = st.columns([0.6,0.7,0.6,0.6,2])
            with a1:
                lbl = "▶ Activate" if is_susp else "⏸ Suspend"
                if st.button(lbl, key=f"s_{uname}", use_container_width=True):
                    action = 'activate' if is_susp else 'suspend'
                    if is_main: _apply_action({'action':action,'target':uname,'payload':{},'req_by':me}); st.success(f"✅ Done")
                    else: _queue(action,uname,{},me)
                    st.rerun()
            with a2:
                nr = 'admin' if role=='viewer' else 'viewer'
                if st.button(f"→ {nr.title()}", key=f"r_{uname}", use_container_width=True):
                    if is_main: _apply_action({'action':'change_role','target':uname,'payload':{'new_role':nr},'req_by':me}); st.success(f"✅ Done")
                    else: _queue('change_role',uname,{'new_role':nr},me)
                    st.rerun()
            with a3:
                if st.button("🔑 Reset", key=f"rk_{uname}", use_container_width=True):
                    if is_main:
                        tok=secrets.token_urlsafe(10); st.session_state.users[uname]['reset_token']=tok; st.session_state[f'tok_{uname}']=tok
                    else: _queue('reset_token',uname,{},me)
                    st.rerun()
            with a4:
                if sum(1 for u in users.values() if u['role']=='admin') > 1 or role!='admin':
                    if st.button("🗑 Delete", key=f"d_{uname}", use_container_width=True):
                        st.session_state[f'cdel_{uname}']=True; st.rerun()
        if st.session_state.get(f'tok_{uname}'):
            st.markdown(f'<div class="tok-box">🔑 Reset token for @{uname}: <b>{st.session_state[f"tok_{uname}"]}</b></div>', unsafe_allow_html=True)
            if st.button("✖ Dismiss", key=f"dis_{uname}"): del st.session_state[f'tok_{uname}']; st.rerun()
        if st.session_state.get(f'cdel_{uname}'):
            st.warning(f"Delete {ud['name']}?")
            cy,cn=st.columns(2)
            with cy:
                if st.button("✅ Yes", key=f"cy_{uname}", type="primary", use_container_width=True):
                    if is_main: _apply_action({'action':'delete','target':uname,'payload':{},'req_by':me})
                    else: _queue('delete',uname,{},me)
                    if f'cdel_{uname}' in st.session_state: del st.session_state[f'cdel_{uname}']
                    st.rerun()
            with cn:
                if st.button("✖ Cancel", key=f"cn_{uname}", use_container_width=True): del st.session_state[f'cdel_{uname}']; st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

    # Add user
    st.markdown('<div class="asec"><div class="asec-t">➕ Add New User</div>', unsafe_allow_html=True)
    c1,c2,c3,c4=st.columns(4)
    nu=c1.text_input("Username",key="nu",placeholder="username"); nn=c2.text_input("Display Name",key="nn",placeholder="Full Name")
    np_=c3.text_input("Password",key="np_",placeholder="password",type="password"); nr2=c4.selectbox("Role",["viewer","admin"],key="nr2")
    if st.button("➕ Add User",key="au",type="primary"):
        if nu and nn and np_:
            ukey=nu.strip().lower()
            if ukey in st.session_state.users: st.warning(f"'{ukey}' exists.")
            else:
                st.session_state.users[ukey]={"hash":_hash(np_),"role":nr2,"name":nn.strip(),"suspended":False,"reset_token":"","is_main_admin":False}
                st.success(f"✅ Added '{ukey}'."); st.rerun()
        else: st.warning("Fill all fields.")
    st.markdown('</div>', unsafe_allow_html=True)

    # Change password
    st.markdown('<div class="asec"><div class="asec-t">🔑 Change Password</div>', unsafe_allow_html=True)
    c1,c2,c3=st.columns(3)
    cpu=c1.selectbox("User",list(users.keys()),key="cpu"); cpn=c2.text_input("New Password",key="cpn",type="password"); cpc=c3.text_input("Confirm",key="cpc",type="password")
    if st.button("🔑 Update",key="cpb",type="primary"):
        if cpn and cpn==cpc: st.session_state.users[cpu]['hash']=_hash(cpn); st.session_state.users[cpu]['reset_token']=''; st.success("✅ Updated.")
        elif cpn!=cpc: st.error("Passwords don't match.")
        else: st.warning("Enter password.")
    st.markdown('</div></div>', unsafe_allow_html=True)

def _queue(action,target,payload,req_by):
    st.session_state.pending.append({'action':action,'target':target,'payload':payload,'req_by':req_by})
    st.info("📨 Request queued for main admin approval.")

def _apply_action(req):
    users=st.session_state.users; target=req['target']; action=req['action']
    if action=='suspend': users[target]['suspended']=True
    elif action=='activate': users[target]['suspended']=False
    elif action=='change_role': users[target]['role']=req['payload']['new_role']
    elif action=='delete':
        if target in users: del users[target]
    elif action=='reset_token':
        tok=secrets.token_urlsafe(10); users[target]['reset_token']=tok; st.session_state[f'tok_{target}']=tok

# ─── ROUTER ───────────────────────────────────────────────────────────────────
if not st.session_state.logged_in:
    show_login()
else:
    pg = st.session_state.page
    if   pg == 'home':      show_home()
    elif pg == 'dashboard': show_dashboard()
    elif pg == 'admin':     show_admin()
    else:                   show_home()
