"""
data_processor.py  —  Invesmate Analytics Dashboard
Parses all Excel files and returns JSON-serialisable dicts
ready to be injected into the dashboard HTML templates.

Files expected:
  1. Free_Class_Lead_Report.xlsx   → BCMB sheet + INSG / INSIGNIA sheet
  2. Offline_Seminar_Report.xlsx   → "Offline Report" sheet
  3. Offline_Indepth_Details.xlsx  → one sheet per location (student records)
  4. Seminar Updated Files.xlsx    → updated seminar data (multi-sheet)
  5. Conversion List.xlsx          → lead conversion tracking
  6. Leads.xlsx                    → raw leads data
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# LOW-LEVEL HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _n(val):
    """Safe float conversion, returns 0 on NaN / error."""
    try:
        v = float(val)
        return 0 if (np.isnan(v) or np.isinf(v)) else v
    except Exception:
        return 0


def _s(val):
    """Safe string, returns '' on None / NaN."""
    if val is None:
        return ''
    try:
        if isinstance(val, float) and np.isnan(val):
            return ''
    except Exception:
        pass
    return str(val).strip()


def _d(val):
    """
    Safe date → 'YYYY-MM-DD' string.
    Handles datetime objects, Timestamps, and messy strings like '18/8/2023 19/8/2023'.
    """
    try:
        if pd.isna(val):
            return ''
    except Exception:
        pass
    try:
        if isinstance(val, (datetime, pd.Timestamp)):
            return val.strftime('%Y-%m-%d')
        s = str(val).strip()
        if '/' in s:
            s = re.split(r'\s{2,}', s)[0].strip()
            return pd.to_datetime(s, dayfirst=True).strftime('%Y-%m-%d')
        return s[:10] if len(s) >= 10 else ''
    except Exception:
        return ''


def _col(df, *keywords, exact=False, exclude=None):
    """
    Return the first column name whose lowercase version contains any keyword.
    If `exact=True` the column must equal the keyword exactly (case-insensitive).
    Columns containing any string in `exclude` are skipped.
    """
    excl = [e.lower() for e in (exclude or [])]
    cols = [str(c) for c in df.columns]
    for kw in keywords:
        kw_l = kw.lower()
        for c in cols:
            c_l = c.lower()
            if any(e in c_l for e in excl):
                continue
            if exact:
                if c_l == kw_l:
                    return c
            else:
                if kw_l in c_l:
                    return c
    return None


# ──────────────────────────────────────────────────────────────────────────────
# TRAINER NAME NORMALISATION
# ──────────────────────────────────────────────────────────────────────────────

TRAINER_MAP = {
    'rohitava majumdar':                    'Rohitava Majumder',
    'rohitav majumder':                     'Rohitava Majumder',
    'rohitava majumder**':                  'Rohitava Majumder',
    'debargha  saha':                       'Debargho Saha',
    'debargha saha':                        'Debargho Saha',
    'debargho\u00a0saha':                   'Debargho Saha',
    'pratim kumer chakraborty':             'Pratim Kumar Chakraborty',
    'hironmoy laheri':                      'Hironmoy Lahiri',
    'hironmoy lahiri\u00a0':               'Hironmoy Lahiri',
    'sandipan das':                         'Sandipan Kumar Das',
    'kunal saha (special advanced class)':  'Kunal Saha',
    'sayan sarker(special advanced class)': 'Sayan Sarker',
}


def _norm_trainer(raw: str) -> str:
    parts = [p.strip() for p in re.split(r',|&|\n', str(raw)) if p.strip()]
    out = []
    for p in parts:
        p = re.sub(r'\s*\(Special Advanced Class\)\s*', '', p, flags=re.I).strip()
        p = re.sub(r'\s+', ' ', p)
        out.append(TRAINER_MAP.get(p.lower(), p))
    return ', '.join(dict.fromkeys(out))   # dedupe, preserve order


# ──────────────────────────────────────────────────────────────────────────────
# FILE 1  —  Free_Class_Lead_Report  →  BCMB + INSIGNIA
# ──────────────────────────────────────────────────────────────────────────────

_SHEET_SKIP = {
    'log', 'hitting', 'call', 're-target', 'retarget', 'backup', 'rough',
    'comparison', 'summary', 'offline', 'forx', 'fund', 'hindi', 'invesmeet',
    'simplify', 'monitoring', 'lead wise', 'joining', 'percentage',
    'day to day', 'sheet1', '8_45', 'sunday', 'tuesday', 'friday',
}


def _pick_sheet(xl, keyword: str):
    """Return the shortest sheet name containing `keyword` that isn't in skip list."""
    candidates = []
    for s in xl.sheet_names:
        sl = s.lower()
        if keyword not in sl:
            continue
        if any(sk in sl for sk in _SHEET_SKIP):
            continue
        candidates.append(s)
    candidates.sort(key=len)
    return candidates[0] if candidates else None


def _parse_bcmb(xl, sheet_name: str) -> list:
    if not sheet_name:
        return []

    df = xl.parse(sheet_name, header=0)
    df.columns = [str(c).strip() for c in df.columns]

    c_trainer  = (_col(df, 'trainer', exact=True) or
                  _col(df, 'trainer', exclude=['re-target', 'retarget']))
    c_type     = _col(df, 'type', exact=True) or _col(df, 'location', exact=True)
    c_date     = (_col(df, 'date', exact=True) or
                  _col(df, 'date', exclude=['web', 'hitting', 'batch']))
    c_targeted = (_col(df, 'targeted', exact=True) or
                  _col(df, 'targeted',
                       exclude=['to', '%', 're-', 'retarget', 'dialed',
                                'visited', 'regist', 'over', 'seat', 'new', 'old']))
    c_reg      = _col(df, 'registered', exact=True) or _col(df, 'registered', exclude=['%', 'to'])
    c_over30   = (_col(df, 'over 30 min', exact=True) or
                  _col(df, 'over 30', exclude=['%', 'to']))
    c_sb       = (_col(df, 'seat booked', exact=True) or
                  _col(df, 'seat booked', exclude=['%', 'to', 'amount']))
    c_joined   = (_col(df, 'total joined', exact=True) or
                  _col(df, 'joined', exclude=['%', 're-', 'new', 'old', 'semi']))
    c_rev      = _col(df, 'seat booking amount') or _col(df, 'course amount')

    records = []
    for _, row in df.iterrows():
        date_val = _d(row.get(c_date, '')) if c_date else ''
        targeted = int(_n(row.get(c_targeted, 0))) if c_targeted else 0
        if not date_val or targeted < 1:
            continue

        trainer  = _norm_trainer(_s(row.get(c_trainer, 'Unknown')) if c_trainer else 'Unknown')
        type_raw = _s(row.get(c_type, 'Live')) if c_type else 'Live'
        reg      = int(_n(row.get(c_reg,    0))) if c_reg    else 0
        over30   = int(_n(row.get(c_over30, 0))) if c_over30 else 0
        sb       = int(_n(row.get(c_sb,     0))) if c_sb     else 0
        joined   = int(_n(row.get(c_joined, 0))) if c_joined else 0
        revenue  = int(_n(row.get(c_rev,    0))) if c_rev    else 0

        t = type_raw.upper()
        if 'REC' in t:                          wtype = 'Rec'
        elif 'BACKUP' in t or 'BACK' in t:      wtype = 'Backup'
        elif 'PRACTICE' in t:                   wtype = 'Practice'
        elif 'CANCEL' in t:                     wtype = 'Cancel'
        elif 'ZOOM' in t:                       wtype = 'Live\n(ZOOM)'
        else:                                    wtype = 'Live'

        if revenue == 0 and sb > 0:
            revenue = sb * 5632          # ₹5,632 avg BCMB seat-booking fee

        ym = date_val[:7]
        records.append({
            'date': date_val, 'yearMonth': ym,
            'trainer': trainer, 'course': 'BCMB',
            'type': wtype, 'mode': 'Online',
            'targeted': targeted, 'registered': reg,
            'over30': over30, 'seatBooked': sb,
            'joined': joined, 'revenue': revenue,
            'expenses': 0, 'surplus': revenue,
        })

    return sorted(records, key=lambda r: r['date'])


def _parse_insg(xl, sheet_name: str) -> list:
    if not sheet_name:
        return []

    df = xl.parse(sheet_name, header=0)
    df.columns = [str(c).strip() for c in df.columns]

    c_trainer  = _col(df, 'trainer', exact=True)
    c_type     = _col(df, 'type', exact=True)
    c_date     = (_col(df, 'date', exact=True) or
                  _col(df, 'date', exclude=['web', 'hitting', 'hidden', 'batch']))
    c_targeted = (_col(df, 'targated', exact=True) or
                  _col(df, 'targeted', exact=True) or
                  _col(df, 'targated', exclude=['%', 'to']))
    c_reg      = _col(df, 'registered', exact=True) or _col(df, 'registered', exclude=['%', 'to'])
    c_over30   = _col(df, 'over 30 min', exact=True) or _col(df, 'over 30', exclude=['%', 'to'])
    c_sb       = _col(df, 'seat booked', exact=True) or _col(df, 'seat booked', exclude=['%', 'to'])
    c_joined   = _col(df, 'unique viewer') or _col(df, 'total joined') or _col(df, 'joined', exclude=['%'])

    records = []
    for _, row in df.iterrows():
        date_val = _d(row.get(c_date, '')) if c_date else ''
        targeted = int(_n(row.get(c_targeted, 0))) if c_targeted else 0
        if not date_val or targeted < 1:
            continue

        trainer  = _norm_trainer(_s(row.get(c_trainer, 'Unknown')) if c_trainer else 'Unknown')
        type_raw = _s(row.get(c_type, 'Live')) if c_type else 'Live'
        reg      = int(_n(row.get(c_reg,    0))) if c_reg    else 0
        over30   = int(_n(row.get(c_over30, 0))) if c_over30 else 0
        sb       = int(_n(row.get(c_sb,     0))) if c_sb     else 0
        joined   = int(_n(row.get(c_joined, 0))) if c_joined else 0
        revenue  = sb * 8999                     # INSIGNIA avg course fee

        wtype = 'Rec' if 'REC' in type_raw.upper() else 'Live'
        ym    = date_val[:7]

        records.append({
            'date': date_val, 'yearMonth': ym,
            'trainer': trainer, 'course': 'INSIGNIA',
            'type': wtype, 'mode': 'Online',
            'targeted': targeted, 'registered': reg,
            'over30': over30, 'seatBooked': sb,
            'joined': joined, 'revenue': revenue,
            'expenses': 0, 'surplus': revenue,
        })

    return sorted(records, key=lambda r: r['date'])


def parse_webinar_file(file_obj):
    """
    Parse Free_Class_Lead_Report.xlsx.
    Returns (bcmb_records, insg_records) — lists of dicts.
    """
    xl = pd.ExcelFile(file_obj)
    bcmb_sheet = _pick_sheet(xl, 'bcmb')
    insg_sheet = _pick_sheet(xl, 'insg') or _pick_sheet(xl, 'insignia')
    return _parse_bcmb(xl, bcmb_sheet), _parse_insg(xl, insg_sheet)


# ──────────────────────────────────────────────────────────────────────────────
# FILE 2  —  Offline_Seminar_Report  →  seminar rows
# ──────────────────────────────────────────────────────────────────────────────

def parse_seminar_file(file_obj):
    """
    Parse Offline_Seminar_Report.xlsx → "Offline Report" sheet.
    Returns list of seminar dicts.
    """
    xl  = pd.ExcelFile(file_obj)
    df  = xl.parse('Offline Report', header=1)
    df  = df[pd.to_numeric(df['Sr No'], errors='coerce').notna()].copy()

    def n(col):
        return pd.to_numeric(df[col], errors='coerce').fillna(0)

    dates   = pd.to_datetime(df['Seminar Date'], errors='coerce')
    records = []
    for i in range(len(df)):
        d = dates.iloc[i]
        if pd.isna(d):
            continue
        exp  = float(n('Actual Expenses').iloc[i])
        arev = float(n('Actual Revenue(W/O GST)\nAttendees').iloc[i])
        trev = float(n('Total Revenue\n(W/O GST)\nAttendees').iloc[i])
        surp = float(n('Surplus or Deficit').iloc[i])
        erev = float(n('Expected Revenue').iloc[i])
        roi  = float(n('Surplus to Expense').iloc[i]) * 100 if exp > 0 else 0

        records.append({
            'date':             str(d.date()),
            'month':            d.strftime('%Y-%m'),
            'location':         _s(df['Location'].iloc[i]).upper(),
            'trainer':          _norm_trainer(_s(df['Trainer'].iloc[i]).replace('\n', ', ')),
            'targeted':         int(n('Targeted\n').iloc[i]),
            'attended':         int(n('Total\nAttended').iloc[i]),
            'sb_seminar':       int(n('Total\nSeat\nBooked\n(in Seminar)').iloc[i]),
            'sb_morning':       int(n('Morning').iloc[i]),
            'sb_evening':       int(n('Evening').iloc[i]),
            'sb_non_webinar':   int(n('Non\nWebinar').iloc[i]),
            'expenses':         round(exp,  2),
            'expected_revenue': round(erev, 2),
            'actual_revenue':   round(arev, 2),
            'total_revenue':    round(trev, 2),
            'surplus':          round(surp, 2),
            'att_rate':         round(float(n('Targeted to Attended (%)').iloc[i]) * 100, 1),
            'sb_rate':          round(float(n('Attended to Seat Booked (%)').iloc[i]) * 100, 1),
            'roi':              round(roi, 1),
        })

    return sorted(records, key=lambda r: r['date'])


# ──────────────────────────────────────────────────────────────────────────────
# FILE 3  —  Offline_Indepth_Details  →  student / attendee data
# ──────────────────────────────────────────────────────────────────────────────

LOC_MAP = {
    'bankura': 'BANKURA', 'bongaon': 'BONGAON', 'arambagh': 'ARAMBAGH',
    'dh': 'DIAMOND HARBOUR', 'bishnupur': 'BISHNUPUR', 'bagnan': 'BAGNAN',
    'midnapore': 'MIDNAPORE', 'midnapure': 'MIDNAPORE',
    'chandannagar': 'CHANDANNAGAR', 'chandag': 'CHANDANNAGAR',
    'beharampur': 'BEHARAMPUR', 'behrampur': 'BEHARAMPUR',
    'krishnanagar': 'KRISHNANAGAR', 'malda': 'MALDA', 'raiganj': 'RAIGANJ',
    'purulia': 'PURULIA', 'burdwan': 'BURDWAN', 'bandel': 'BANDEL',
    'durgapur': 'DURGAPUR', 'cooachbehar': 'COOCHBEHAR', 'siliguri': 'SILIGURI',
    'silliguri': 'SILIGURI', 'basirhat': 'BASIRHAT', 'bolpur': 'BOLPUR',
    'kakdwip': 'KAKDWIP', 'balurghat': 'BALURGHAT', 'ghatal': 'GHATAL',
    'bankura_7_12': 'BANKURA', 'chakdha': 'CHAKDHA', 'jhargram': 'JHARGRAM',
    'katwa': 'KATWA', 'kathi': 'KANTHI', 'bongaon_28_12': 'BONGAON',
    'chandrakona': 'CHANDRAKONA', 'bethuadahari': 'BETHUADAHARI',
    'haldia': 'HALDIA', 'sonarpur': 'SONARPUR', 'ambika kalna': 'AMBIKA KALNA',
    'alipurduar': 'ALIPURDUAR', 'arambagh_1stfeb': 'ARAMBAGH',
    'asansol': 'ASANSOL', 'jalpaiguri': 'JALPAIGURI',
    'rampurhat': 'RAMPURHAT', 'adra': 'ADRA', 'nabadwip': 'NABADWIP',
    'coochbehar_re': 'COOCHBEHAR', 'silliguri_re': 'SILIGURI',
    'basirhat_re': 'BASIRHAT', 'bolpur_re': 'BOLPUR',
    'kakdwip_re': 'KAKDWIP', 'durgapur_re': 'DURGAPUR',
    'bandel_re': 'BANDEL', 'burdwan_re': 'BURDWAN',
    'purulia_re': 'PURULIA', 'raiganj_re': 'RAIGANJ', 'malda_re': 'MALDA',
    'krishnanagar_re': 'KRISHNANAGAR', 'behrampur_re': 'BEHARAMPUR',
    'chandanagar_re': 'CHANDANNAGAR', 'midnapure_re': 'MIDNAPORE',
    'bagnan_re': 'BAGNAN', 'bishnupur_re': 'BISHNUPUR',
    'dh_re': 'DIAMOND HARBOUR', 'arambag_re': 'ARAMBAGH',
    'bongaon_re': 'BONGAON', 'midnapur': 'MIDNAPORE',
}

_ATT_SKIP = {
    'conversion 2', 'con', 'sheet1', 'summary', 'count', 'marking', 'line graph',
    'simplify bcmb', 'simplify_hitting', 'simplify insignia',
    'sales month wise summary', 'bcmb_webinar hitting report',
    'insignia webinar hitting report', 'offline seminar hitting report',
    'retargeted webinar hitting repo', 'webinar monitoring log',
    'lead wise log bcmb', 'sunday new lead percentage',
    'tuesday new lead percentage', 'friday new lead percentage',
    'day to day bcmb joining report', 'joining percentage',
    'bcmb log re-target', 'bcmb re-target', 'bcmb backup', 'invesmeet',
    'comparison', 'day to day bcmb ', 'forx', 'fund', 'hindi', 'rough sheet',
    '8_45', 'bcmb log', 'bcmb call analysis', 'insg', 'insignia log', 'bcmb',
    'offline', 'insglog', 'bcmblog',
}


def _classify_course(svc_name, svc_code):
    sn = str(svc_name).lower()
    sc = str(svc_code).lower()
    if 'insignia' in sn or 'global capital' in sn or 'ins10' in sc:
        return 'INSIGNIA'
    if 'equity' in sn and 'strategy' in sn:
        return 'Equity Strategy'
    if 'f&o' in sn or ('future' in sn and 'option' in sn):
        return 'F&O'
    if 'intraday' in sn or 'swing' in sn:
        return 'Intraday/Swing'
    if 'commodity' in sn:
        return 'Commodity'
    return 'BCMB'


def parse_attendee_file(file_obj):
    """
    Parse Offline_Indepth_Details.xlsx (multi-sheet).
    Returns (summary_dict, course_type_dict, sales_rep_dict, location_dict).
    """
    xl       = pd.ExcelFile(file_obj)
    all_rows = []

    for sheet in xl.sheet_names:
        if sheet.lower() in _ATT_SKIP:
            continue
        try:
            df = xl.parse(sheet)
            if df.empty:
                continue
            df.columns = [str(c).strip().lower().replace(' ', '_') for c in df.columns]
            if 'student_name' not in df.columns and 'student_invid' not in df.columns:
                continue

            key      = re.sub(r'\s+', '_', sheet.lower()).strip('_')
            location = LOC_MAP.get(sheet.lower(), LOC_MAP.get(key, sheet.upper()))

            for col in ['payment_received', 'total_amount', 'total_due',
                        'total_additional_charges', 'total_gst']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            df['status']      = (df['status'].fillna('Unknown').astype(str).str.strip()
                                 if 'status' in df.columns else 'Unknown')
            df['location']    = location
            df['course_type'] = df.apply(
                lambda r: _classify_course(r.get('service_name', ''),
                                           r.get('service_code', '')), axis=1)
            all_rows.append(df)
        except Exception:
            continue

    if not all_rows:
        return {}, {}, {}, {}

    att = pd.concat(all_rows, ignore_index=True)

    pr  = att['payment_received'] if 'payment_received' in att.columns else pd.Series(dtype=float)
    ta  = att['total_amount']     if 'total_amount'     in att.columns else pd.Series(dtype=float)
    td  = att['total_due']        if 'total_due'        in att.columns else pd.Series(dtype=float)
    sid = att['student_invid']    if 'student_invid'    in att.columns else pd.RangeIndex(len(att))

    def ss(s): return round(float(s.sum()), 2) if len(s) else 0
    def sm(s): p = s[s > 0]; return round(float(p.mean()), 2) if len(p) else 0

    summary = {
        'total_students':    int(sid.nunique()),
        'total_records':     int(len(att)),
        'total_revenue':     ss(pr),
        'total_amount':      ss(ta),
        'total_due':         ss(td),
        'active_students':   int((att['status'] == 'Active').sum()),
        'inactive_students': int((att['status'] == 'Inactive').sum()),
        'closed_students':   int((att['status'] == 'Closed').sum()),
        'avg_payment':       sm(pr),
    }

    ct_stats = {}
    for ct, g in att.groupby('course_type'):
        gpr = g['payment_received'] if 'payment_received' in g.columns else pd.Series(dtype=float)
        gta = g['total_amount']     if 'total_amount'     in g.columns else pd.Series(dtype=float)
        gtd = g['total_due']        if 'total_due'        in g.columns else pd.Series(dtype=float)
        ct_stats[ct] = {
            'count':      int(len(g)),
            'revenue':    ss(gpr),
            'active':     int((g['status'] == 'Active').sum()),
            'avg_amount': round(float(gta.mean()), 2) if len(gta) else 0,
            'total_due':  ss(gtd),
        }

    sr_stats = {}
    srn_col = next((c for c in att.columns if 'sales_rep' in c), None)
    if srn_col:
        for rep, g in att[att[srn_col].notna()].groupby(srn_col):
            r = str(rep).strip()
            if not r or r == 'nan':
                continue
            gpr = g['payment_received'] if 'payment_received' in g.columns else pd.Series(dtype=float)
            sr_stats[r] = {
                'deals':    int(len(g)),
                'revenue':  ss(gpr),
                'active':   int((g['status'] == 'Active').sum()),
                'avg_deal': sm(gpr),
            }
    sr_stats = dict(sorted(sr_stats.items(), key=lambda x: -x[1]['revenue'])[:25])

    loc_stats = {}
    for loc, g in att.groupby('location'):
        gpr = g['payment_received'] if 'payment_received' in g.columns else pd.Series(dtype=float)
        gtd = g['total_due']        if 'total_due'        in g.columns else pd.Series(dtype=float)
        gid = g['student_invid']    if 'student_invid'    in g.columns else pd.RangeIndex(len(g))
        loc_stats[loc] = {
            'students':  int(gid.nunique()),
            'revenue':   ss(gpr),
            'active':    int((g['status'] == 'Active').sum()),
            'total_due': ss(gtd),
        }
    loc_stats = dict(sorted(loc_stats.items(), key=lambda x: -x[1]['revenue'])[:40])

    return summary, ct_stats, sr_stats, loc_stats


# ──────────────────────────────────────────────────────────────────────────────
# FILE 4  —  Seminar Updated Files  →  updated seminar data
# ──────────────────────────────────────────────────────────────────────────────

def parse_seminar_updated_file(file_obj):
    """
    Parse Seminar Updated Files.xlsx.
    Returns list of updated seminar records with any additional fields available.
    Falls back gracefully if sheet structure differs.
    """
    if file_obj is None:
        return []

    try:
        xl = pd.ExcelFile(file_obj)
    except Exception:
        return []

    # Try known sheet names first, then fall back to first sheet
    target_sheet = None
    for candidate in ['Offline Report', 'Sheet1', 'Data', 'Seminar']:
        if candidate in xl.sheet_names:
            target_sheet = candidate
            break
    if not target_sheet and xl.sheet_names:
        target_sheet = xl.sheet_names[0]
    if not target_sheet:
        return []

    try:
        # Try header=1 first (matches original seminar format), then header=0
        for hdr in [1, 0]:
            try:
                df = xl.parse(target_sheet, header=hdr)
                df.columns = [str(c).strip() for c in df.columns]
                # Check we have useful numeric data
                if len(df.columns) >= 3 and len(df) >= 1:
                    break
            except Exception:
                continue

        records = []
        # Flexible column detection
        c_date  = _col(df, 'date', 'seminar date')
        c_loc   = _col(df, 'location', 'city', 'venue')
        c_tgt   = _col(df, 'targeted', 'target')
        c_att   = _col(df, 'attended', 'attendance', 'total attended')
        c_sb    = _col(df, 'seat booked', 'booked')
        c_exp   = _col(df, 'expenses', 'expense', 'actual expenses')
        c_rev   = _col(df, 'revenue', 'actual revenue')
        c_surp  = _col(df, 'surplus', 'deficit')
        c_train = _col(df, 'trainer')

        for _, row in df.iterrows():
            date_val = _d(row.get(c_date, '')) if c_date else ''
            if not date_val:
                continue

            rec = {
                'date':           date_val,
                'month':          date_val[:7],
                'location':       _s(row.get(c_loc, '')).upper()   if c_loc   else '',
                'trainer':        _norm_trainer(_s(row.get(c_train, ''))) if c_train else '',
                'targeted':       int(_n(row.get(c_tgt,  0)))       if c_tgt   else 0,
                'attended':       int(_n(row.get(c_att,  0)))       if c_att   else 0,
                'sb_seminar':     int(_n(row.get(c_sb,   0)))       if c_sb    else 0,
                'expenses':       round(_n(row.get(c_exp,  0)), 2)  if c_exp   else 0,
                'actual_revenue': round(_n(row.get(c_rev,  0)), 2)  if c_rev   else 0,
                'surplus':        round(_n(row.get(c_surp, 0)), 2)  if c_surp  else 0,
                'source':         'updated',
            }
            records.append(rec)

        return sorted(records, key=lambda r: r['date'])

    except Exception:
        return []


# ──────────────────────────────────────────────────────────────────────────────
# FILE 5  —  Conversion List  →  lead-to-sale conversion tracking
# ──────────────────────────────────────────────────────────────────────────────

def parse_conversion_file(file_obj):
    """
    Parse Conversion List.xlsx.
    Returns list of conversion records.
    """
    if file_obj is None:
        return []

    try:
        xl = pd.ExcelFile(file_obj)
    except Exception:
        return []

    all_records = []

    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, header=0)
            if df.empty or len(df.columns) < 2:
                continue
            df.columns = [str(c).strip() for c in df.columns]

            # Flexible column detection
            c_name    = _col(df, 'name', 'student', 'lead')
            c_date    = _col(df, 'date', 'conversion date', 'sale date')
            c_course  = _col(df, 'course', 'product', 'service')
            c_amount  = _col(df, 'amount', 'revenue', 'payment', 'fee')
            c_status  = _col(df, 'status', 'stage')
            c_rep     = _col(df, 'sales rep', 'rep', 'assigned', 'counsellor')
            c_source  = _col(df, 'source', 'lead source', 'channel')
            c_loc     = _col(df, 'location', 'city', 'centre')
            c_phone   = _col(df, 'phone', 'mobile', 'contact')

            for _, row in df.iterrows():
                name = _s(row.get(c_name, '')) if c_name else ''
                if not name or name.lower() in ('nan', 'name', 'student name'):
                    continue

                date_val = _d(row.get(c_date, '')) if c_date else ''
                amount   = round(_n(row.get(c_amount, 0)), 2) if c_amount else 0

                rec = {
                    'name':     name,
                    'date':     date_val,
                    'month':    date_val[:7] if date_val else '',
                    'course':   _s(row.get(c_course, ''))   if c_course else '',
                    'amount':   amount,
                    'status':   _s(row.get(c_status, ''))   if c_status else '',
                    'rep':      _s(row.get(c_rep,    ''))   if c_rep    else '',
                    'source':   _s(row.get(c_source, ''))   if c_source else '',
                    'location': _s(row.get(c_loc,    '')).upper() if c_loc else '',
                    'phone':    _s(row.get(c_phone,  ''))   if c_phone  else '',
                    'sheet':    sheet,
                }
                all_records.append(rec)

        except Exception:
            continue

    return sorted(all_records, key=lambda r: r.get('date') or '')


def _summarise_conversions(records: list) -> dict:
    """Aggregate conversion records into dashboard-ready stats."""
    if not records:
        return {
            'total': 0, 'total_revenue': 0,
            'by_month': {}, 'by_course': {}, 'by_rep': {}, 'by_source': {},
        }

    total_rev = round(sum(r.get('amount', 0) for r in records), 2)

    by_month = {}
    for r in records:
        m = r.get('month') or 'Unknown'
        by_month.setdefault(m, {'count': 0, 'revenue': 0})
        by_month[m]['count']   += 1
        by_month[m]['revenue'] += r.get('amount', 0)

    by_course = {}
    for r in records:
        c = r.get('course') or 'Unknown'
        by_course.setdefault(c, {'count': 0, 'revenue': 0})
        by_course[c]['count']   += 1
        by_course[c]['revenue'] += r.get('amount', 0)

    by_rep = {}
    for r in records:
        rep = r.get('rep') or 'Unknown'
        by_rep.setdefault(rep, {'count': 0, 'revenue': 0})
        by_rep[rep]['count']   += 1
        by_rep[rep]['revenue'] += r.get('amount', 0)
    by_rep = dict(sorted(by_rep.items(), key=lambda x: -x[1]['revenue'])[:20])

    by_source = {}
    for r in records:
        src = r.get('source') or 'Unknown'
        by_source.setdefault(src, {'count': 0, 'revenue': 0})
        by_source[src]['count']   += 1
        by_source[src]['revenue'] += r.get('amount', 0)

    return {
        'total':         len(records),
        'total_revenue': total_rev,
        'by_month':      dict(sorted(by_month.items())),
        'by_course':     by_course,
        'by_rep':        by_rep,
        'by_source':     by_source,
    }


# ──────────────────────────────────────────────────────────────────────────────
# FILE 6  —  Leads  →  raw leads pipeline data
# ──────────────────────────────────────────────────────────────────────────────

def parse_leads_file(file_obj):
    """
    Parse Leads.xlsx.
    Returns list of lead records.
    """
    if file_obj is None:
        return []

    try:
        xl = pd.ExcelFile(file_obj)
    except Exception:
        return []

    all_leads = []

    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, header=0)
            if df.empty or len(df.columns) < 2:
                continue
            df.columns = [str(c).strip() for c in df.columns]

            c_name    = _col(df, 'name', 'lead name', 'student')
            c_date    = _col(df, 'date', 'lead date', 'created')
            c_source  = _col(df, 'source', 'lead source', 'channel', 'medium')
            c_status  = _col(df, 'status', 'stage', 'disposition')
            c_course  = _col(df, 'course', 'product', 'interested in', 'interest')
            c_rep     = _col(df, 'assigned', 'rep', 'counsellor', 'sales rep')
            c_phone   = _col(df, 'phone', 'mobile', 'contact')
            c_loc     = _col(df, 'location', 'city', 'area')

            for _, row in df.iterrows():
                name = _s(row.get(c_name, '')) if c_name else ''
                if not name or name.lower() in ('nan', 'name', 'lead name'):
                    continue

                date_val = _d(row.get(c_date, '')) if c_date else ''

                lead = {
                    'name':     name,
                    'date':     date_val,
                    'month':    date_val[:7] if date_val else '',
                    'source':   _s(row.get(c_source, '')) if c_source else '',
                    'status':   _s(row.get(c_status, '')) if c_status else '',
                    'course':   _s(row.get(c_course, '')) if c_course else '',
                    'rep':      _s(row.get(c_rep,    '')) if c_rep    else '',
                    'phone':    _s(row.get(c_phone,  '')) if c_phone  else '',
                    'location': _s(row.get(c_loc,    '')).upper() if c_loc else '',
                    'sheet':    sheet,
                }
                all_leads.append(lead)

        except Exception:
            continue

    return sorted(all_leads, key=lambda r: r.get('date') or '')


def _summarise_leads(records: list) -> dict:
    """Aggregate lead records into dashboard-ready stats."""
    if not records:
        return {
            'total': 0,
            'by_month': {}, 'by_source': {}, 'by_status': {}, 'by_course': {},
        }

    by_month = {}
    for r in records:
        m = r.get('month') or 'Unknown'
        by_month.setdefault(m, 0)
        by_month[m] += 1

    by_source = {}
    for r in records:
        s = r.get('source') or 'Unknown'
        by_source.setdefault(s, 0)
        by_source[s] += 1
    by_source = dict(sorted(by_source.items(), key=lambda x: -x[1])[:20])

    by_status = {}
    for r in records:
        s = r.get('status') or 'Unknown'
        by_status.setdefault(s, 0)
        by_status[s] += 1

    by_course = {}
    for r in records:
        c = r.get('course') or 'Unknown'
        by_course.setdefault(c, 0)
        by_course[c] += 1

    return {
        'total':     len(records),
        'by_month':  dict(sorted(by_month.items())),
        'by_source': by_source,
        'by_status': by_status,
        'by_course': by_course,
    }


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def process_all(webinar_file, seminar_file, attendee_file,
                seminar_updated_file=None, conversion_file=None, leads_file=None):
    """
    Parse all files and return a single dict with everything
    the dashboard templates need.

    Required:
        webinar_file, seminar_file, attendee_file

    Optional (new):
        seminar_updated_file, conversion_file, leads_file

    Returns:
        {
          'bcmb':              list[dict],
          'insg':              list[dict],
          'offline':           list[dict],
          'seminar':           list[dict],
          'seminar_updated':   list[dict],
          'att_summary':       dict,
          'ct_stats':          dict,
          'sr_stats':          dict,
          'loc_stats':         dict,
          'conversion_stats':  dict,
          'leads_stats':       dict,
          'errors':            list[str],
          'stats':             dict,
        }
    """
    errors = []

    try:
        bcmb, insg = parse_webinar_file(webinar_file)
    except Exception as e:
        errors.append(f'Webinar file: {e}')
        bcmb, insg = [], []

    try:
        seminar = parse_seminar_file(seminar_file)
    except Exception as e:
        errors.append(f'Seminar file: {e}')
        seminar = []

    try:
        att_summary, ct_stats, sr_stats, loc_stats = parse_attendee_file(attendee_file)
    except Exception as e:
        errors.append(f'Attendee file: {e}')
        att_summary = ct_stats = sr_stats = loc_stats = {}

    # Optional new files — fail gracefully
    try:
        seminar_updated = parse_seminar_updated_file(seminar_updated_file)
    except Exception as e:
        errors.append(f'Seminar Updated file: {e}')
        seminar_updated = []

    try:
        conversion_records = parse_conversion_file(conversion_file)
        conversion_stats   = _summarise_conversions(conversion_records)
    except Exception as e:
        errors.append(f'Conversion file: {e}')
        conversion_stats = _summarise_conversions([])

    try:
        leads_records = parse_leads_file(leads_file)
        leads_stats   = _summarise_leads(leads_records)
    except Exception as e:
        errors.append(f'Leads file: {e}')
        leads_stats = _summarise_leads([])

    # OFFLINE_DATA: bridge format that sits inside ALL_DATA for filter compat
    offline_rows = [{
        'date':       s['date'],    'yearMonth':  s['month'],
        'trainer':    s['trainer'], 'location':   s['location'],
        'course':     'OFFLINE',    'type':       'Offline',  'mode': 'Offline',
        'targeted':   s['targeted'], 'registered': s['attended'],
        'over30':     s['attended'], 'seatBooked': s['sb_seminar'],
        'joined':     s['sb_seminar'],
        'revenue':    s['actual_revenue'],
        'expenses':   s['expenses'],
        'surplus':    s['surplus'],
    } for s in seminar]

    return {
        'bcmb':             bcmb,
        'insg':             insg,
        'offline':          offline_rows,
        'seminar':          seminar,
        'seminar_updated':  seminar_updated,
        'att_summary':      att_summary,
        'ct_stats':         ct_stats,
        'sr_stats':         sr_stats,
        'loc_stats':        loc_stats,
        'conversion_stats': conversion_stats,
        'leads_stats':      leads_stats,
        'errors':           errors,
        'stats': {
            'bcmb_count':         len(bcmb),
            'insg_count':         len(insg),
            'seminar_count':      len(seminar),
            'seminar_upd_count':  len(seminar_updated),
            'locations':          len(set(s['location'] for s in seminar)),
            'students':           att_summary.get('total_students', 0),
            'conversions':        conversion_stats.get('total', 0),
            'leads':              leads_stats.get('total', 0),
        },
    }
