"""
data_processor.py  —  Invesmate Analytics Dashboard
Parses all files and returns JSON-serialisable dicts for HTML templates.

Offline section requires 3 files:
  1. Seminar Updated Sheet  — attendance, seat-book, student info
  2. Conversion List        — orders, payments, courses purchased
  3. Leads Report           — lead source, campaign, stage, owner, etc.

Online section requires:
  4. Free_Class_Lead_Report.xlsx — BCMB + INSG sheets
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime

# ──────────────────────────────────────────────────────────────────────────────
# LOW-LEVEL HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def _n(val):
    try:
        v = float(val)
        return 0 if (np.isnan(v) or np.isinf(v)) else v
    except Exception:
        return 0

def _s(val):
    if val is None:
        return ''
    try:
        if isinstance(val, float) and np.isnan(val):
            return ''
    except Exception:
        pass
    return str(val).strip()

def _d(val):
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
    excl = [e.lower() for e in (exclude or [])]
    cols = [str(c) for c in df.columns]
    for kw in keywords:
        kw_l = kw.lower()
        for c in cols:
            c_l = c.lower()
            if any(e in c_l for e in excl):
                continue
            if (exact and c_l == kw_l) or (not exact and kw_l in c_l):
                return c
    return None

def safe_numeric(series):
    return pd.to_numeric(series, errors='coerce').fillna(0)

def clean_mobile(x):
    if pd.isna(x):
        return None
    s = re.sub(r'\D', '', str(x))
    return s[-10:] if len(s) >= 10 else None

def parse_date_series(series):
    for fmt in ['%d-%b-%Y','%d-%b-%y','%d/%m/%Y','%Y-%m-%d','%d-%m-%Y','%b-%d-%Y','%d %b %Y']:
        try:
            parsed = pd.to_datetime(series, format=fmt, errors='coerce')
            if parsed.notna().any():
                return parsed
        except Exception:
            pass
    return pd.to_datetime(series, errors='coerce', dayfirst=True)

def normalize_status(status):
    if not status:
        return ''
    s = str(status).strip().lower()
    if s in ['paid','completed','success','active','converted']:
        return 'Active'
    if s in ['partial','partially paid','in progress']:
        return 'Partially Converted'
    if s in ['failed','cancelled','canceled','inactive','pending']:
        return 'Inactive'
    return str(status).strip()

COMBO_MATCH = 'Power Of Trading & Investing Combo Course'

# ──────────────────────────────────────────────────────────────────────────────
# FILE LOADER
# ──────────────────────────────────────────────────────────────────────────────

def _load_file(file_obj, name=''):
    name = (name or '').lower()
    try:
        if name.endswith('.csv'):
            try:
                return pd.read_csv(file_obj)
            except Exception:
                file_obj.seek(0)
                return pd.read_csv(file_obj, encoding='latin1')
        if name.endswith(('.xlsx','.xls')):
            return pd.read_excel(file_obj, sheet_name=0)
        try:
            return pd.read_excel(file_obj, sheet_name=0)
        except Exception:
            try: file_obj.seek(0)
            except Exception: pass
            try:
                return pd.read_csv(file_obj)
            except Exception:
                file_obj.seek(0)
                return pd.read_csv(file_obj, encoding='latin1')
    except Exception as e:
        raise ValueError(f'Error reading file ({name}): {e}') from e

def _detect(df, *candidates):
    norm = {c.strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in norm:
            return norm[key]
    return None

# ──────────────────────────────────────────────────────────────────────────────
# TRAINER NORMALISATION (online files)
# ──────────────────────────────────────────────────────────────────────────────

TRAINER_MAP = {
    'rohitava majumdar':'Rohitava Majumder','rohitav majumder':'Rohitava Majumder',
    'rohitava majumder**':'Rohitava Majumder','debargha  saha':'Debargho Saha',
    'debargha saha':'Debargho Saha','debargho\u00a0saha':'Debargho Saha',
    'pratim kumer chakraborty':'Pratim Kumar Chakraborty',
    'hironmoy laheri':'Hironmoy Lahiri','hironmoy lahiri\u00a0':'Hironmoy Lahiri',
    'sandipan das':'Sandipan Kumar Das',
    'kunal saha (special advanced class)':'Kunal Saha',
    'sayan sarker(special advanced class)':'Sayan Sarker',
}

def _norm_trainer(raw):
    parts = [p.strip() for p in re.split(r',|&|\n', str(raw)) if p.strip()]
    out = []
    for p in parts:
        p = re.sub(r'\s*\(Special Advanced Class\)\s*','',p,flags=re.I).strip()
        p = re.sub(r'\s+',' ',p)
        out.append(TRAINER_MAP.get(p.lower(),p))
    return ', '.join(dict.fromkeys(out))

# ──────────────────────────────────────────────────────────────────────────────
# ONLINE FILE — BCMB + INSIGNIA
# ──────────────────────────────────────────────────────────────────────────────

_SHEET_SKIP = {
    'log','hitting','call','re-target','retarget','backup','rough','comparison',
    'summary','offline','forx','fund','hindi','invesmeet','simplify','monitoring',
    'lead wise','joining','percentage','day to day','sheet1','8_45','sunday','tuesday','friday',
}

def _pick_sheet(xl, keyword):
    candidates = [s for s in xl.sheet_names
                  if keyword in s.lower() and not any(sk in s.lower() for sk in _SHEET_SKIP)]
    candidates.sort(key=len)
    return candidates[0] if candidates else None

def _parse_bcmb(xl, sheet_name):
    if not sheet_name: return []
    df = xl.parse(sheet_name, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    c_trainer = _col(df,'trainer',exact=True) or _col(df,'trainer',exclude=['re-target','retarget'])
    c_type    = _col(df,'type',exact=True) or _col(df,'location',exact=True)
    c_date    = _col(df,'date',exact=True) or _col(df,'date',exclude=['web','hitting','batch'])
    c_tgt     = _col(df,'targeted',exact=True) or _col(df,'targeted',exclude=['to','%','re-','retarget','dialed','visited','regist','over','seat','new','old'])
    c_reg     = _col(df,'registered',exact=True) or _col(df,'registered',exclude=['%','to'])
    c_o30     = _col(df,'over 30 min',exact=True) or _col(df,'over 30',exclude=['%','to'])
    c_sb      = _col(df,'seat booked',exact=True) or _col(df,'seat booked',exclude=['%','to','amount'])
    c_join    = _col(df,'total joined',exact=True) or _col(df,'joined',exclude=['%','re-','new','old','semi'])
    c_rev     = _col(df,'seat booking amount') or _col(df,'course amount')
    records = []
    for _, row in df.iterrows():
        dv = _d(row.get(c_date,'')) if c_date else ''
        tg = int(_n(row.get(c_tgt,0))) if c_tgt else 0
        if not dv or tg < 1: continue
        tr = _norm_trainer(_s(row.get(c_trainer,'Unknown')) if c_trainer else 'Unknown')
        ty = _s(row.get(c_type,'Live')) if c_type else 'Live'
        rg = int(_n(row.get(c_reg,0))) if c_reg else 0
        o3 = int(_n(row.get(c_o30,0))) if c_o30 else 0
        sb = int(_n(row.get(c_sb,0)))  if c_sb  else 0
        jn = int(_n(row.get(c_join,0)))if c_join else 0
        rv = int(_n(row.get(c_rev,0))) if c_rev  else 0
        t  = ty.upper()
        wt = ('Rec' if 'REC' in t else 'Backup' if 'BACKUP' in t or 'BACK' in t
              else 'Practice' if 'PRACTICE' in t else 'Cancel' if 'CANCEL' in t
              else 'Live\n(ZOOM)' if 'ZOOM' in t else 'Live')
        if rv == 0 and sb > 0: rv = sb * 5632
        records.append({'date':dv,'yearMonth':dv[:7],'trainer':tr,'course':'BCMB','type':wt,
                        'mode':'Online','targeted':tg,'registered':rg,'over30':o3,
                        'seatBooked':sb,'joined':jn,'revenue':rv,'expenses':0,'surplus':rv})
    return sorted(records, key=lambda r: r['date'])

def _parse_insg(xl, sheet_name):
    if not sheet_name: return []
    df = xl.parse(sheet_name, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    c_trainer = _col(df,'trainer',exact=True)
    c_type    = _col(df,'type',exact=True)
    c_date    = _col(df,'date',exact=True) or _col(df,'date',exclude=['web','hitting','hidden','batch'])
    c_tgt     = _col(df,'targated',exact=True) or _col(df,'targeted',exact=True) or _col(df,'targated',exclude=['%','to'])
    c_reg     = _col(df,'registered',exact=True) or _col(df,'registered',exclude=['%','to'])
    c_o30     = _col(df,'over 30 min',exact=True) or _col(df,'over 30',exclude=['%','to'])
    c_sb      = _col(df,'seat booked',exact=True) or _col(df,'seat booked',exclude=['%','to'])
    c_join    = _col(df,'unique viewer') or _col(df,'total joined') or _col(df,'joined',exclude=['%'])
    records = []
    for _, row in df.iterrows():
        dv = _d(row.get(c_date,'')) if c_date else ''
        tg = int(_n(row.get(c_tgt,0))) if c_tgt else 0
        if not dv or tg < 1: continue
        tr = _norm_trainer(_s(row.get(c_trainer,'Unknown')) if c_trainer else 'Unknown')
        ty = _s(row.get(c_type,'Live')) if c_type else 'Live'
        sb = int(_n(row.get(c_sb,0))) if c_sb else 0
        records.append({'date':dv,'yearMonth':dv[:7],'trainer':tr,'course':'INSIGNIA',
                        'type':'Rec' if 'REC' in ty.upper() else 'Live','mode':'Online',
                        'targeted':tg,'registered':int(_n(row.get(c_reg,0))) if c_reg else 0,
                        'over30':int(_n(row.get(c_o30,0))) if c_o30 else 0,
                        'seatBooked':sb,'joined':int(_n(row.get(c_join,0))) if c_join else 0,
                        'revenue':sb*8999,'expenses':0,'surplus':sb*8999})
    return sorted(records, key=lambda r: r['date'])

def parse_webinar_file(file_obj):
    xl = pd.ExcelFile(file_obj)
    return (_parse_bcmb(xl, _pick_sheet(xl,'bcmb')),
            _parse_insg(xl, _pick_sheet(xl,'insg') or _pick_sheet(xl,'insignia')))

# ──────────────────────────────────────────────────────────────────────────────
# OFFLINE FILES — Student matching (mirrors app.py logic exactly)
# ──────────────────────────────────────────────────────────────────────────────

def parse_offline_files(seminar_updated_file, conversion_file, leads_file,
                        sem_name='', conv_name='', leads_name=''):
    """
    Parse Seminar Updated Sheet + Conversion List + Leads Report.
    Returns (students, orders, seminar_meta, agg)
    """
    if seminar_updated_file is None or conversion_file is None:
        return [], [], [], {}

    # ── Seminar sheet ──────────────────────────────────────────────────────
    try:
        sem = _load_file(seminar_updated_file, sem_name)
        sem.columns = [str(c).strip() for c in sem.columns]
    except Exception as e:
        return [], [], [], {'error': f'Seminar file: {e}'}

    c_mobile   = _detect(sem,'Mobile','Phone','mobile','phone','Contact')
    c_altmob   = _detect(sem,'Alternate Number','Alt Mobile','alternate_number','Alternate Mobile','Alternative Mobile')
    c_name     = _detect(sem,'NAME','Name','Student Name','name')
    c_place    = _detect(sem,'Place','Location','Venue','City','place')
    c_trainer  = _detect(sem,'Trainer / Presenter','Trainer','Presenter','trainer')
    c_semdate  = _detect(sem,'Seminar Date','Date','seminar_date','Event Date')
    c_session  = _detect(sem,'Session','session','Batch','Time')
    c_attended = _detect(sem,'Is Attended ?','Is Attended?','Attended','attended','is_attended','IsAttended','ATTENDED','Attendance','Present','attend')
    c_amount   = _detect(sem,'Amount Paid','Seat Amount','seat_amount','amount_paid','SeatAmount')

    sem['mobile_clean']     = sem[c_mobile].apply(clean_mobile) if c_mobile else None
    sem['alt_mobile_clean'] = sem[c_altmob].apply(clean_mobile) if c_altmob else None
    sem['seminar_date']     = parse_date_series(sem[c_semdate]) if c_semdate else pd.NaT
    sem['seat_book_amount'] = safe_numeric(sem[c_amount]) if c_amount else 0
    sem['attended_flag']    = (sem[c_attended].astype(str).str.strip().str.upper()
                                .isin(['YES','TRUE','1','Y'])) if c_attended else False

    attendees = sem[
        ((sem['attended_flag']) | (sem['seat_book_amount'] > 0)) &
        ((sem['mobile_clean'].notna()) | (sem['alt_mobile_clean'].notna()))
    ].copy().reset_index(drop=True)

    # ── Conversion list ────────────────────────────────────────────────────
    try:
        conv = _load_file(conversion_file, conv_name)
        conv.columns = [str(c).strip() for c in conv.columns]
    except Exception as e:
        return [], [], [], {'error': f'Conversion file: {e}'}

    cc_mobile   = _detect(conv,'phone','Phone','mobile','Mobile','Contact')
    cc_service  = _detect(conv,'service_name','Service Name','Course','course_name','ServiceName')
    cc_orderdt  = _detect(conv,'order_date','Order Date','OrderDate','Date')
    cc_payrec   = _detect(conv,'payment_received','Payment Received','PaymentReceived','amount_paid')
    cc_gst      = _detect(conv,'total_gst','GST','gst','TotalGST')
    cc_due      = _detect(conv,'total_due','Due','total_due_amount','TotalDue')
    cc_trainer  = _detect(conv,'trainer','Trainer')
    cc_salesrep = _detect(conv,'sales_rep_name','Sales Rep','SalesRep','sales_rep')
    cc_mode     = _detect(conv,'payment_mode','Payment Mode','mode')
    cc_status   = _detect(conv,'status','Status')
    cc_orderid  = _detect(conv,'orderID','Order ID','order_id','OrderId')

    conv['mobile_clean']       = conv[cc_mobile].apply(clean_mobile) if cc_mobile else None
    conv['order_date_clean']   = (pd.to_datetime(conv[cc_orderdt], errors='coerce', utc=True)
                                   .dt.tz_localize(None) if cc_orderdt else pd.NaT)
    conv['payment_received']   = safe_numeric(conv[cc_payrec]) if cc_payrec else 0
    conv['total_gst']          = safe_numeric(conv[cc_gst])    if cc_gst    else 0
    conv['total_due']          = safe_numeric(conv[cc_due])    if cc_due    else 0
    conv['paid_amount']        = conv['payment_received']
    conv['service_name_clean'] = conv[cc_service].astype(str).str.strip() if cc_service else ''
    conv['trainer_clean']      = conv[cc_trainer].astype(str).str.strip() if cc_trainer else ''
    conv['sales_rep_clean']    = conv[cc_salesrep].astype(str).str.strip() if cc_salesrep else ''
    conv['payment_mode_clean'] = conv[cc_mode].astype(str).str.strip()    if cc_mode    else ''
    conv['status_clean']       = conv[cc_status].astype(str).str.strip()  if cc_status  else ''
    conv['order_id_clean']     = conv[cc_orderid].astype(str).str.strip() if cc_orderid else ''

    # ── Leads ──────────────────────────────────────────────────────────────
    lead_map = pd.DataFrame()
    lc_mobile = lc_convfrom = lc_source = lc_campaign = lc_status_l = None
    lc_stage  = lc_owner = lc_state = lc_attempted = lc_service = None
    lc_email  = lc_remarks = lc_name_l = None

    if leads_file is not None:
        try:
            leads = _load_file(leads_file, leads_name)
            leads.columns = [str(c).strip() for c in leads.columns]
            lc_mobile    = _detect(leads,'phone','Phone','mobile','Mobile')
            lc_convfrom  = _detect(leads,'converted_from','ConvertedFrom','lead_type','LeadType')
            lc_source    = _detect(leads,'leadsource','lead_source','LeadSource','Source')
            lc_campaign  = _detect(leads,'campaign_name','Campaign','CampaignName')
            lc_status_l  = _detect(leads,'leadstatus','lead_status','LeadStatus','Status')
            lc_stage     = _detect(leads,'stage_name','StageName','Stage')
            lc_owner     = _detect(leads,'leadownername','LeadOwner','lead_owner','Owner')
            lc_state     = _detect(leads,'state','State','Province')
            lc_attempted = _detect(leads,'Attempted/Unattempted','attempted','Attempted')
            lc_service   = _detect(leads,'servicename','ServiceName','service_name')
            lc_email     = _detect(leads,'email','Email')
            lc_remarks   = _detect(leads,'remarks','Remarks','Notes')
            lc_name_l    = _detect(leads,'name','Name','StudentName')
            if lc_mobile:
                leads['mobile_clean'] = leads[lc_mobile].apply(clean_mobile)
                lead_map = leads.dropna(subset=['mobile_clean']).drop_duplicates('mobile_clean').set_index('mobile_clean')
        except Exception:
            pass

    def get_lead(possible_mobiles):
        blank = {'webinar_type':'','lead_source':'','campaign_name':'','lead_status':'',
                 'stage_name':'','lead_owner':'','state':'','attempted':'',
                 'service_name_lead':'','email':'','remarks':'','lead_name':''}
        if lead_map.empty:
            return blank
        for mob in possible_mobiles:
            if mob and mob in lead_map.index:
                r = lead_map.loc[mob]
                if isinstance(r, pd.DataFrame): r = r.iloc[0]
                def gs(col):
                    return str(r[col]).strip() if col and col in r.index and pd.notna(r[col]) else ''
                wt = gs(lc_convfrom)
                if not wt:
                    src = gs(lc_source)
                    wt = 'Webinar' if 'WBN' in src.upper() else ('Non Webinar' if src else '')
                blank.update({'webinar_type':wt,'lead_source':gs(lc_source),
                              'campaign_name':gs(lc_campaign),'lead_status':gs(lc_status_l),
                              'stage_name':gs(lc_stage),'lead_owner':gs(lc_owner),
                              'state':gs(lc_state),'attempted':gs(lc_attempted),
                              'service_name_lead':gs(lc_service),'email':gs(lc_email),
                              'remarks':gs(lc_remarks),'lead_name':gs(lc_name_l)})
                break
        return blank

    # ── Match loop ─────────────────────────────────────────────────────────
    student_rows = []
    order_rows   = []

    for _, row in attendees.iterrows():
        mob      = row.get('mobile_clean')
        alt_mob  = row.get('alt_mobile_clean')
        possible = [m for m in [mob, alt_mob] if m]
        sem_dt   = row['seminar_date']

        entry = {
            'name':               _s(row.get(c_name,'')) if c_name else '',
            'mobile':             mob or alt_mob or '',
            'place':              _s(row.get(c_place,'')) if c_place else '',
            'trainer':            _s(row.get(c_trainer,'')) if c_trainer else '',
            'seminar_date':       sem_dt.strftime('%Y-%m-%d') if pd.notna(sem_dt) else '',
            'seminar_month':      sem_dt.strftime('%Y-%m')    if pd.notna(sem_dt) else '',
            'session':            _s(row.get(c_session,'')).upper() if c_session else '',
            'attended':           bool(row.get('attended_flag',False)),
            'seat_book_amount':   float(row.get('seat_book_amount',0) or 0),
            'seat_booked':        bool(float(row.get('seat_book_amount',0) or 0) > 0),
            'primary_course':     '',
            'primary_order_date': '',
            'primary_paid':       0.0,
            'primary_due':        0.0,
            'primary_gst':        0.0,
            'primary_mode':       '',
            'primary_status':     '',
            'additional_courses': [],
            'additional_paid':    0.0,
            'additional_due':     0.0,
            'converted':          False,
            'sales_rep':          '',
            'match_reason':       '',
        }

        all_orders = (conv[conv['mobile_clean'].isin(possible)]
                      .sort_values('order_date_clean').copy()) if possible else pd.DataFrame()

        if not possible:
            entry['match_reason'] = 'No mobile'
        elif all_orders.empty:
            entry['match_reason'] = 'No conversion row'
        else:
            if pd.notna(sem_dt) and all_orders['order_date_clean'].notna().any():
                after_cnt = (all_orders['order_date_clean'] >= sem_dt).sum()
                entry['match_reason'] = 'Matched (post seminar)' if after_cnt > 0 else 'Matched (pre seminar)'
            else:
                entry['match_reason'] = 'Matched'

        if not all_orders.empty:
            entry['primary_status'] = normalize_status(all_orders.iloc[-1]['status_clean'])
            # Only mark converted if at least one order has payment > 0
            paid_orders = all_orders[all_orders['paid_amount'] > 0]
            entry['converted'] = len(paid_orders) > 0

            valid_after = (all_orders[all_orders['order_date_clean'] >= sem_dt]
                           if pd.notna(sem_dt) and all_orders['order_date_clean'].notna().any()
                           else pd.DataFrame())
            primary_pool = valid_after if not valid_after.empty else all_orders
            # For primary selection, prefer paid orders
            if not primary_pool[primary_pool['paid_amount'] > 0].empty:
                primary_pool = primary_pool[primary_pool['paid_amount'] > 0]
            pti_pool     = primary_pool[primary_pool['service_name_clean'].str.contains(COMBO_MATCH, na=False, case=False)]
            primary      = pti_pool.iloc[0] if not pti_pool.empty else primary_pool.iloc[0]

            entry['primary_course']     = primary['service_name_clean']
            entry['primary_order_date'] = primary['order_date_clean'].strftime('%Y-%m-%d') if pd.notna(primary['order_date_clean']) else ''
            entry['primary_paid']       = float(primary['paid_amount'])
            entry['primary_due']        = float(primary['total_due'])
            entry['primary_gst']        = float(primary['total_gst'])
            entry['primary_mode']       = str(primary['payment_mode_clean']).strip()
            entry['sales_rep']          = str(primary['sales_rep_clean']).strip()

            # Additional = ALL OTHER PAID orders (not the primary)
            paid_others = paid_orders[paid_orders.index != primary.name]
            entry['additional_courses'] = [
                {
                    'course':  str(o['service_name_clean']).strip(),
                    'paid':    float(o['paid_amount']),
                    'due':     float(o['total_due']),
                    'gst':     float(o['total_gst']),
                    'mode':    str(o['payment_mode_clean']).strip(),
                    'status':  normalize_status(o['status_clean']),
                    'order_date': o['order_date_clean'].strftime('%Y-%m-%d') if pd.notna(o['order_date_clean']) else '',
                    'sales_rep': str(o['sales_rep_clean']).strip(),
                    'order_id':  str(o['order_id_clean']).strip(),
                }
                for _, o in paid_others.iterrows()
            ]
            entry['additional_paid']    = float(paid_others['paid_amount'].sum())
            entry['additional_due']     = float(paid_others['total_due'].sum())

            # Export ALL orders (paid and unpaid) but mark paid status
            for _, o in all_orders.iterrows():
                order_rows.append({
                    'name':         entry['name'],  'mobile':       entry['mobile'],
                    'place':        entry['place'],  'seminar_date': entry['seminar_date'],
                    'course':       str(o['service_name_clean']).strip(),
                    'order_date':   o['order_date_clean'].strftime('%Y-%m-%d') if pd.notna(o['order_date_clean']) else '',
                    'order_month':  o['order_date_clean'].strftime('%Y-%m')    if pd.notna(o['order_date_clean']) else '',
                    'paid_amount':  float(o['paid_amount']),
                    'total_due':    float(o['total_due']),
                    'total_gst':    float(o['total_gst']),
                    'payment_mode': str(o['payment_mode_clean']).strip(),
                    'status':       normalize_status(o['status_clean']),
                    'sales_rep':    str(o['sales_rep_clean']).strip(),
                    'is_primary':   bool(o.name == primary.name),
                    'order_id':     str(o['order_id_clean']).strip(),
                })

        entry.update(get_lead(possible))
        student_rows.append(entry)

    # ── Seminar meta (per event summary for charts) ────────────────────────
    meta_map = {}
    for s in student_rows:
        key = (s['seminar_date'], s['place'])
        if key not in meta_map:
            meta_map[key] = {'date':s['seminar_date'],'month':s['seminar_month'],
                             'place':s['place'],'trainer':s['trainer'],'session':s['session'],
                             'total':0,'attended':0,'seat_booked':0,'seat_book_amount':0.0,
                             'converted':0,'paid':0.0,'due':0.0}
        m = meta_map[key]
        m['total']            += 1
        m['attended']         += 1 if s['attended'] else 0
        m['seat_booked']      += 1 if s['seat_booked'] else 0
        m['seat_book_amount'] += s['seat_book_amount']
        m['converted']        += 1 if s['converted'] else 0
        m['paid']             += s['primary_paid'] + s['additional_paid']
        m['due']              += s['primary_due']
    seminar_meta = sorted(meta_map.values(), key=lambda r: r['date'])

    # ── Aggregations ───────────────────────────────────────────────────────
    total      = len(student_rows)
    conv_count = sum(1 for s in student_rows if s['converted'])
    t_paid     = sum(s['primary_paid'] + s['additional_paid'] for s in student_rows)
    t_due      = sum(s['primary_due'] + s.get('additional_due', 0) for s in student_rows)

    course_stats = {}
    for s in student_rows:
        if not s['converted'] or not s['primary_course']: continue
        c = s['primary_course']
        if c not in course_stats:
            course_stats[c] = {'count':0,'paid':0.0,'due':0.0,'fully_paid':0}
        course_stats[c]['count']      += 1
        course_stats[c]['paid']       += s['primary_paid']
        course_stats[c]['due']        += s['primary_due']
        course_stats[c]['fully_paid'] += 1 if s['primary_due'] <= 0 else 0
    course_stats = dict(sorted(course_stats.items(), key=lambda x: -x[1]['count']))

    sr_stats = {}
    for s in student_rows:
        if not s['converted'] or not s['sales_rep'] or s['sales_rep'] in ('','nan'): continue
        r = s['sales_rep']
        if r not in sr_stats:
            sr_stats[r] = {'deals':0,'revenue':0.0,'due':0.0,'active':0,'avg_deal':0.0}
        sr_stats[r]['deals']  += 1
        sr_stats[r]['revenue']+= s['primary_paid']
        sr_stats[r]['due']    += s['primary_due']
        sr_stats[r]['active'] += 1 if s['primary_status'] == 'Active' else 0
    for r in sr_stats:
        sr_stats[r]['avg_deal'] = round(sr_stats[r]['revenue'] / sr_stats[r]['deals'], 2) if sr_stats[r]['deals'] else 0
    sr_stats = dict(sorted(sr_stats.items(), key=lambda x: -x[1]['revenue'])[:25])

    loc_stats = {}
    for s in student_rows:
        loc = s['place'] or 'Unknown'
        if loc not in loc_stats:
            loc_stats[loc] = {'total':0,'converted':0,'paid':0.0,'due':0.0,'seat_booked':0}
        loc_stats[loc]['total']       += 1
        loc_stats[loc]['converted']   += 1 if s['converted'] else 0
        loc_stats[loc]['paid']        += s['primary_paid']
        loc_stats[loc]['due']         += s['primary_due']
        loc_stats[loc]['seat_booked'] += 1 if s['seat_booked'] else 0
    loc_stats = dict(sorted(loc_stats.items(), key=lambda x: -x[1]['paid'])[:40])

    lead_src_stats = {}
    for s in student_rows:
        src = s.get('lead_source') or 'Unknown'
        if src == 'nan': src = 'Unknown'
        if src not in lead_src_stats:
            lead_src_stats[src] = {'count':0,'converted':0}
        lead_src_stats[src]['count']    += 1
        lead_src_stats[src]['converted']+= 1 if s['converted'] else 0

    stage_stats = {}
    for s in student_rows:
        st = s.get('stage_name') or ''
        if not st or st == 'nan': continue
        if st not in stage_stats:
            stage_stats[st] = {'count':0,'converted':0}
        stage_stats[st]['count']    += 1
        stage_stats[st]['converted']+= 1 if s['converted'] else 0

    trainer_stats = {}
    for s in student_rows:
        tr = s['trainer'] or 'Unknown'
        if tr not in trainer_stats:
            trainer_stats[tr] = {'total':0,'converted':0,'paid':0.0,'seat_booked':0}
        trainer_stats[tr]['total']       += 1
        trainer_stats[tr]['converted']   += 1 if s['converted'] else 0
        trainer_stats[tr]['paid']        += s['primary_paid']
        trainer_stats[tr]['seat_booked'] += 1 if s['seat_booked'] else 0

    monthly = {}
    for s in student_rows:
        m = s['seminar_month'] or 'Unknown'
        if m not in monthly:
            monthly[m] = {'total':0,'converted':0,'paid':0.0,'seat_booked':0,'seat_amount':0.0}
        monthly[m]['total']       += 1
        monthly[m]['converted']   += 1 if s['converted'] else 0
        monthly[m]['paid']        += s['primary_paid']
        monthly[m]['seat_booked'] += 1 if s['seat_booked'] else 0
        monthly[m]['seat_amount'] += s['seat_book_amount']
    monthly = dict(sorted(monthly.items()))

    # Extra counts for KPIs
    n_attended    = sum(1 for s in student_rows if s['attended'])
    n_seat_booked = sum(1 for s in student_rows if s['seat_booked'])
    n_fully_paid  = sum(1 for s in student_rows if s['converted'] and s['primary_due'] <= 0)
    n_has_due     = sum(1 for s in student_rows if s['converted'] and s['primary_due'] > 0)
    n_webinar     = sum(1 for s in student_rows if s.get('webinar_type') == 'Webinar')
    n_non_webinar = sum(1 for s in student_rows if s.get('webinar_type') == 'Non Webinar')
    n_attempted   = sum(1 for s in student_rows if s.get('attempted') == 'Attempted')
    n_unattempted = sum(1 for s in student_rows if s.get('attempted') == 'Unattempted')
    n_add_rev     = round(sum(s['additional_paid'] for s in student_rows), 2)

    # Unique seminars = unique (date, place) pairs
    num_seminars  = len(set((s['seminar_date'], s['place']) for s in student_rows))
    # Unique locations
    num_locations = len(set(s['place'] for s in student_rows if s['place']))

    # Lead status breakdown
    lead_status_stats = {}
    for s in student_rows:
        ls = s.get('lead_status') or 'Unknown'
        if ls in ('', 'nan'): ls = 'Unknown'
        if ls not in lead_status_stats:
            lead_status_stats[ls] = {'count': 0, 'converted': 0}
        lead_status_stats[ls]['count']    += 1
        lead_status_stats[ls]['converted']+= 1 if s['converted'] else 0

    # Campaign breakdown
    campaign_stats = {}
    for s in student_rows:
        cp = s.get('campaign_name') or ''
        if cp in ('', 'nan'): continue
        if cp not in campaign_stats:
            campaign_stats[cp] = {'count': 0, 'converted': 0, 'revenue': 0.0}
        campaign_stats[cp]['count']    += 1
        campaign_stats[cp]['converted']+= 1 if s['converted'] else 0
        campaign_stats[cp]['revenue']  += s['primary_paid']

    agg = {
        'total_attendees':    total,
        'num_attended':       n_attended,
        'num_seat_booked':    n_seat_booked,
        'num_seminars':       num_seminars,
        'num_locations':      num_locations,
        'converted':          conv_count,
        'conversion_rate':    round(conv_count / n_seat_booked * 100, 1) if n_seat_booked else 0,
        'attended_rate':      round(n_attended / total * 100, 1) if total else 0,
        'seat_to_conv_rate':  round(conv_count / n_seat_booked * 100, 1) if n_seat_booked else 0,
        'total_paid':         round(t_paid, 2),
        'total_due':          round(t_due, 2),
        'seat_book_count':    n_seat_booked,
        'seat_book_amount':   round(sum(s['seat_book_amount'] for s in student_rows), 2),
        'fully_paid':         n_fully_paid,
        'has_due':            n_has_due,
        'additional_revenue': n_add_rev,
        'avg_paid':           round(t_paid / conv_count, 2) if conv_count else 0,
        'webinar_leads':      n_webinar,
        'non_webinar_leads':  n_non_webinar,
        'attempted':          n_attempted,
        'unattempted':        n_unattempted,
        'unique_courses':     len(course_stats),
        'course_stats':       course_stats,
        'sales_rep_stats':    sr_stats,
        'location_stats':     loc_stats,
        'lead_source_stats':  lead_src_stats,
        'lead_status_stats':  lead_status_stats,
        'campaign_stats':     campaign_stats,
        'stage_stats':        stage_stats,
        'trainer_stats':      trainer_stats,
        'monthly_trend':      monthly,
    }
    return student_rows, order_rows, seminar_meta, agg


# ──────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ──────────────────────────────────────────────────────────────────────────────

def process_all(webinar_file=None, seminar_file=None, attendee_file=None,
                seminar_updated_file=None, conversion_file=None, leads_file=None,
                sem_name='', conv_name='', leads_name=''):
    errors = []

    try:
        bcmb, insg = parse_webinar_file(webinar_file) if webinar_file else ([],[])
    except Exception as e:
        errors.append(f'Webinar file: {e}')
        bcmb, insg = [], []

    try:
        students, orders, seminar_meta, agg = parse_offline_files(
            seminar_updated_file, conversion_file, leads_file,
            sem_name, conv_name, leads_name)
    except Exception as e:
        errors.append(f'Offline files: {e}')
        students, orders, seminar_meta, agg = [], [], [], {}

    # Bridge offline rows for integrated template
    offline_rows = [{'date':s['date'],'yearMonth':s['month'],'trainer':s['trainer'],
                     'location':s['place'],'course':'OFFLINE','type':'Offline','mode':'Offline',
                     'targeted':s['total'],'registered':s['attended'],'over30':s['attended'],
                     'seatBooked':s['seat_booked'],'joined':s['seat_booked'],
                     'revenue':s['paid'],'expenses':0,'surplus':s['paid']}
                    for s in seminar_meta]

    return {
        'bcmb':            bcmb,
        'insg':            insg,
        'offline':         offline_rows,
        'seminar':         seminar_meta,      # kept for legacy
        'students':        students,           # NEW: per-student records
        'orders':          orders,             # NEW: per-order records
        'seminar_meta':    seminar_meta,       # NEW: per-event summary
        'offline_agg':     agg,               # NEW: aggregated KPIs
        'att_summary':     {},
        'ct_stats':        agg.get('course_stats',{}),
        'sr_stats':        agg.get('sales_rep_stats',{}),
        'loc_stats':       agg.get('location_stats',{}),
        'conversion_stats':{},
        'leads_stats':     {},
        'seminar_updated': [],
        'errors':          errors,
        'stats': {
            'bcmb_count':    len(bcmb),
            'insg_count':    len(insg),
            'seminar_count': len(seminar_meta),
            'locations':     len(set(s['place'] for s in seminar_meta)),
            'students':      agg.get('total_attendees',0),
            'conversions':   agg.get('converted',0),
            'leads':         0,
        },
    }
