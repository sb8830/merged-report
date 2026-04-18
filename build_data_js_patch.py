# DROP-IN REPLACEMENT for build_data_js in the merged app.py
# Replace the existing build_data_js function with this one

def build_data_js(data, mode):
    import json
    def _j(o): return json.dumps(o, ensure_ascii=False, default=str)

    b   = _j(data['bcmb'])
    i   = _j(data['insg'])
    off = _j(data['offline'])
    # NEW: student-level offline data
    stu  = _j(data.get('students', []))
    ord_ = _j(data.get('orders', []))
    agg  = _j(data.get('offline_agg', {}))
    # Legacy aggregate tables (still used by integrated template)
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
