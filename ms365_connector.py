"""
ms365_connector.py  —  SharePoint / Microsoft 365 file fetcher
Compatible with Python 3.9+

Fetches 4 Excel files:
  webinar         -> Free Class Lead Report      (BCMB + INSIGNIA)
  seminar_updated -> Seminar Updated Sheet        (attendance, seat bookings)
  conversion      -> Conversion List              (orders, payments)
  leads           -> Leads Report                 (lead source, campaign, stage)
"""

import base64
import io
import requests
import streamlit as st

_TENANT    = "admininvesmate360.onmicrosoft.com"
_CLIENT_ID = "d3590ed6-52b3-4102-aeff-aad2292ab01c"

_FILES = {
    "webinar": {
        "name":         "Free Class Lead Report.xlsx",
        "user":         "admin_admininvesmate360_onmicrosoft_com",
        "item_id":      "B4E16F58-734E-403A-8F5E-3E60656AF593",
        "share_secret": "SHARE_URL_WEBINAR",
    },
    "seminar_updated": {
        "name":         "Seminar Updated Sheet.xlsx",
        "user":         "admin_admininvesmate360_onmicrosoft_com",
        "share_secret": "SHARE_URL_SEMINAR_UPDATE",
    },
    "conversion": {
        "name":         "Conversion List.xlsx",
        "user":         "swatatabanerjee_invesmate_com",
        "share_secret": "SHARE_URL_CONVERSION",
    },
    "leads": {
        "name":         "Leads.xlsx",
        "user":         "swatatabanerjee_invesmate_com",
        "share_secret": "SHARE_URL_LEADS",
    },
}


def _safe_json(resp):
    try:
        return resp.json()
    except Exception:
        return {}


def _graph_user_id(raw):
    raw = (raw or "").strip()
    if not raw or "@" in raw:
        return raw
    parts = raw.split("_")
    if len(parts) >= 3:
        return parts[0] + "@" + ".".join(parts[1:])
    return raw


def _get_secret(name):
    try:
        return st.secrets.get(name, "").strip()
    except Exception:
        return ""


def _encode_share_url(share_url):
    encoded = (
        base64.urlsafe_b64encode(share_url.encode("utf-8"))
        .decode("utf-8")
        .rstrip("=")
    )
    return "u!" + encoded


def _get_token():
    try:
        email    = st.secrets["MS_EMAIL"].strip()
        password = st.secrets["MS_PASSWORD"].strip()
    except KeyError as e:
        raise ConnectionError(
            "Missing secret: {}. "
            "Add MS_EMAIL and MS_PASSWORD in Streamlit Cloud Secrets.".format(e)
        )

    resp = requests.post(
        "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(_TENANT),
        data={
            "grant_type": "password",
            "client_id":  _CLIENT_ID,
            "username":   email,
            "password":   password,
            "scope":      "https://graph.microsoft.com/.default offline_access",
        },
        timeout=20,
    )
    body  = _safe_json(resp)

    if resp.status_code != 200:
        err   = body.get("error_description") or body.get("error") or resp.text
        err_l = err.lower()
        if "aadsts50126" in err_l or "aadsts50034" in err_l:
            raise ConnectionError("Wrong email or password. Check MS_EMAIL / MS_PASSWORD.")
        if "aadsts53003" in err_l or "conditional" in err_l:
            raise ConnectionError("Conditional Access policy is blocking sign-in.")
        if "aadsts7000218" in err_l:
            raise ConnectionError("Enable public client flows in your Azure App Registration.")
        raise ConnectionError("Authentication failed: {}".format(err[:400]))

    token = body.get("access_token")
    if not token:
        raise ConnectionError("Authentication succeeded but no access token was returned.")
    return token


def _is_excel(resp):
    ct = (resp.headers.get("Content-Type") or "").lower()
    return (
        "spreadsheet" in ct
        or "excel" in ct
        or ("octet-stream" in ct and resp.content[:4] == b"PK\x03\x04")
        or resp.content[:4] == b"PK\x03\x04"
    )


def _from_share_url(token, share_url):
    if not share_url:
        return None
    resp = requests.get(
        "https://graph.microsoft.com/v1.0/shares/{}/driveItem/content".format(
            _encode_share_url(share_url)
        ),
        headers={"Authorization": "Bearer {}".format(token)},
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code == 200 and _is_excel(resp):
        return io.BytesIO(resp.content)
    return None


def _from_item_id(token, user, item_id):
    if not user or not item_id:
        return None
    resp = requests.get(
        "https://graph.microsoft.com/v1.0/users/{}/drive/items/{}/content".format(
            user, item_id
        ),
        headers={"Authorization": "Bearer {}".format(token)},
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code == 200 and _is_excel(resp):
        return io.BytesIO(resp.content)
    return None


def _from_search(token, user, filename):
    if not user:
        return None
    sr = requests.get(
        "https://graph.microsoft.com/v1.0/users/{}/drive/root/search(q='{}')".format(
            user, filename
        ),
        headers={"Authorization": "Bearer {}".format(token)},
        timeout=20,
    )
    if sr.status_code != 200:
        return None
    items = _safe_json(sr).get("value", [])
    match = next(
        (i for i in items if str(i.get("name", "")).lower() == filename.lower()),
        items[0] if items else None,
    )
    if not match:
        return None
    dl = requests.get(
        "https://graph.microsoft.com/v1.0/users/{}/drive/items/{}/content".format(
            user, match["id"]
        ),
        headers={"Authorization": "Bearer {}".format(token)},
        timeout=30,
        allow_redirects=True,
    )
    if dl.status_code == 200 and _is_excel(dl):
        return io.BytesIO(dl.content)
    return None


def _download(token, file_key):
    meta      = _FILES[file_key]
    name      = meta["name"]
    user      = _graph_user_id(meta.get("user", ""))
    share_url = _get_secret(meta.get("share_secret", ""))

    data = _from_share_url(token, share_url)
    if data is not None:
        return data

    data = _from_item_id(token, user, meta.get("item_id", ""))
    if data is not None:
        return data

    data = _from_search(token, user, name)
    if data is not None:
        return data

    me = _get_secret("MS_EMAIL")
    if me and me != user:
        data = _from_search(token, me, name)
        if data is not None:
            return data

    raise FileNotFoundError(
        "Could not fetch '{}' from Microsoft 365. "
        "Set '{}' in Streamlit Cloud Secrets.".format(name, meta["share_secret"])
    )


# ── Public functions ──────────────────────────────────────────────────────────

@st.cache_data(ttl=0, show_spinner=False)
def fetch_excel_files(_cache_bust=0):
    """Fetch all 4 Excel files. Returns dict keyed by file name."""
    token = _get_token()
    return {key: _download(token, key) for key in _FILES}


def check_secrets_configured():
    """Returns (ok, missing_list). Checks MS_EMAIL and MS_PASSWORD."""
    missing = []
    try:
        for key in ["MS_EMAIL", "MS_PASSWORD"]:
            if not st.secrets.get(key, "").strip():
                missing.append(key)
    except Exception:
        missing = ["MS_EMAIL", "MS_PASSWORD"]
    return len(missing) == 0, missing


def check_share_urls_configured():
    """Returns dict of {secret_name: bool} for each SharePoint URL secret."""
    secrets_to_check = [
        "SHARE_URL_WEBINAR",
        "SHARE_URL_SEMINAR_UPDATE",
        "SHARE_URL_CONVERSION",
        "SHARE_URL_LEADS",
    ]
    result = {}
    for s in secrets_to_check:
        try:
            result[s] = bool(st.secrets.get(s, "").strip())
        except Exception:
            result[s] = False
    return result
