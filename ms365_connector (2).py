"""
ms365_connector.py  —  SharePoint connector using Microsoft account credentials

Supports 6 Excel files fetched via SharePoint "Anyone with link" share URLs:
  - SHARE_URL_WEBINAR        → Free Class Lead Report (BCMB + INSIGNIA)
  - SHARE_URL_SEMINAR        → Offline Seminar Report
  - SHARE_URL_ATTENDEE       → Offline Indepth Details
  - SHARE_URL_SEMINAR_UPDATE → Seminar Updated Files
  - SHARE_URL_CONVERSION     → Conversion List
  - SHARE_URL_LEADS          → Leads
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
    "seminar": {
        "name":         "Offline Seminar Report.xlsx",
        "user":         "admin_admininvesmate360_onmicrosoft_com",
        "item_id":      "A4283220-7EF3-49B5-87DD-B7FD023D436D",
        "share_secret": "SHARE_URL_SEMINAR",
    },
    "attendee": {
        "name":         "Offline Indepth Details.xlsx",
        "user":         "sourajpal_invesmate_com",
        "share_secret": "SHARE_URL_ATTENDEE",
    },
    "seminar_updated": {
        "name":         "Seminar Updated Files.xlsx",
        "user":         "",          # SharePoint team site — share URL is primary
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


def _safe_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {}


def _graph_user_id(raw: str) -> str:
    """Convert OneDrive-style user IDs to Graph-compatible UPNs."""
    raw = (raw or "").strip()
    if not raw or "@" in raw:
        return raw
    parts = raw.split("_")
    if len(parts) >= 3:
        return parts[0] + "@" + ".".join(parts[1:])
    return raw


def _get_secret(name: str) -> str:
    try:
        return st.secrets.get(name, "").strip()
    except Exception:
        return ""


def _encode_share_url(share_url: str) -> str:
    encoded = (base64.urlsafe_b64encode(share_url.encode("utf-8"))
               .decode("utf-8").rstrip("="))
    return "u!" + encoded


def _get_token() -> str:
    try:
        email    = st.secrets["MS_EMAIL"].strip()
        password = st.secrets["MS_PASSWORD"].strip()
    except KeyError as e:
        raise ConnectionError(
            f"❌ Missing secret: {e}\n"
            "Add MS_EMAIL and MS_PASSWORD to Streamlit Cloud Secrets."
        )

    resp = requests.post(
        f"https://login.microsoftonline.com/{_TENANT}/oauth2/v2.0/token",
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
            raise ConnectionError(
                "❌ Wrong email or password.\n"
                "Check MS_EMAIL and MS_PASSWORD in Streamlit Secrets."
            )
        if "aadsts53003" in err_l or "conditional" in err_l:
            raise ConnectionError(
                "❌ Conditional Access policy is blocking sign-in.\n"
                "Ask your admin to exclude this app from the Conditional Access policy."
            )
        if "aadsts7000218" in err_l:
            raise ConnectionError(
                "❌ Client not allowed to use ROPC flow.\n"
                "Enable public client flows in Azure App Registration."
            )
        raise ConnectionError(f"❌ Authentication failed:\n{err[:500]}")

    token = body.get("access_token")
    if not token:
        raise ConnectionError("❌ Authentication succeeded but no access token was returned.")
    return token


def _is_excel(resp: requests.Response) -> bool:
    ct = (resp.headers.get("Content-Type") or "").lower()
    return (
        "spreadsheet" in ct
        or "excel" in ct
        or ("octet-stream" in ct and resp.content[:4] == b"PK\x03\x04")
        or resp.content[:4] == b"PK\x03\x04"
    )


def _download_from_share_url(token: str, share_url: str) -> io.BytesIO | None:
    if not share_url:
        return None
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/shares/{_encode_share_url(share_url)}/driveItem/content",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code == 200 and _is_excel(resp):
        return io.BytesIO(resp.content)
    return None


def _download_by_item_id(token: str, user: str, item_id: str) -> io.BytesIO | None:
    if not user or not item_id:
        return None
    resp = requests.get(
        f"https://graph.microsoft.com/v1.0/users/{user}/drive/items/{item_id}/content",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
        allow_redirects=True,
    )
    if resp.status_code == 200 and _is_excel(resp):
        return io.BytesIO(resp.content)
    return None


def _download_by_search(token: str, user: str, filename: str) -> io.BytesIO | None:
    if not user:
        return None
    sr = requests.get(
        f"https://graph.microsoft.com/v1.0/users/{user}/drive/root/search(q='{filename}')",
        headers={"Authorization": f"Bearer {token}"},
        timeout=20,
    )
    if sr.status_code != 200:
        return None

    items = _safe_json(sr).get("value", [])
    match = next((i for i in items if str(i.get("name", "")).lower() == filename.lower()), None)
    if not match and items:
        match = items[0]
    if not match:
        return None

    dl = requests.get(
        f"https://graph.microsoft.com/v1.0/users/{user}/drive/items/{match['id']}/content",
        headers={"Authorization": f"Bearer {token}"},
        timeout=30,
        allow_redirects=True,
    )
    if dl.status_code == 200 and _is_excel(dl):
        return io.BytesIO(dl.content)
    return None


def _download(token: str, file_key: str) -> io.BytesIO:
    meta      = _FILES[file_key]
    name      = meta["name"]
    user      = _graph_user_id(meta.get("user", ""))
    share_url = _get_secret(meta.get("share_secret", ""))

    # 1) Most robust: share URL from secrets
    data = _download_from_share_url(token, share_url)
    if data is not None:
        return data

    # 2) Existing item ID
    data = _download_by_item_id(token, user, meta.get("item_id", ""))
    if data is not None:
        return data

    # 3) Search by exact filename in owner drive
    data = _download_by_search(token, user, name)
    if data is not None:
        return data

    # 4) Try signed-in account as fallback
    me_user = _get_secret("MS_EMAIL")
    if me_user and me_user != user:
        data = _download_by_search(token, me_user, name)
        if data is not None:
            return data

    raise FileNotFoundError(
        f"❌ File '{name}' could not be fetched from Microsoft 365.\n"
        "Tried: SHARE_URL secret, item ID, and filename search.\n"
        f"Ensure the secret '{meta['share_secret']}' is set in Streamlit Cloud Secrets."
    )


@st.cache_data(ttl=0, show_spinner=False)
def fetch_excel_files(_cache_bust: int = 0) -> dict:
    """
    Fetch all 6 Excel files from Microsoft 365.
    Returns a dict with keys:
      webinar, seminar, attendee, seminar_updated, conversion, leads
    Each value is an io.BytesIO of the Excel file.
    """
    token = _get_token()
    return {key: _download(token, key) for key in _FILES}


def check_secrets_configured() -> tuple:
    """
    Returns (ok: bool, missing: list[str]).
    Checks that at minimum MS_EMAIL and MS_PASSWORD are set,
    plus warns if any SHARE_URL_* secrets are missing.
    """
    missing = []
    try:
        for key in ["MS_EMAIL", "MS_PASSWORD"]:
            if not st.secrets.get(key, "").strip():
                missing.append(key)
    except Exception:
        missing = ["MS_EMAIL", "MS_PASSWORD"]
    return len(missing) == 0, missing


def check_share_urls_configured() -> dict:
    """
    Returns a dict of {secret_name: bool} for each SHARE_URL_* secret.
    """
    share_secrets = [
        "SHARE_URL_WEBINAR",
        "SHARE_URL_SEMINAR",
        "SHARE_URL_ATTENDEE",
        "SHARE_URL_SEMINAR_UPDATE",
        "SHARE_URL_CONVERSION",
        "SHARE_URL_LEADS",
    ]
    result = {}
    for s in share_secrets:
        try:
            result[s] = bool(st.secrets.get(s, "").strip())
        except Exception:
            result[s] = False
    return result
