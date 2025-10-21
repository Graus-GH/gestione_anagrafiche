import re
from urllib.parse import urlparse, parse_qs

def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())

def index_of(options, value):
    v = str(value or "")
    try:
        return options.index(v)
    except ValueError:
        return 0

def ensure_drive_view_url(url: str) -> str:
    """
    Accetta:
      - https://drive.google.com/file/d/<ID>/view?usp=...
      - https://drive.google.com/open?id=<ID>
      - https://drive.google.com/uc?export=view&id=<ID>
    Ritorna sempre: https://drive.google.com/uc?export=view&id=<ID>
    """
    if not url:
        return ""
    parsed = urlparse(url)
    if "drive.google.com" not in parsed.netloc:
      # URL generico: ritorno così com'è (immagini pubbliche dirette)
      return url
    # Estraggo ID
    # pattern /file/d/<id>/ or query id=<id>
    m = re.search(r"/file/d/([^/]+)/", parsed.path)
    if m:
        file_id = m.group(1)
    else:
        qs = parse_qs(parsed.query)
        file_id = qs.get("id", [""])[0]
    if not file_id:
        return url
    return f"https://drive.google.com/uc?export=view&id={file_id}"

def image_suggestions_for_key(key: str):
    """
    Stub: qui puoi collegare una tua mappa chiave->URL suggeriti (es. da un foglio/DB).
    Per ora ritorna lista vuota o esempi.
    """
    if not key:
        return []
    # Esempio: se il codice contiene "TEST" offriamo un URL demo
    if "TEST" in key.upper():
        return ["https://drive.google.com/uc?export=view&id=1WmltIuDp5YqOTPjyKHx4qNXUcQoDyNBf"]
    return []
