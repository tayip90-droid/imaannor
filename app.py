# app.py
from flask import Flask, render_template, jsonify, request
from flask_caching import Cache
import os, json, re, difflib, time
from collections import defaultdict
from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union

# <<< Excel için >>>
import pandas as pd

app = Flask(__name__)
app.config['CACHE_TYPE'] = 'simple'
app.config['CACHE_DEFAULT_TIMEOUT'] = 3600
cache = Cache(app)

# Parti renkleri
PARTY_COLORS = {
    "AK Parti": "#f39c12",
    "CHP": "#FF0000",          # daha cart kırmızı
    "MHP": "#2980b9",
    "İYİ Parti": "#FFEB3B",    # açık sarı
    "DEM Parti": "#6c3483",
    "Büyük Birlik": "#4A4A4A", # koyu gri
    "Yeniden Refah": "#d35400",
}
DEFAULT_COLOR = "#f8f9f9"

# === Excel dosya yolu (gerekirse değiştir) ===
DATA_EXCEL_PATH = os.path.join(app.root_path, "static", "BKM_MARKA_CIROLAR.xlsx")

# ---------------------------
# Yardımcılar (Geo)
# ---------------------------

def load_geojson(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def filter_polygons(geojson):
    feats = []
    for ft in geojson.get("features", []):
        geom = (ft.get("geometry") or {})
        gtype = geom.get("type")
        if gtype in ("Polygon", "MultiPolygon"):
            feats.append(ft)
    return {"type": "FeatureCollection", "features": feats}

def keep_significant_components(geojson, min_ratio=0.003):
    for ft in geojson.get("features", []):
        geom = ft.get("geometry") or {}
        if geom.get("type") == "MultiPolygon":
            try:
                g = shape(geom)
                areas = [(poly.area, poly) for poly in g.geoms]
                total = sum(a for a, _ in areas) or 1.0
                kept = [poly for (a, poly) in areas if (a / total) >= min_ratio]
                if not kept:
                    kept = [max(g.geoms, key=lambda p: p.area)]
                ft["geometry"] = (
                    kept[0].__geo_interface__ if len(kept) == 1
                    else MultiPolygon(kept).__geo_interface__
                )
            except Exception:
                pass
    return geojson

def get_first(props, keys):
    for k in keys:
        v = props.get(k)
        if v not in (None, ""):
            return str(v)
    return None

def province_name_from_province(props):
    keys = [
        "NAME","name","NAME_1","NAME_TR","Il","IL","il","il_adi",
        "province","prov_name","ADMIN_NAME","ADM1_TR","ADM1_EN","ADMIN",
        "name:tr"
    ]
    return get_first(props, keys)

def province_name_from_district(props):
    keys = [
        "İlYeni", "IlYeni", "ilYeni", "il_yeni",
        "ADM1_TR","ADM1_EN","ADM1_NAME","NAME_1",
        "province","prov_name",
        "Il","IL","il","il_adi",
        "ADMIN_NAME_1","PARENT_ADM","PARENT_NAME",
        "ADMIN","name:tr"
    ]
    return get_first(props, keys)

_TMAP = str.maketrans({
    "Ç":"C","Ö":"O","Ş":"S","İ":"I","I":"I","Ü":"U","Ğ":"G",
    "ç":"c","ö":"o","ş":"s","ı":"i","i":"i","ü":"u","ğ":"g"
})
def _normalize_name(s: str) -> str:
    if not s: return ""
    s = re.sub(r"\(.*?\)", " ", s)
    s = s.translate(_TMAP).lower()
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

ALIASES = {
    "icel": "mersin",
    "urfa": "sanliurfa",
    "sanli urfa": "sanliurfa",
    "k maras": "kahramanmaras",
    "kmaras": "kahramanmaras",
    "maras": "kahramanmaras",
}

def ensure_name_field(geojson, name_getter, out_key="NAME"):
    for ft in geojson.get("features", []):
        props = ft.setdefault("properties", {})
        nm = name_getter(props)
        if nm and not props.get(out_key):
            props[out_key] = nm
    return geojson

def colorize_districts(ilce_geojson):
    def _to_float(x):
        if x is None: return None
        if isinstance(x, (int, float)): return float(x)
        s = str(x).strip().replace("%","").replace(",","." )
        try: return float(s)
        except Exception: return None

    party_keys = list(PARTY_COLORS.keys())
    for ft in ilce_geojson.get("features", []):
        props = ft.setdefault("properties", {})
        parti = props.get("SecilenParti") or props.get("Secilen Parti") or props.get("KAZANAN") or props.get("Kazanan")
        if not parti:
            best_party, best_val = None, None
            for p in party_keys:
                val = _to_float(props.get(p))
                if val is None: continue
                if (best_val is None) or (val > best_val):
                    best_val, best_party = val, p
            parti = best_party
        props["color"] = PARTY_COLORS.get(parti, DEFAULT_COLOR)
    return ilce_geojson

# ---------------------------
# Routes: Harita sınırları
# ---------------------------

@app.route("/")
def index():
    return render_template("map.html")

@app.route("/get_boundaries")
@cache.cached(key_prefix="get_boundaries_preprocessed_v1")
def get_boundaries():
    base = os.path.join(app.root_path, "static")
    il_path = os.path.join(base, "updated_il_geojson.json")
    ilce_path = os.path.join(base, "updated_ilce_geojson.json")

    if not os.path.exists(il_path) or not os.path.exists(ilce_path):
        return jsonify({"error": "GeoJSON dosyaları bulunamadı"}), 404

    # Preprocess edilmiş dosyaları olduğu gibi oku
    il = load_geojson(il_path)
    ilce = load_geojson(ilce_path)

    # Güvenlik için sadece poligonları tut
    il = filter_polygons(il)
    ilce = filter_polygons(ilce)

    # Eğer preprocessing sırasında NAME doldurulmadıysa garanti altına al
    il = ensure_name_field(il, province_name_from_province, out_key="NAME")
    ilce = ensure_name_field(ilce, province_name_from_district, out_key="NAME")

    # İllerin renkleri (SecilenParti varsa)
    for ft in il.get("features", []):
        props = ft.setdefault("properties", {})
        parti = props.get("SecilenParti")
        color = PARTY_COLORS.get(parti, DEFAULT_COLOR)
        props["color"] = color

    # İlçe renkleri (parti tahmini de yapar)
    ilce = colorize_districts(ilce)

    return jsonify({
        "il": il,
        "ilce": ilce,
        "party_colors": PARTY_COLORS,
        "default_color": DEFAULT_COLOR
    })

# ---------------------------
# Routes: MARKA noktaları (Excel → JSON)
# ---------------------------

BRANDS_DEFAULT = ["IMANNOOR", "VAKKO", "AKER", "ARMİNE"]

_BRANDS_DF_CACHE = {"mtime": None, "df": None}

def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None

def _normalize_tr(s: str) -> str:
    if s is None: return ""
    s = str(s).strip().lower()
    s = s.replace("ı", "i").replace("İ", "i")
    return s

def _canon_brand(s: str) -> str:
    m = {
        "imannoor":"IMANNOOR", "imannor":"IMANNOOR",
        "vakko":"VAKKO",
        "aker":"AKER",
        "armine":"ARMİNE", "armıne":"ARMİNE"
    }
    key = _normalize_tr(s)
    return m.get(key, s.upper())

def _load_brands_df():
    """Excel'i mtime'a göre cache'ler."""
    if not os.path.exists(DATA_EXCEL_PATH):
        raise FileNotFoundError(f"Excel bulunamadı: {DATA_EXCEL_PATH}")
    mtime = os.path.getmtime(DATA_EXCEL_PATH)
    if _BRANDS_DF_CACHE["df"] is not None and _BRANDS_DF_CACHE["mtime"] == mtime:
        return _BRANDS_DF_CACHE["df"]
    df = pd.read_excel(DATA_EXCEL_PATH)  # sheet_name gerekirse belirt
    _BRANDS_DF_CACHE["df"] = df
    _BRANDS_DF_CACHE["mtime"] = mtime
    return df

@app.route("/get_brands")
@cache.cached(timeout=1800, query_string=True)  # sorgu parametrelerine göre cache
def get_brands():
    """
    Excel'den markaların noktalarını çeker.
    Beklenen/uyarlanan alanlar:
      - Marka: MARKA / FIRMA / BKM_MARKA / BRAND
      - Yer:   BKM_IL_ILCE_NEW / BKM_IL_ILCE / IL_ILCE / ILCE_IL / ILCE
      - Koord: X/LON/LONGITUDE, Y/LAT/LATITUDE
      - Toplam metrikler (opsiyonel): IL_ILCE_CIRO / IL_ILCE_ADET / IL_ILCE_TICKET_SIZE
      - E-Com metrikler (opsiyonel):  IL_ILCE_ECOM_CIRO / IL_ILCE_ECOM_ADET / IL_ILCE_ECOM_TICKET_SIZE
      - Fiziki metrikler (opsiyonel): IL_ILCE_FIZIKI_CIRO / IL_ILCE_FIZIKI_ADET / IL_ILCE_FIZIKI_TICKET_SIZE

    Döndürülen alanlar:
      brand, BKM_IL_ILCE_NEW,
      (toplam) IL_ILCE_CIRO, IL_ILCE_ADET, IL_ILCE_TICKET_SIZE,
      (ecom)   IL_ILCE_ECOM_CIRO, IL_ILCE_ECOM_ADET, IL_ILCE_ECOM_TICKET_SIZE,
      (fiziki) IL_ILCE_FIZIKI_CIRO, IL_ILCE_FIZIKI_ADET, IL_ILCE_FIZIKI_TICKET_SIZE,
      lon, lat
    """
    brands_param = request.args.get("brands", "")
    if brands_param.strip():
        wanted = [x.strip() for x in brands_param.split(",") if x.strip()]
    else:
        wanted = BRANDS_DEFAULT

    try:
        src = _load_brands_df().copy()
    except Exception as e:
        return jsonify({"error": f"Excel okuma hatası: {e}"}), 500

    # Kolon tahminleri
    brand_col = _pick_col(src, ["MARKA","FIRMA","BKM_MARKA","BRAND","Firma","firma"])
    city_col  = _pick_col(src, ["BKM_IL_ILCE_NEW","BKM_IL_ILCE","IL_ILCE","ILCE_IL","ILCE","İL_İLÇE","ILCE-IL"])

    # Eski (toplam) metrik kolonları
    ciro_col_total  = _pick_col(src, ["IL_ILCE_CIRO","CIRO","CİRO","Ciro"])
    adet_col_total  = _pick_col(src, ["IL_ILCE_ADET","ADET","Adet"])
    tsize_col_total = _pick_col(src, ["IL_ILCE_TICKET_SIZE","TICKET_SIZE","Ticket_Size","TicketSize"])

    # Yeni E-COM / FİZİKİ kolonları
    ecom_ciro_col   = _pick_col(src, ["IL_ILCE_ECOM_CIRO","ECOM_CIRO","E-COM_CIRO","ECOM Ciro"])
    ecom_adet_col   = _pick_col(src, ["IL_ILCE_ECOM_ADET","ECOM_ADET","E-COM_ADET","ECOM Adet"])
    ecom_tsize_col  = _pick_col(src, ["IL_ILCE_ECOM_TICKET_SIZE","ECOM_TICKET_SIZE","E-COM_TICKET_SIZE","ECOM Ticket Size"])

    fiz_ciro_col    = _pick_col(src, ["IL_ILCE_FIZIKI_CIRO","FIZIKI_CIRO","FİZİKİ_CİRO","Fiziki Ciro"])
    fiz_adet_col    = _pick_col(src, ["IL_ILCE_FIZIKI_ADET","FIZIKI_ADET","FİZİKİ_ADET","Fiziki Adet"])
    fiz_tsize_col   = _pick_col(src, ["IL_ILCE_FIZIKI_TICKET_SIZE","FIZIKI_TICKET_SIZE","FİZİKİ_TICKET_SIZE","Fiziki Ticket Size"])

    # Koordinatlar
    x_col     = _pick_col(src, ["X","Lon","LON","Longitude","LONGITUDE","X_KOORDINAT"])
    y_col     = _pick_col(src, ["Y","Lat","LAT","Latitude","LATITUDE","Y_KOORDINAT"])

    if not brand_col or not city_col or not x_col or not y_col:
        return jsonify({"error": f"Excel kolonları eksik. Bulunanlar: {list(src.columns)}"}), 400

    # Standardize isimler
    rename_map = {
        brand_col: "MARKA",
        city_col: "BKM_IL_ILCE_NEW",
        x_col: "X",
        y_col: "Y",
    }
    if ciro_col_total:  rename_map[ciro_col_total]  = "IL_ILCE_CIRO"
    if adet_col_total:  rename_map[adet_col_total]  = "IL_ILCE_ADET"
    if tsize_col_total: rename_map[tsize_col_total] = "IL_ILCE_TICKET_SIZE"

    if ecom_ciro_col:   rename_map[ecom_ciro_col]   = "IL_ILCE_ECOM_CIRO"
    if ecom_adet_col:   rename_map[ecom_adet_col]   = "IL_ILCE_ECOM_ADET"
    if ecom_tsize_col:  rename_map[ecom_tsize_col]  = "IL_ILCE_ECOM_TICKET_SIZE"

    if fiz_ciro_col:    rename_map[fiz_ciro_col]    = "IL_ILCE_FIZIKI_CIRO"
    if fiz_adet_col:    rename_map[fiz_adet_col]    = "IL_ILCE_FIZIKI_ADET"
    if fiz_tsize_col:   rename_map[fiz_tsize_col]   = "IL_ILCE_FIZIKI_TICKET_SIZE"

    df = src.rename(columns=rename_map)

    # Marka canonical
    df["MARKA_CANON"] = df["MARKA"].apply(_canon_brand)

    # Sayısal tip
    for col in [
        "X","Y",
        "IL_ILCE_CIRO","IL_ILCE_ADET","IL_ILCE_TICKET_SIZE",
        "IL_ILCE_ECOM_CIRO","IL_ILCE_ECOM_ADET","IL_ILCE_ECOM_TICKET_SIZE",
        "IL_ILCE_FIZIKI_CIRO","IL_ILCE_FIZIKI_ADET","IL_ILCE_FIZIKI_TICKET_SIZE"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Koordinatlar geçerli mi?
    df = df.dropna(subset=["X","Y"])
    df = df[(df["X"].between(-180, 180)) & (df["Y"].between(-90, 90))]

    # Filtre: sadece istenen markalar
    df = df[df["MARKA_CANON"].isin(wanted)].copy()

    # Çıkış
    records = []
    for _, r in df.iterrows():
        records.append({
            "brand": str(r.get("MARKA_CANON", "")),
            "BKM_IL_ILCE_NEW": str(r.get("BKM_IL_ILCE_NEW", "")),

            # Toplam (opsiyonel)
            "IL_ILCE_CIRO": float(r["IL_ILCE_CIRO"]) if "IL_ILCE_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_CIRO")) else Non    e,
            "IL_ILCE_ADET": float(r["IL_ILCE_ADET"]) if "IL_ILCE_ADET" in df.columns and pd.notna(r.get("IL_ILCE_ADET")) else None,
            "IL_ILCE_TICKET_SIZE": float(r["IL_ILCE_TICKET_SIZE"]) if "IL_ILCE_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_TICKET_SIZE")) else None,

            # E-Com (opsiyonel)
            "IL_ILCE_ECOM_CIRO": float(r["IL_ILCE_ECOM_CIRO"]) if "IL_ILCE_ECOM_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_CIRO")) else None,
            "IL_ILCE_ECOM_ADET": float(r["IL_ILCE_ECOM_ADET"]) if "IL_ILCE_ECOM_ADET" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_ADET")) else None,
            "IL_ILCE_ECOM_TICKET_SIZE": float(r["IL_ILCE_ECOM_TICKET_SIZE"]) if "IL_ILCE_ECOM_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_TICKET_SIZE")) else None,

            # Fiziki (opsiyonel)
            "IL_ILCE_FIZIKI_CIRO": float(r["IL_ILCE_FIZIKI_CIRO"]) if "IL_ILCE_FIZIKI_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_CIRO")) else None,
            "IL_ILCE_FIZIKI_ADET": float(r["IL_ILCE_FIZIKI_ADET"]) if "IL_ILCE_FIZIKI_ADET" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_ADET")) else None,
            "IL_ILCE_FIZIKI_TICKET_SIZE": float(r["IL_ILCE_FIZIKI_TICKET_SIZE"]) if "IL_ILCE_FIZIKI_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_TICKET_SIZE")) else None,

            "lon": float(r["X"]),
            "lat": float(r["Y"]),
        })

    return jsonify({"points": records, "brands": wanted})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=84, debug=True, use_reloader=False)
