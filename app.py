# app.py
from flask import Flask, render_template, jsonify, request
from flask_caching import Cache
import os, json, re, difflib
from collections import defaultdict
from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union
from shapely.validation import make_valid  # Shapely 2.x

# Excel
import pandas as pd

app = Flask(__name__)
app.config['CACHE_TYPE'] = 'simple'
app.config['CACHE_DEFAULT_TIMEOUT'] = 3600
cache = Cache(app)

# Parti renkleri
PARTY_COLORS = {
    "AK Parti": "#f39c12",
    "CHP": "#FF0000",
    "MHP": "#2980b9",
    "İYİ Parti": "#FFEB3B",
    "DEM Parti": "#6c3483",
    "Büyük Birlik": "#4A4A4A",
    "Yeniden Refah": "#d35400",
}
DEFAULT_COLOR = "#f8f9f9"

# Excel dosya yolu
DATA_EXCEL_PATH = os.path.join(app.root_path, "static", "BKM_MARKA_CIROLAR.xlsx")

# ---------------------------
# Geo yardımcıları
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

def _keep_significant_parts_geom(geom, min_ratio=0.003):
    try:
        if geom.geom_type == "MultiPolygon":
            parts = list(geom.geoms)
            areas = [p.area for p in parts]
            total = sum(areas) or 1.0
            kept = [p for p, a in zip(parts, areas) if (a/total) >= min_ratio]
            if not kept:
                kept = [max(parts, key=lambda g: g.area)]
            return kept[0] if len(kept) == 1 else MultiPolygon(kept)
        return geom
    except Exception:
        return geom

def get_first(props, keys):
    for k in keys:
        v = props.get(k)
        if v not in (None, ""):
            return str(v)
    return None

def province_name_from_province(props):
    # name:tr öncelikli
    keys = [
        "name:tr",
        "NAME","name","NAME_1","NAME_TR","Il","IL","il","il_adi",
        "province","prov_name","ADMIN_NAME","ADM1_TR","ADM1_EN","ADMIN"
    ]
    return get_first(props, keys)

def province_name_from_district(props):
    keys = [
        "İlYeni","IlYeni","ilYeni","il_yeni",
        "ADM1_TR","ADM1_EN","ADM1_NAME","NAME_1",
        "province","prov_name",
        "Il","IL","il","il_adi",
        "ADMIN_NAME_1","PARENT_ADM","PARENT_NAME",
        "ADMIN","name:tr",
        "İl",  # noktalı büyük İ
        "ILCE_IL","ilce_il","Ilce_Il","il_ilce",
        "IL_ADI","IL_AD","Sehir","Şehir","SEHIR",
        "PARENT","PROVINCE","PROVINCIA"
    ]
    return get_first(props, keys)

_TMAP = str.maketrans({
    "Ç":"C","Ö":"O","Ş":"S","İ":"I","I":"I","Ü":"U","Ğ":"G",
    "ç":"c","ö":"o","ş":"s","ı":"i","i":"i","ü":"u","ğ":"g",
    "â":"a","Â":"A","ê":"e","Ê":"E","î":"i","Î":"I","ô":"o","Ô":"O","û":"u","Û":"U"
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
    "elazig": "elazig",  # normalize güvence
    "hakkari": "hakkari",
}

def ensure_name_field(geojson, name_getter, out_key="NAME"):
    for ft in geojson.get("features", []):
        props = ft.setdefault("properties", {})
        nm = name_getter(props)
        if nm and not props.get(out_key):
            props[out_key] = nm
    return geojson

def dedupe_provinces(il_geojson):
    buckets = defaultdict(list)
    for ft in il_geojson.get("features", []):
        props = ft.get("properties", {}) or {}
        nm = province_name_from_province(props) or ""
        key = _normalize_name(nm)
        buckets[key].append(ft)

    new_features = []
    for key, fts in buckets.items():
        if len(fts) == 1:
            new_features.append(fts[0]); continue
        geoms = [shape(ft["geometry"]) for ft in fts if ft.get("geometry")]
        union_geom = unary_union(geoms) if geoms else None
        base_props = (fts[0].get("properties", {}) or {}).copy()
        for ft in fts:
            p = ft.get("properties", {}) or {}
            if p.get("SecilenParti"):
                base_props["SecilenParti"] = p["SecilenParti"]; break
        new_features.append({
            "type": "Feature",
            "properties": base_props,
            "geometry": union_geom.__geo_interface__ if union_geom else None
        })
    return {"type": "FeatureCollection", "features": new_features}

# ---- Robust geometry ops ----
def _to_valid(geom):
    try:
        g2 = make_valid(geom)
        if g2.is_empty:
            # fallback
            g2 = geom.buffer(0)
        return g2
    except Exception:
        try:
            return geom.buffer(0)
        except Exception:
            return geom  # son çare

def _safe_unary_union(geoms):
    cleaned = []
    dropped = 0
    for g in geoms:
        try:
            vg = _to_valid(g)
            if not vg.is_empty:
                cleaned.append(vg)
            else:
                dropped += 1
        except Exception:
            dropped += 1
    if not cleaned:
        return None, dropped
    try:
        return unary_union(cleaned), dropped
    except Exception:
        # stepwise union fallback
        from shapely.ops import unary_union as uu
        acc = cleaned[0]
        for g in cleaned[1:]:
            try:
                acc = uu([acc, g])
            except Exception:
                try:
                    acc = _to_valid(acc).union(_to_valid(g))
                except Exception:
                    pass
        return acc, dropped

def update_province_boundaries(il_geojson, ilce_geojson):
    # Province canonical set
    canon_norm_to_orig = {}
    for pft in il_geojson.get("features", []):
        p_name = province_name_from_province(pft.get("properties", {}) or {})
        if not p_name:
            continue
        norm = _normalize_name(p_name)
        norm = ALIASES.get(norm, norm)
        canon_norm_to_orig[norm] = p_name
    canon_keys = list(canon_norm_to_orig.keys())

    # Group districts by province
    grouped = {}
    for dft in ilce_geojson.get("features", []):
        props = dft.get("properties", {}) or {}
        raw = province_name_from_district(props)
        if not raw:
            continue
        norm = _normalize_name(raw)
        norm = ALIASES.get(norm, norm)
        if norm not in canon_norm_to_orig:
            matches = difflib.get_close_matches(norm, canon_keys, n=1, cutoff=0.75)
            if matches:
                norm = matches[0]
        if norm in canon_norm_to_orig:
            grouped.setdefault(norm, []).append(dft)

    print("[DEBUG] grouped district counts (by province norm):",
          {k: len(v) for k, v in grouped.items()})

    updated = 0; skipped = []; invalid_counts = {}
    for pft in il_geojson.get("features", []):
        p_name = province_name_from_province(pft.get("properties", {}) or {})
        if not p_name:
            continue
        p_norm = _normalize_name(p_name)
        p_norm = ALIASES.get(p_norm, p_norm)
        dlist = grouped.get(p_norm)
        if not dlist:
            skipped.append(p_name); continue

        geoms = []
        for dft in dlist:
            dgeom = dft.get("geometry")
            if not dgeom: continue
            try:
                geoms.append(shape(dgeom))
            except Exception:
                pass
        if not geoms:
            skipped.append(p_name); continue

        try:
            union_geom, dropped = _safe_unary_union(geoms)
            invalid_counts[p_name] = dropped
            if union_geom is None or union_geom.is_empty:
                skipped.append(p_name); continue

            # ORİJİNAL İL POLİGONUYLA CLIP
            orig = None
            try:
                if pft.get("geometry"):
                    orig = _to_valid(shape(pft["geometry"]))
            except Exception:
                orig = None
            if orig:
                try:
                    union_geom = _to_valid(union_geom).intersection(orig)
                except Exception:
                    union_geom = _to_valid(union_geom)

            # küçük parçaları at
            union_geom = _keep_significant_parts_geom(union_geom, min_ratio=0.003)

            pft["geometry"] = union_geom.__geo_interface__
            updated += 1
        except Exception:
            skipped.append(p_name)

    il_geojson = keep_significant_components(il_geojson, min_ratio=0.003)
    print(f"[update_province_boundaries] updated={updated}, skipped={len(skipped)}")
    if invalid_counts:
        print("[DEBUG] invalid district parts dropped per province:", invalid_counts)
    if skipped:
        print("[skipped provinces]", skipped)
    return il_geojson

def colorize_districts(ilce_geojson):
    def _to_float(x):
        if x is None: return None
        if isinstance(x, (int,float)): return float(x)
        s = str(x).strip().replace("%","").replace(",",".")
        try: return float(s)
        except: return None

    party_keys = list(PARTY_COLORS.keys())
    for ft in ilce_geojson.get("features", []):
        props = ft.setdefault("properties", {})
        parti = props.get("SecilenParti") or props.get("Secilen Parti") \
                or props.get("KAZANAN") or props.get("Kazanan")
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
@cache.cached(key_prefix="get_boundaries_v12")  # cache kır
def get_boundaries():
    base = os.path.join(app.root_path, "static")
    il_path = os.path.join(base, "updated_il_geojson.json")
    ilce_path = os.path.join(base, "updated_ilce_geojson.json")

    if not os.path.exists(il_path) or not os.path.exists(ilce_path):
        return jsonify({"error": "GeoJSON dosyaları bulunamadı"}), 404

    il = load_geojson(il_path)
    ilce = load_geojson(ilce_path)

    # Örnek property anahtarları
    sample_il = [list((f.get("properties") or {}).keys()) for f in il.get("features", [])[:3]]
    sample_ilce = [list((f.get("properties") or {}).keys()) for f in ilce.get("features", [])[:3]]
    print("[DEBUG] Province sample keys:", sample_il)
    print("[DEBUG] District sample keys:", sample_ilce)

    il = filter_polygons(il)
    ilce = filter_polygons(ilce)

    # DİKKAT: deniz taşmasını artırdığı için kapalı
    # ilce = crop_to_largest_component(ilce)

    il = update_province_boundaries(il, ilce)
    il = dedupe_provinces(il)

    il = ensure_name_field(il, province_name_from_province, out_key="NAME")
    ilce = ensure_name_field(ilce, province_name_from_district, out_key="NAME")

    for ft in il.get("features", []):
        props = ft.setdefault("properties", {})
        parti = props.get("SecilenParti")
        props["color"] = PARTY_COLORS.get(parti, DEFAULT_COLOR)

    ilce = colorize_districts(ilce)

    total = len(il.get("features", []))
    skipped_provs = [ft.get("properties", {}).get("NAME") for ft in il.get("features", []) if not ft.get("geometry")]
    print(f"[DEBUG] Provinces total={total}, skipped={len(skipped_provs)}")
    if skipped_provs:
        print("[DEBUG] Skipped provinces list:", skipped_provs)

    return jsonify({"il": il, "ilce": ilce, "party_colors": PARTY_COLORS, "default_color": DEFAULT_COLOR})

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
    s = s.replace("ı","i").replace("İ","i")
    return s

def _canon_brand(s: str) -> str:
    m = {
        "imannoor":"IMANNOOR","imannor":"IMANNOOR",
        "vakko":"VAKKO",
        "aker":"AKER",
        "armine":"ARMİNE","armıne":"ARMİNE"
    }
    key = _normalize_tr(s)
    return m.get(key, s.upper())

def _load_brands_df():
    if not os.path.exists(DATA_EXCEL_PATH):
        raise FileNotFoundError(f"Excel bulunamadı: {DATA_EXCEL_PATH}")
    mtime = os.path.getmtime(DATA_EXCEL_PATH)
    if _BRANDS_DF_CACHE["df"] is not None and _BRANDS_DF_CACHE["mtime"] == mtime:
        return _BRANDS_DF_CACHE["df"]
    df = pd.read_excel(DATA_EXCEL_PATH)
    _BRANDS_DF_CACHE["df"] = df
    _BRANDS_DF_CACHE["mtime"] = mtime
    return df

@app.route("/get_brands")
@cache.cached(timeout=1800, query_string=True)
def get_brands():
    brands_param = request.args.get("brands", "")
    if brands_param.strip():
        wanted = [x.strip() for x in brands_param.split(",") if x.strip()]
    else:
        wanted = BRANDS_DEFAULT

    try:
        src = _load_brands_df().copy()
    except Exception as e:
        print("[DEBUG] Excel okuma hatası:", e)
        return jsonify({"error": f"Excel okuma hatası: {e}"}), 500

    # Kolon tahminleri
    brand_col = _pick_col(src, ["MARKA","FIRMA","BKM_MARKA","BRAND","Firma","firma"])
    city_col  = _pick_col(src, ["BKM_IL_ILCE_NEW","BKM_IL_ILCE","IL_ILCE","ILCE_IL","ILCE","İL_İLÇE","ILCE-IL"])

    ciro_col_total  = _pick_col(src, ["IL_ILCE_CIRO","CIRO","CİRO","Ciro"])
    adet_col_total  = _pick_col(src, ["IL_ILCE_ADET","ADET","Adet"])
    tsize_col_total = _pick_col(src, ["IL_ILCE_TICKET_SIZE","TICKET_SIZE","Ticket_Size","TicketSize"])

    ecom_ciro_col   = _pick_col(src, ["IL_ILCE_ECOM_CIRO","ECOM_CIRO","E-COM_CIRO","ECOM Ciro"])
    ecom_adet_col   = _pick_col(src, ["IL_ILCE_ECOM_ADET","ECOM_ADET","E-COM_ADET","ECOM Adet"])
    ecom_tsize_col  = _pick_col(src, ["IL_ILCE_ECOM_TICKET_SIZE","ECOM_TICKET_SIZE","E-COM_TICKET_SIZE","ECOM Ticket Size"])

    fiz_ciro_col    = _pick_col(src, ["IL_ILCE_FIZIKI_CIRO","FIZIKI_CIRO","FİZİKİ_CİRO","Fiziki Ciro"])
    fiz_adet_col    = _pick_col(src, ["IL_ILCE_FIZIKI_ADET","FIZIKI_ADET","FİZİKİ_ADET","Fiziki Adet"])
    fiz_tsize_col   = _pick_col(src, ["IL_ILCE_FIZIKI_TICKET_SIZE","FIZIKI_TICKET_SIZE","FİZİKİ_TICKET_SIZE","Fiziki Ticket Size"])

    x_col = _pick_col(src, ["X","Lon","LON","Longitude","LONGITUDE","X_KOORDINAT"])
    y_col = _pick_col(src, ["Y","Lat","LAT","Latitude","LATITUDE","Y_KOORDINAT"])

    if not brand_col or not city_col or not x_col or not y_col:
        print("[DEBUG] Excel kolonları eksik:", list(src.columns))
        return jsonify({"error": f"Excel kolonları eksik. Bulunanlar: {list(src.columns)}"}), 400

    rename_map = { brand_col:"MARKA", city_col:"BKM_IL_ILCE_NEW", x_col:"X", y_col:"Y" }
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

    df["MARKA_CANON"] = df["MARKA"].apply(_canon_brand)

    for col in [
        "X","Y",
        "IL_ILCE_CIRO","IL_ILCE_ADET","IL_ILCE_TICKET_SIZE",
        "IL_ILCE_ECOM_CIRO","IL_ILCE_ECOM_ADET","IL_ILCE_ECOM_TICKET_SIZE",
        "IL_ILCE_FIZIKI_CIRO","IL_ILCE_FIZIKI_ADET","IL_ILCE_FIZIKI_TICKET_SIZE"
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["X","Y"])
    df = df[(df["X"].between(-180, 180)) & (df["Y"].between(-90, 90))]

    df = df[df["MARKA_CANON"].isin(wanted)].copy()

    records = []
    for _, r in df.iterrows():
        records.append({
            "brand": str(r.get("MARKA_CANON", "")),
            "BKM_IL_ILCE_NEW": str(r.get("BKM_IL_ILCE_NEW", "")),
            "IL_ILCE_CIRO": float(r["IL_ILCE_CIRO"]) if "IL_ILCE_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_CIRO")) else None,
            "IL_ILCE_ADET": float(r["IL_ILCE_ADET"]) if "IL_ILCE_ADET" in df.columns and pd.notna(r.get("IL_ILCE_ADET")) else None,
            "IL_ILCE_TICKET_SIZE": float(r["IL_ILCE_TICKET_SIZE"]) if "IL_ILCE_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_TICKET_SIZE")) else None,
            "IL_ILCE_ECOM_CIRO": float(r["IL_ILCE_ECOM_CIRO"]) if "IL_ILCE_ECOM_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_CIRO")) else None,
            "IL_ILCE_ECOM_ADET": float(r["IL_ILCE_ECOM_ADET"]) if "IL_ILCE_ECOM_ADET" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_ADET")) else None,
            "IL_ILCE_ECOM_TICKET_SIZE": float(r["IL_ILCE_ECOM_TICKET_SIZE"]) if "IL_ILCE_ECOM_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_ECOM_TICKET_SIZE")) else None,
            "IL_ILCE_FIZIKI_CIRO": float(r["IL_ILCE_FIZIKI_CIRO"]) if "IL_ILCE_FIZIKI_CIRO" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_CIRO")) else None,
            "IL_ILCE_FIZIKI_ADET": float(r["IL_ILCE_FIZIKI_ADET"]) if "IL_ILCE_FIZIKI_ADET" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_ADET")) else None,
            "IL_ILCE_FIZIKI_TICKET_SIZE": float(r["IL_ILCE_FIZIKI_TICKET_SIZE"]) if "IL_ILCE_FIZIKI_TICKET_SIZE" in df.columns and pd.notna(r.get("IL_ILCE_FIZIKI_TICKET_SIZE")) else None,
            "lon": float(r["X"]),
            "lat": float(r["Y"]),
        })

    return jsonify({"points": records, "brands": wanted})

# ---------------------------
# Run
# ---------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
