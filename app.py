from flask import Flask, render_template, jsonify, request
from flask_caching import Cache
import os, json, re, difflib
from collections import defaultdict
from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union

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

DATA_EXCEL_PATH = os.path.join(app.root_path, "static", "BKM_MARKA_CIROLAR.xlsx")

# ---------------------------
# Helpers
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

def crop_to_largest_component(geojson):
    for feature in geojson.get("features", []):
        geom = feature.get("geometry") or {}
        if geom.get("type") == "MultiPolygon":
            try:
                shapely_geom = shape(geom)
                if getattr(shapely_geom, "geoms", None):
                    largest = max(shapely_geom.geoms, key=lambda a: a.area)
                    feature["geometry"] = largest.__geo_interface__
            except Exception:
                pass
    return geojson

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
    keys = ["NAME","name","NAME_1","NAME_TR","Il","IL","il","il_adi",
            "province","prov_name","ADMIN_NAME","ADM1_TR","ADM1_EN","ADMIN","name:tr"]
    return get_first(props, keys)

def province_name_from_district(props):
    keys = [
        "İlYeni","IlYeni","ilYeni","il_yeni",
        "ADM1_TR","ADM1_EN","ADM1_NAME","NAME_1",
        "province","prov_name",
        "Il","IL","il","il_adi",
        "ADMIN_NAME_1","PARENT_ADM","PARENT_NAME",
        "ADMIN","name:tr",
        # Ek anahtarlar
        "İl", "ILCE_IL","ilce_il","Ilce_Il","il_ilce",
        "IL_ADI","IL_AD","Sehir","Şehir","SEHIR",
        "PARENT","PROVINCE","PROVINCIA"
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
    "icel":"mersin","urfa":"sanliurfa","sanli urfa":"sanliurfa",
    "k maras":"kahramanmaras","kmaras":"kahramanmaras","maras":"kahramanmaras",
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
            "type":"Feature","properties":base_props,
            "geometry": union_geom.__geo_interface__ if union_geom else None
        })
    return {"type":"FeatureCollection","features":new_features}

def update_province_boundaries(il_geojson, ilce_geojson):
    canon_norm_to_orig = {}
    for pft in il_geojson.get("features", []):
        p_name = province_name_from_province(pft.get("properties", {}) or {})
        if not p_name: continue
        norm = _normalize_name(p_name)
        norm = ALIASES.get(norm, norm)
        canon_norm_to_orig[norm] = p_name
    canon_keys = list(canon_norm_to_orig.keys())

    grouped = {}
    for dft in ilce_geojson.get("features", []):
        props = dft.get("properties", {}) or {}
        raw = province_name_from_district(props)
        if not raw: continue
        norm = _normalize_name(raw)
        norm = ALIASES.get(norm, norm)
        if norm not in canon_norm_to_orig:
            import difflib
            matches = difflib.get_close_matches(norm, canon_keys, n=1, cutoff=0.75)
            if matches: norm = matches[0]
        if norm in canon_norm_to_orig:
            grouped.setdefault(norm, []).append(dft)

    updated = 0; skipped = []
    for pft in il_geojson.get("features", []):
        p_name = province_name_from_province(pft.get("properties", {}) or {})
        if not p_name: continue
        p_norm = _normalize_name(p_name)
        p_norm = ALIASES.get(p_norm, p_norm)
        if p_norm not in grouped:
            skipped.append(p_name); continue
        geoms = []
        for dft in grouped[p_norm]:
            dgeom = dft.get("geometry")
            if not dgeom: continue
            try: geoms.append(shape(dgeom))
            except Exception: pass
        if geoms:
            try:
                union_geom = unary_union(geoms)
                pft["geometry"] = union_geom.__geo_interface__; updated += 1
            except Exception: skipped.append(p_name)

    il_geojson = keep_significant_components(il_geojson, min_ratio=0.003)
    print(f"[update_province_boundaries] updated={updated}, skipped={len(skipped)}")
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
# Routes
# ---------------------------

@app.route("/")
def index():
    return render_template("map.html")

@app.route("/get_boundaries")
@cache.cached(key_prefix="get_boundaries_v9")
def get_boundaries():
    base = os.path.join(app.root_path, "static")
    il_path = os.path.join(base, "updated_il_geojson.json")
    ilce_path = os.path.join(base, "updated_ilce_geojson.json")

    if not os.path.exists(il_path) or not os.path.exists(ilce_path):
        return jsonify({"error":"GeoJSON dosyaları bulunamadı"}),404

    il = load_geojson(il_path)
    ilce = load_geojson(ilce_path)

    # Debug: property anahtarlarını göster
    sample_il = [list((f.get("properties") or {}).keys()) for f in il.get("features", [])[:3]]
    sample_ilce = [list((f.get("properties") or {}).keys()) for f in ilce.get("features", [])[:3]]
    print("[DEBUG] Province sample keys:", sample_il)
    print("[DEBUG] District sample keys:", sample_ilce)

    il = filter_polygons(il)
    ilce = filter_polygons(ilce)
    ilce = crop_to_largest_component(ilce)

    il = update_province_boundaries(il, ilce)
    il = dedupe_provinces(il)

    il = ensure_name_field(il, province_name_from_province, out_key="NAME")
    ilce = ensure_name_field(ilce, province_name_from_district, out_key="NAME")

    for ft in il.get("features", []):
        props = ft.setdefault("properties", {})
        parti = props.get("SecilenParti")
        props["color"] = PARTY_COLORS.get(parti, DEFAULT_COLOR)

    ilce = colorize_districts(ilce)

    # Kaç il skip edilmiş logla
    total = len(il.get("features", []))
    skipped = [ft.get("properties",{}).get("NAME") for ft in il.get("features",[]) if not ft.get("geometry")]
    print(f"[DEBUG] Provinces total={total}, skipped={len(skipped)}")
    if skipped:
        print("[DEBUG] Skipped provinces list:", skipped)

    return jsonify({"il":il,"ilce":ilce,"party_colors":PARTY_COLORS,"default_color":DEFAULT_COLOR})

# ---------------------------
# Brands endpoint (aynı kaldı)
# ---------------------------
# ... (get_brands fonksiyonu aynen sende olduğu gibi kalabilir)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
