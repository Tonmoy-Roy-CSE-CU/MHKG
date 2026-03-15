"""
MHKG OLAP Interface  v5.2  —  CovKG BIKE interaction model
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
KEY CHANGE vs v5.1:
  The dim-tree is now fully interactive, matching CovKG BIKE exactly.

  LEFT PANEL — "Structure Extraction"
    • Each dimension level row is a clickable button.
      - Single-click  → sets it as the active filtering level (blue highlight,
                         shown in middle panel as "Selected level: mhp:X")
      - Double-click  → ALSO adds it to the Group-By list (green highlight)
        (implemented as: click once to focus → a separate ✚ Add button adds it
         to summary, keeping the UX clean inside Dash's callback model)
      • Each measure row is also clickable → selects that measure in Summary.
      • Selected levels shown with green background + checkmark.
      • Active (focused-for-filtering) level shown with blue border.

  MIDDLE PANEL — "Instance Filtering"
    • Top label: "Selected level: mhp:<level>"  (exactly like CovKG)
    • Attribute dropdown auto-populated from LEVEL_ATTR_MAP.
    • Instance checklist with search, Select All / Clear All.
    • "+ Add to Group-By" button adds the active level to Summary levels.

  RIGHT PANEL — "Selection Summary"
    • Measures sub-card (red border):
        - Selected measure name
        - RadioItems for agg function (like CovKG's radio buttons)
    • Levels sub-card (green border):
        - Each added level as a removable pill card
        - Shows selected instances as badges
    • Active Filters sub-card (blue border) listing all filters.

  All v5.1 backend fixes (BUG-1…BUG-8) retained.

INSTALL:
  pip install dash dash-bootstrap-components rdflib pandas plotly flask

RUN:
  python mhkg_olap_patched.py  →  http://127.0.0.1:8050
"""

import re, io, os, base64, time, uuid, tempfile, threading, gc

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import flask

import dash
from dash import dcc, html, dash_table, Input, Output, State, ctx, no_update, ALL, MATCH
import dash_bootstrap_components as dbc
from rdflib import Graph, Namespace, RDF, RDFS, OWL, XSD, URIRef, Literal

# ══════════════════════════════════════════════════════════════════════════════
#  NAMESPACES
# ══════════════════════════════════════════════════════════════════════════════
MHP  = Namespace("http://mhkg.example.com/datasets/mentalhealth/abox/mdProperty#")
MHS  = Namespace("http://mhkg.example.com/datasets/mentalhealth/abox/mdStructure#")
MHA  = Namespace("http://mhkg.example.com/datasets/mentalhealth/abox/mdAttribute#")
ONTO = Namespace("http://mhkg.example.com/datasets/mentalhealth/abox/")
DATA = Namespace("http://mhkg.example.com/datasets/mentalhealth/abox/data#")
QB   = Namespace("http://purl.org/linked-data/cube#")
QB4O = Namespace("http://purl.org/qb4olap/cubes#")

# ══════════════════════════════════════════════════════════════════════════════
#  LEVEL → mha: attribute map
# ══════════════════════════════════════════════════════════════════════════════
LEVEL_ATTR_MAP: dict[str, str] = {
    "All":            "allName",
    "Continent":      "continentName",
    "Region":         "regionName",
    "Country":        "countryName",
    "GenderCategory": "genderCategory",
    "Gender":         "gender",
    "LifeStage":      "lifeStageName",
    "AgeGroup":       "ageGroupRange",
    "University":     "universityName",
    "AcademicYear":   "academicYearName",
    "CGPACategory":   "cgpaCategory",
    "Occupation":     "occupationName",
    "SleepQuality":   "sleepQuality",
    "SupportSystem":  "socialSupportAttr",
    "Year":           "yearValue",
    "Quarter":        "quarterValue",
    "Month":          "monthValue",
    "Week":           "weekValue",
    "Day":            "dateValue",
}

ALL_KNOWN_LEVELS = [
    "Country","Gender","AgeGroup","CGPACategory","AcademicYear",
    "Occupation","SleepQuality","SupportSystem","Year",
    "Continent","Region","GenderCategory","LifeStage","University",
    "Quarter","Month","Week","Day",
]

# Dimension groupings for the tree
# Format: dim_id → (dim_label, hierarchy_label, [levels in order from top to bottom])
DIM_GROUPS: dict[str, tuple[str, str, list[str]]] = {
    "geographyDim":     ("Geography",        "geographyHierarchy",   ["Continent","Region","Country"]),
    "demographicsDim":  ("Demographics",     "demographicsHierarchy",["GenderCategory","Gender","LifeStage","AgeGroup"]),
    "academicDim":      ("Academic",         "academicHierarchy",    ["University","AcademicYear","CGPACategory"]),
    "workEnvDim":       ("Work Environment", "workEnvHierarchy",     ["Occupation"]),
    "lifestyleDim":     ("Lifestyle",        "lifestyleHierarchy",   ["SleepQuality"]),
    "supportSystemDim": ("Support System",   "supportHierarchy",     ["SupportSystem"]),
    "timeDim":          ("Time",             "timeHierarchy",        ["Year","Quarter","Month","Week","Day"]),
}

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE
# ══════════════════════════════════════════════════════════════════════════════
_lock  = threading.Lock()
_state: dict = {
    "tbox": None, "obs_df": None, "dim_groups": {},
    "tbox_name": None, "abox_name": None,
    "loading": False, "load_progress": "", "load_pct": 0,
    "datasets": [], "parse_start": 0, "obs_done": 0, "obs_total": 0,
    "last_sparql": "",
    "_chunk_path": None, "_chunk_bytes": 0, "_chunk_total": 0,
}

# ══════════════════════════════════════════════════════════════════════════════
#  TTL SANITIZER
# ══════════════════════════════════════════════════════════════════════════════
_RE_PFXDECL   = re.compile(r'@prefix\s+(\w*):\s+<([^>]+)>\s*\.', re.IGNORECASE)
_RE_BAD_PAREN = re.compile(r'\b([A-Za-z]\w+):([\w\-\.%]*\([\w\-\.%\(\)]*\)[\w\-\.%\(\)]*)')

def _safe_decode(raw: bytes) -> str:
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        try: return raw.decode(enc)
        except (UnicodeDecodeError, LookupError): continue
    return raw.decode("utf-8", errors="replace")

def sanitize_ttl(raw_bytes: bytes) -> str:
    text = _safe_decode(raw_bytes).replace("\r\n","\n").replace("\r","\n")
    prefixes = {m.group(1): m.group(2) for m in _RE_PFXDECL.finditer(text)}
    def _expand(m, _p=prefixes):
        base = _p.get(m.group(1))
        return f"<{base}{m.group(2)}>" if base else m.group(0)
    fixed = []
    for line in text.split("\n"):
        s = line.strip()
        if s.startswith("#"):
            fixed.append(line.encode("ascii", errors="replace").decode("ascii"))
        elif s.lower().startswith("@prefix"):
            fixed.append(line)
        else:
            fixed.append(_RE_BAD_PAREN.sub(_expand, line))
    return "\n".join(fixed)

# ══════════════════════════════════════════════════════════════════════════════
#  TBOX PARSER
# ══════════════════════════════════════════════════════════════════════════════
def _localname(uri: str) -> str:
    return str(uri).split("#")[-1].split("/")[-1]

def _label(g, uri) -> str:
    for o in g.objects(uri, RDFS.label):
        s = str(o)
        return s[:s.rfind("@")].strip('"').strip() if "@" in s else s.strip('"').strip()
    return _localname(str(uri))

def _topo_sort_levels(steps):
    from collections import defaultdict, deque
    children_of = defaultdict(list)
    has_parent = set()
    all_nodes = set()
    for child, parent in steps:
        if parent == "All":
            all_nodes.add(child); continue
        if child == "All":
            continue
        children_of[parent].append(child)
        has_parent.add(child)
        all_nodes.update([child, parent])
    roots = [n for n in all_nodes if n not in has_parent]
    order = []
    visited = set()
    q = deque(roots)
    while q:
        node = q.popleft()
        if node in visited: continue
        visited.add(node); order.append(node)
        for ch in children_of.get(node, []):
            q.append(ch)
    order.reverse()
    return order

def parse_tbox(raw: bytes) -> dict:
    clean = sanitize_ttl(raw)
    g = Graph()
    try:
        g.parse(io.StringIO(clean), format="turtle")
    except Exception as e1:
        try:
            g = Graph(); g.parse(io.StringIO(clean), format="n3")
        except Exception as e2:
            raise ValueError(f"TBox parse failed.\nTurtle: {e1}\nN3: {e2}") from e2

    dim_hier_steps = {}
    for step in g.subjects(RDF.type, QB4O.HierarchyStep):
        hier  = next(iter(g.objects(step, QB4O.inHierarchy)), None)
        if hier is None: continue
        dim   = next(iter(g.objects(hier, QB4O.inDimension)), None)
        child_lv  = next(iter(g.objects(step, QB4O.childLevel)),  None)
        parent_lv = next(iter(g.objects(step, QB4O.parentLevel)), None)
        if not all([dim, child_lv, parent_lv]): continue
        d, h = str(dim), str(hier)
        dim_hier_steps.setdefault(d, {}).setdefault(h, []).append(
            (_localname(str(child_lv)), _localname(str(parent_lv)))
        )

    tbox_dim_groups = {}
    for dim in g.subjects(RDF.type, QB.DimensionProperty):
        dim_str = str(dim)
        dim_id  = _localname(dim_str)
        dim_lbl = _label(g, dim)
        for hier in g.objects(dim, QB4O.hasHierarchy):
            hier_str = str(hier)
            hier_id  = _localname(hier_str)
            steps    = dim_hier_steps.get(dim_str, {}).get(hier_str, [])
            if steps:
                ordered = _topo_sort_levels(steps)
            else:
                ordered = [_localname(str(lv))
                           for lv in g.objects(hier, QB4O.hasLevel)
                           if _localname(str(lv)) != "All"]
            ordered = [ln for ln in ordered if ln != "All"]
            if not ordered: continue
            if dim_id in tbox_dim_groups:
                existing = list(tbox_dim_groups[dim_id][2])
                for ln in ordered:
                    if ln not in existing: existing.append(ln)
                tbox_dim_groups[dim_id] = (tbox_dim_groups[dim_id][0],
                                           tbox_dim_groups[dim_id][1], existing)
            else:
                tbox_dim_groups[dim_id] = (dim_lbl, hier_id, ordered)

    datasets = []
    for ds in g.subjects(RDF.type, QB.DataSet):
        cuboid = next(iter(g.objects(ds, QB.structure)), None)
        if cuboid is None: continue
        levels_seen = set()
        levels, measures = [], []
        for bn in g.objects(cuboid, QB.component):
            for lv in g.objects(bn, QB4O.level):
                ln = _localname(str(lv))
                if ln not in levels_seen and ln != "All":
                    levels_seen.add(ln); levels.append({"uri":str(lv),"name":ln})
            for ms in g.objects(bn, QB.measure):
                agg_fns = [_localname(str(a)) for a in g.objects(bn, QB4O.aggregateFunction)]
                ms_name = _localname(str(ms))
                if not any(m["name"]==ms_name for m in measures):
                    measures.append({"uri":str(ms),"name":ms_name,
                                     "agg":agg_fns or ["avg","count","min","max","sum"]})
        cube = next(iter(g.objects(cuboid, QB4O.isCuboidOf)), cuboid)
        for bn in g.objects(cube, QB.component):
            for dim in g.objects(bn, QB4O.dimension):
                for hier in g.objects(dim, QB4O.hasHierarchy):
                    for lv in g.objects(hier, QB4O.hasLevel):
                        ln = _localname(str(lv))
                        if ln not in levels_seen and ln != "All":
                            levels_seen.add(ln); levels.append({"uri":str(lv),"name":ln})
        datasets.append({"label":_label(g,ds),"value":str(ds),"cuboid":str(cuboid),
                         "levels":levels,"measures":measures})
    return {"graph":g, "datasets":datasets, "dim_groups":tbox_dim_groups}

# ══════════════════════════════════════════════════════════════════════════════
#  FAST ABOX PARSER
# ══════════════════════════════════════════════════════════════════════════════
def _pfx_map(text):
    return {m.group(1):m.group(2) for m in re.finditer(r'@prefix\s+(\w*):\s+<([^>]+)>',text)}

def _resolve(token, pfx):
    t = token.strip().rstrip(";,.")
    if t.startswith("<") and t.endswith(">"): return t[1:-1]
    if ":" in t:
        p,local = t.split(":",1)
        b = pfx.get(p)
        if b: return b+local
    return t

def _fast_parse_abox(text, progress_cb=None):
    pfx = _pfx_map(text)
    if progress_cb: progress_cb("Building label lookup…",8)
    member_labels = {}
    lm_re = re.compile(
        r'((?:onto|dataset):\S+)\s+a\s+qb4o:LevelMember(?:[^;.]*)?;(.*?)(?=\n\n|\n(?:onto|mhobs|mhsui|dataset):\S|\Z)',
        re.DOTALL)
    name_attrs = ["mha:countryName","mha:continentName","mha:regionName","mha:gender",
                  "mha:genderCategory","mha:lifeStageName","mha:ageGroupRange",
                  "mha:universityName","mha:academicYearName","mha:cgpaCategory",
                  "mha:occupationName","mha:sleepQuality","mha:socialSupportAttr",
                  "mha:yearValue","mha:quarterValue","mha:monthValue","mha:weekValue",
                  "mha:dateValue","mha:allName"]
    id_attrs = ["mha:occupationId","mha:supportSystemId","mha:lifestyleId",
                "mha:countryId","mha:continentId","mha:regionId","mha:genderId",
                "mha:ageGroupId","mha:universityId","mha:cgpaId","mha:academicYearId",
                "mha:yearId","mha:quarterId","mha:monthId","mha:weekId","mha:dayId"]
    name_attr_pat = re.compile(r'('+"|".join(re.escape(a) for a in name_attrs)+r')\s+"([^"]+)"')
    id_attr_pat   = re.compile(r'('+"|".join(re.escape(a) for a in id_attrs  )+r')\s+"([^"]+)"')
    id_to_label: dict[str, str] = {}
    for m in lm_re.finditer(text):
        body = m.group(2)
        nm = name_attr_pat.search(body)
        if nm:
            token=m.group(1).strip(); full=_resolve(token,pfx); label=nm.group(2)
            member_labels[full]=label; member_labels[token]=label
            id_m = id_attr_pat.search(body)
            if id_m: id_to_label[id_m.group(2)] = label
            frag = token.split("#")[-1] if "#" in token else None
            if frag and frag not in id_to_label: id_to_label[frag] = label
    for m in re.finditer(r'onto:\w+#(\S+)',text):
        token=m.group(0).rstrip('.,;'); frag=m.group(1).rstrip('.,;')
        if token not in member_labels:
            full=_resolve(token,pfx)
            label = id_to_label.get(frag, frag.replace("_"," "))
            member_labels[token]=label; member_labels[full]=label
    if progress_cb: progress_cb(f"Labels: {len(member_labels):,} found. Scanning…",18)
    obs_re  = re.compile(r'(\w+:\S+)\s+a\s+qb:Observation\s*;(.*?)(?=\n\n\w+:\S+\s+a\s+qb:Observation|\n\nonto:|\Z)',re.DOTALL)
    prop_re = re.compile(r'mhp:(\w+)\s+((?:onto|mhp|mha|dataset):[^\s;,.]+|"[^"]*"(?:\^\^[^\s;.]+)?)',re.MULTILINE)
    ds_re   = re.compile(r'qb:dataSet\s+(dataset:\S+)')
    lit_re  = re.compile(r'"([^"]*)"')
    DIM_PROPS     = set(ALL_KNOWN_LEVELS)
    MEASURE_PROPS = {"depressionScore","anxietyScore","stressLevel","sleepHours",
                     "screenTime","physicalActivityHours","socialSupportScore",
                     "suicideRate","suicideRateLow","suicideRateHigh"}
    INLINE_PROPS  = {"age","cgpa","relationshipStatus","socialSupport","familyHistory",
                     "treatment","whoRegionCode","whoRegionName","locationCode","sexCode","valueLabel"}
    obs_list = list(obs_re.finditer(text))
    n_obs = len(obs_list)
    if progress_cb: progress_cb(f"Found {n_obs:,} observations — parsing…",22,n_obs)
    rows = []
    for i,m in enumerate(obs_list):
        block=m.group(2); row={}
        dsm=ds_re.search(block)
        if dsm: row["_dataset"]=_localname(dsm.group(1))
        for pm in prop_re.finditer(block):
            prop=pm.group(1); value=pm.group(2).strip().rstrip(";,.")
            if value.startswith('"'):
                lm2=lit_re.match(value)
                if lm2:
                    raw_val=lm2.group(1)
                    if prop in MEASURE_PROPS or prop in INLINE_PROPS:
                        try: row[prop]=float(raw_val)
                        except ValueError: row[prop]=raw_val
            else:
                full=_resolve(value,pfx)
                frag_id = full.split("#")[-1] if "#" in full else None
                label=(member_labels.get(full)
                       or member_labels.get(value)
                       or (frag_id and id_to_label.get(frag_id))
                       or _localname(full))
                if prop in DIM_PROPS or prop in MEASURE_PROPS or prop in INLINE_PROPS:
                    row[prop]=label
        rows.append(row)
        if progress_cb and i%500==0 and n_obs>0:
            progress_cb(f"Parsed {i:,}/{n_obs:,} observations…",22+int(72*i/n_obs),n_obs,i)
    if progress_cb: progress_cb(f"Building DataFrame ({n_obs:,} rows)…",96)
    df = pd.DataFrame(rows) if rows else pd.DataFrame()
    df = _derive_rollup_columns(df, text)
    return df

ROLLUP_CHAINS = [
    ("Country",      "Region",         "inRegion",         "countryName"),
    ("Region",       "Continent",      "inContinent",      "regionName"),
    ("Gender",       "GenderCategory", "inGenderCategory", "gender"),
    ("AgeGroup",     "LifeStage",      "inLifeStage",      "ageGroupRange"),
    ("CGPACategory", "AcademicYear",   "inAcademicYear",   "cgpaCategory"),
    ("AcademicYear", "University",     "inUniversity",     "academicYearName"),
    ("University",   "Country",        "inCountryAcad",    "universityName"),
    ("Day",          "Week",           "inWeek",           "dateValue"),
    ("Week",         "Month",          "inMonth",          "weekValue"),
    ("Month",        "Quarter",        "inQuarter",        "monthValue"),
    ("Quarter",      "Year",           "inYear",           "quarterValue"),
]

def _derive_rollup_columns(df, abox_text):
    if df.empty:
        return df
    block_re = re.compile(
        r'onto:\w+#(\S+?)\s+a\s+qb4o:LevelMember(.*?)(?=\nonto:|\n\n|\Z)',
        re.DOTALL
    )
    any_name_re = re.compile(
        r'mha:(?:allName|continentName|regionName|countryName|'
        r'gender(?:Category)?|lifeStageName|ageGroupRange|'
        r'universityName|academicYearName|cgpaCategory|'
        r'occupationName|sleepQuality|socialSupportAttr|'
        r'yearValue|quarterValue|monthValue|weekValue|dateValue)'
        r'\s+"([^"]+)"'
    )
    frag_to_label = {}
    for m in block_re.finditer(abox_text):
        frag = m.group(1).rstrip('.,; ')
        nm   = any_name_re.search(m.group(2))
        if nm:
            frag_to_label[frag] = nm.group(1)
    rollup_maps = {}
    for (child_col, parent_col, rollup_attr, child_name_attr) in ROLLUP_CHAINS:
        rollup_re    = re.compile(rf'mha:{rollup_attr}\s+onto:\w+#(\S+?)(?:\s|;|\.)')
        child_lbl_re = re.compile(rf'mha:{child_name_attr}\s+"([^"]+)"')
        lkp = {}
        for m in block_re.finditer(abox_text):
            body = m.group(2)
            rm   = rollup_re.search(body)
            if not rm:
                continue
            parent_frag  = rm.group(1).rstrip('.,; ')
            parent_label = frag_to_label.get(parent_frag)
            if not parent_label:
                continue
            cm = child_lbl_re.search(body)
            if cm:
                lkp[cm.group(1)] = parent_label
        rollup_maps[(child_col, parent_col)] = lkp
    for (child_col, parent_col, _, _) in ROLLUP_CHAINS:
        lkp = rollup_maps.get((child_col, parent_col), {})
        if not lkp or child_col not in df.columns:
            continue
        derived = df[child_col].map(lkp)
        if derived.notna().any():
            df[parent_col] = derived
    return df


def parse_abox_from_file(filepath, tbox_graph, progress_cb=None):
    sz=os.path.getsize(filepath)/1_048_576
    if progress_cb: progress_cb(f"Reading {sz:.1f} MB from disk…",2)
    with open(filepath,"rb") as f: raw=f.read()
    text=_safe_decode(raw); del raw; gc.collect()
    text=text.replace("\r\n","\n").replace("\r","\n")
    return _fast_parse_abox(text,progress_cb=progress_cb)

# ══════════════════════════════════════════════════════════════════════════════
#  QUERY ENGINE
# ══════════════════════════════════════════════════════════════════════════════
AGG_MAP={"avg":"mean","sum":"sum","count":"count","min":"min","max":"max"}

def run_olap_query(df,levels,measure,agg_fn,filters):
    if df is None or df.empty or measure not in df.columns: return pd.DataFrame()
    mask=pd.Series(True,index=df.index)
    for col,vals in filters.items():
        if not vals or col not in df.columns: continue
        vals_lower={str(v).lower().replace("_"," ").replace("-"," ") for v in vals}
        col_norm=df[col].astype(str).str.lower().str.replace("_"," ",regex=False).str.replace("-"," ",regex=False)
        mask &= col_norm.isin(vals_lower)
    d=df[mask].copy()
    d[measure]=pd.to_numeric(d[measure],errors="coerce")
    agg_fn_pd=AGG_MAP.get(agg_fn,"mean"); col_name=f"{agg_fn}_{measure}"
    avail=[lv for lv in levels if lv in d.columns]
    if not avail:
        val=getattr(d[measure].dropna(),agg_fn_pd)()
        return pd.DataFrame([{col_name:round(float(val),6)}])
    return d.groupby(avail)[measure].agg(agg_fn_pd).reset_index().rename(columns={measure:col_name}).sort_values(col_name,ascending=False)

# ══════════════════════════════════════════════════════════════════════════════
#  DASH APP + FLASK
# ══════════════════════════════════════════════════════════════════════════════
C={
    "nav":"#1a3a5c","blue":"#2e7d9f","accent":"#4db8e8",
    "green":"#28a745","amber":"#ffc107","red":"#dc3545",
    "bg":"#f4f8fb","border":"#c8dce8","text":"#1a3a5c",
    "lv_active":"#c8e0f5","lv_selected":"#d4edda",
    "ms_active":"#f8d7da",
}
CARD={"border":f"2px solid {C['border']}","borderRadius":"8px","padding":"0","backgroundColor":"white"}

app=dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP,dbc.icons.FONT_AWESOME],
    title="MHKG OLAP",
    suppress_callback_exceptions=True,
)
server=app.server

# ── Flask chunked upload ──────────────────────────────────────────────────────
@server.route("/upload-abox-init",methods=["POST"])
def upload_init():
    data=flask.request.get_json(force=True)
    total_bytes=int(data.get("size",0))
    tmp=tempfile.NamedTemporaryFile(delete=False,suffix=".ttl",prefix="mhkg_"); tmp.close()
    with _lock:
        _state.update({"_chunk_path":tmp.name,"_chunk_bytes":0,"_chunk_total":total_bytes,
                        "abox_name":data.get("filename","abox.ttl"),"loading":False,"load_pct":0,
                        "load_progress":f"Receiving {total_bytes/1_048_576:.1f} MB…",
                        "obs_df":None,"parse_start":0,"obs_done":0,"obs_total":0})
    return flask.jsonify({"tmp":tmp.name})

@server.route("/upload-abox-chunk",methods=["POST"])
def upload_chunk():
    tmp_path=flask.request.headers.get("X-Tmp-Path","")
    if not tmp_path or not os.path.exists(tmp_path): return flask.jsonify({"error":"bad tmp"}),400
    chunk=flask.request.data
    with open(tmp_path,"ab") as f: f.write(chunk)
    with _lock:
        _state["_chunk_bytes"]+=len(chunk)
        total=_state["_chunk_total"]; done=_state["_chunk_bytes"]
        _state["load_progress"]=f"Receiving… {done/1_048_576:.1f}/{total/1_048_576:.1f} MB"
        _state["load_pct"]=max(1,int(20*done/total) if total else 0)
        _state["loading"]=True
    return flask.jsonify({"received":len(chunk)})

@server.route("/upload-abox-finalise",methods=["POST"])
def upload_finalise():
    data=flask.request.get_json(force=True)
    tmp_path=data.get("tmp",""); filename=data.get("filename","abox.ttl")
    if not tmp_path or not os.path.exists(tmp_path): return flask.jsonify({"error":"tmp not found"}),400
    with _lock:
        _state["load_progress"]="Upload complete — starting parse…"
        _state["load_pct"]=21; _state["loading"]=True; _state["abox_name"]=filename
    def _bg():
        def _prog(msg,pct=0,obs_total=0,obs_done=0):
            with _lock:
                _state["load_progress"]=msg; _state["load_pct"]=max(1,min(99,pct))
                if obs_total and not _state["parse_start"]:
                    _state["parse_start"]=time.time(); _state["obs_total"]=obs_total
                if obs_done: _state["obs_done"]=obs_done
        try:
            df=parse_abox_from_file(tmp_path,_state.get("tbox"),progress_cb=_prog)
            with _lock:
                _state["obs_df"]=df
                _state["load_progress"]=f"Done — {len(df):,} observations loaded."
                _state["load_pct"]=100
        except Exception as ex:
            with _lock: _state["load_progress"]=f"Error: {ex}"; _state["load_pct"]=0
        finally:
            with _lock: _state["loading"]=False
            try: os.unlink(tmp_path)
            except OSError: pass
    threading.Thread(target=_bg,daemon=True).start()
    return flask.jsonify({"status":"parsing"})

# ══════════════════════════════════════════════════════════════════════════════
#  LAYOUT
# ══════════════════════════════════════════════════════════════════════════════
navbar=dbc.Navbar(dbc.Container([
    html.Div([
        html.Span([
            html.I(className="fa fa-brain me-2",style={"color":C["accent"]}),
            html.Strong("MHKG",style={"color":"white","fontSize":"1.15rem","letterSpacing":"0.04em"}),
            html.Span(" OLAP",style={"color":C["accent"],"fontSize":"1.05rem"}),
            html.Span(" · Mental Health Knowledge Graph — OLAP Interface",
                      style={"color":"#9fcde8","fontSize":"0.82rem","marginLeft":"8px"}),
        ]),
        html.Div([
            dbc.Badge("QB4OLAP",color="info",className="me-1",style={"fontSize":"0.72rem"}),
            dbc.Badge("v3.0",color="secondary",style={"fontSize":"0.72rem"}),
        ],className="ms-auto d-flex align-items-center"),
    ],className="d-flex align-items-center w-100"),
],fluid=True),color=C["nav"],dark=True,sticky="top",
style={"borderBottom":f"2px solid {C['accent']}","padding":"6px 0"},)

# ── shared sub-card style ─────────────────────────────────────────────────────
def subcard(border_color): return {
    "border":f"1px solid {border_color}","borderRadius":"4px",
    "padding":"6px 8px","marginBottom":"8px","backgroundColor":"white",
}
def subcard_header(color): return {
    "color":color,"fontWeight":"700","fontSize":"0.72rem",
    "textTransform":"uppercase","letterSpacing":"0.05em","marginBottom":"4px",
}

# ── LEFT panel ────────────────────────────────────────────────────────────────
left_panel=dbc.Card([
    dbc.CardHeader(html.Span([html.I(className="fa fa-sitemap me-2"),
                               html.B("Structure Extraction")],
                              style={"color":C["nav"],"fontSize":"0.85rem"}),
                   style={"backgroundColor":"#eaf3fb","padding":"5px 10px"}),
    dbc.CardBody([
        html.Div(style=subcard(C["blue"]),children=[
            html.Div("TBox IRI",style=subcard_header(C["blue"])),
            html.Div("http://mhkg.example.com/…/mentalhealth/",
                     className="text-muted text-truncate mb-1",
                     style={"fontFamily":"monospace","fontSize":"0.7rem"}),
            dcc.Upload(id="upload-tbox",children=html.Div([
                html.I(className="fa fa-cube me-1",style={"color":C["blue"],"fontSize":"0.8rem"}),
                html.Span("Browse TBox (.ttl)",style={"fontSize":"0.77rem","color":C["blue"]}),
            ],style={"textAlign":"center","padding":"4px 6px"}),
            style={"border":f"1px dashed {C['blue']}","borderRadius":"4px",
                   "backgroundColor":C["bg"],"cursor":"pointer"},
            multiple=False,max_size=50*1_048_576),
            html.Div(id="upload-tbox-status",className="mt-1",style={"fontSize":"0.75rem"}),
        ]),
        html.Div(style=subcard(C["nav"]),children=[
            html.Div("ABox IRI",style=subcard_header(C["nav"])),
            html.Div("http://mhkg.example.com/…/abox/",
                     className="text-muted text-truncate mb-1",
                     style={"fontFamily":"monospace","fontSize":"0.7rem"}),
            html.Div([
                dbc.Button([html.I(className="fa fa-folder-open me-1",style={"fontSize":"0.78rem"}),
                            html.Span("Browse ABox (.ttl)",style={"fontSize":"0.77rem"})],
                           id="abox-browse-btn",color="primary",outline=True,size="sm",
                           style={"padding":"3px 10px"}),
                html.Div(id="abox-file-wrap",style={"position":"absolute","width":"0","height":"0","overflow":"hidden"}),
            ],id="abox-drop-zone",style={"textAlign":"center","padding":"5px",
                "border":f"1px dashed {C['nav']}","borderRadius":"4px",
                "backgroundColor":C["bg"],"position":"relative"}),
            html.Div(id="upload-abox-status",className="mt-1",style={"fontSize":"0.75rem"}),
        ]),
        html.Div([
            html.Div([
                html.Span(id="load-status",style={"fontSize":"0.73rem","color":C["blue"]}),
                html.Span(id="pct-badge",style={"fontWeight":"bold","color":C["blue"],
                          "marginLeft":"5px","fontSize":"0.73rem"}),
            ],style={"minHeight":"16px","marginBottom":"2px"}),
            dbc.Progress(id="load-bar",value=0,striped=True,animated=True,color="info",
                         style={"height":"6px","borderRadius":"3px","transition":"width 0.25s"}),
            html.Div(id="parse-substats",style={"minHeight":"12px","fontFamily":"monospace",
                     "fontSize":"0.68rem","color":"#666","marginTop":"2px"}),
        ],className="mb-2"),
        html.Hr(style={"margin":"4px 0"}),
        html.Div("Dataset",style=subcard_header(C["nav"])),
        dcc.Dropdown(id="dd-dataset",placeholder="— load TBox first —",
                     className="mb-1",style={"fontSize":"0.82rem"}),
        html.Div(id="schema-info",className="mb-1",style={"fontSize":"0.75rem","color":"#555"}),
        html.Hr(style={"margin":"4px 0"}),
        html.Div("Dimensions & Measures",style=subcard_header(C["nav"])),
        html.Div(id="dim-tree",style={
            "border":f"1px solid {C['border']}","borderRadius":"4px",
            "overflow":"hidden","fontSize":"0.81rem",
        }),
    ],style={"padding":"8px","overflowY":"auto","maxHeight":"78vh"}),
],style={**CARD,"border":f"1px solid {C['blue']}","height":"82vh","overflowY":"auto"})

# ── MIDDLE panel ──────────────────────────────────────────────────────────────
mid_panel=dbc.Card([
    dbc.CardHeader(html.Span([html.I(className="fa fa-filter me-2"),
                               html.B("Instance Filtering")],
                              style={"color":C["nav"],"fontSize":"0.85rem"}),
                   style={"backgroundColor":"#eaf3fb","padding":"5px 10px"}),
    dbc.CardBody([
        html.Div(id="selected-level-label",className="mb-2",
                 style={"minHeight":"20px","fontWeight":"600",
                        "fontSize":"0.82rem","color":C["blue"]}),
        dbc.Row([
            dbc.Col([
                html.Label("Attributes",style={"fontWeight":"600","fontSize":"0.78rem","marginBottom":"2px"}),
                dcc.Dropdown(id="dd-attr",placeholder="—",className="mb-2",
                             style={"fontSize":"0.8rem"}),
            ],md=6),
            dbc.Col([
                html.Label("Filter Condition",style={"fontWeight":"600","fontSize":"0.78rem","marginBottom":"2px"}),
                dcc.Dropdown(id="dd-filter-op",value="eq",className="mb-2",
                             style={"fontSize":"0.8rem"},
                             options=[{"label":"Equal to (=)","value":"eq"},
                                      {"label":"Contains","value":"contains"},
                                      {"label":"Not equal (≠)","value":"neq"}]),
            ],md=6),
        ]),
        html.Label("To Be Viewed Property",style={"fontWeight":"600","fontSize":"0.78rem","marginBottom":"2px"}),
        dbc.Input(id="filter-value",placeholder="Search instances…",debounce=True,size="sm",
                  className="mb-2"),
        html.Div(
            dbc.Checklist(id="instance-checklist",options=[],value=[],inline=False,
                          style={"padding":"5px","fontSize":"0.82rem"}),
            style={"border":f"1px solid {C['border']}","borderRadius":"4px",
                   "backgroundColor":C["bg"],"maxHeight":"46vh","overflowY":"auto"},
        ),
        html.Div([
            dbc.ButtonGroup([
                dbc.Button("Select All",id="btn-all",  size="sm",color="secondary",outline=True,
                           style={"fontSize":"0.78rem"}),
                dbc.Button("Clear All", id="btn-clear",size="sm",color="danger",outline=True,
                           style={"fontSize":"0.78rem"}),
                dbc.Button([html.I(className="fa fa-plus me-1"),"Add to Group-By"],
                           id="btn-add-level",size="sm",color="primary",outline=True,
                           style={"fontSize":"0.78rem"}),
            ]),
            html.Span(id="instance-count",style={"fontSize":"0.72rem","color":"#777","marginLeft":"8px"}),
        ],className="mt-2 d-flex align-items-center flex-wrap"),
    ],style={"padding":"8px"}),
],style={**CARD,"border":f"1px solid {C['nav']}","height":"82vh","overflowY":"auto"})

# ── RIGHT panel ───────────────────────────────────────────────────────────────
right_panel=dbc.Card([
    dbc.CardHeader(html.Span([html.I(className="fa fa-clipboard-list me-2"),
                               html.B("Selection Summary")],
                              style={"color":C["nav"],"fontSize":"0.85rem"}),
                   style={"backgroundColor":"#eaf3fb","padding":"5px 10px"}),
    dbc.CardBody([
        html.Div(style=subcard(C["red"]),children=[
            html.Div("Measures",style=subcard_header(C["red"])),
            html.Div(id="summary-measure-box"),
            dcc.RadioItems(
                id="dd-agg",
                options=[],
                value="avg",
                style={"paddingLeft":"6px","fontSize":"0.82rem"},
                inputStyle={"marginRight":"4px"},
                labelStyle={"display":"block","padding":"2px 0"},
            ),
        ]),
        html.Div(style=subcard(C["green"]),children=[
            html.Div("Levels",style=subcard_header(C["green"])),
            html.Div(id="summary-levels-box",
                     children=html.Span("No levels added yet",
                                         style={"fontSize":"0.78rem","color":"#999"})),
        ]),
        html.Div(style=subcard(C["blue"]),children=[
            html.Div("Active Filters",style=subcard_header(C["blue"])),
            html.Div(id="active-filters-display",
                     children=html.Span("No active filters",style={"fontSize":"0.78rem","color":"#999"})),
        ]),
    ],style={"padding":"8px"}),
],style={**CARD,"border":f"1px solid {C['green']}","height":"82vh","overflowY":"auto"})

# ── stores + hidden stubs ─────────────────────────────────────────────────────
stores=html.Div([
    dcc.Store(id="store-result"),
    dcc.Store(id="sparql-store",data=""),
    dcc.Store(id="store-active-level",data=""),
    dcc.Store(id="store-selected-levels",data=[]),
    dcc.Store(id="store-instances",data={}),
    dcc.Store(id="store-active-measure",data=""),
    dcc.Store(id="store-agg",data="avg"),
    dcc.Store(id="store-ds-levels",data=[]),
    dcc.Store(id="store-ds-measures",data=[]),
    dcc.Store(id="store-expanded-dims",data=[]),
    html.Div(id="btn-copy-sparql",  style={"display":"none"}),
    html.Div(id="copy-confirm",     style={"display":"none"}),
    html.Div(id="sparql-text-block",style={"display":"none"}),
    html.Div(id="btn-goto-result",  style={"display":"none"}),
    html.Div(id="btn-goto-olap",    style={"display":"none"}),
    html.Div(id="result-page-title",style={"display":"none"}),
    html.Div(id="page-result",      style={"display":"none"}),
    dcc.Interval(id="interval-poll",interval=300,disabled=False),
])

# ── Page layout ───────────────────────────────────────────────────────────────
olap_page=html.Div([
    dbc.Container([
        # ── 3-panel row ──────────────────────────────────────────────────────
        # CHANGE 1: added mb-4 (bottom margin ~24px) so buttons sit lower
        dbc.Row([
            dbc.Col(left_panel,  md=3,className="pe-1"),
            dbc.Col(mid_panel,   md=5,className="px-1"),
            dbc.Col(right_panel, md=4,className="ps-1"),
        ],className="mt-1 g-2 mb-1"),

        # ── Action bar ───────────────────────────────────────────────────────
        # CHANGE 2: removed position:sticky so bar sits naturally below panels
        #           with generous padding; added a subtle separator shadow
        html.Div([
            dbc.ButtonGroup([
                dbc.Button([html.I(className="fa fa-code me-2"),"Generate SPARQL"],
                           id="btn-sparql",color="warning",outline=True,size="md",
                           style={"fontSize":"0.86rem","fontWeight":"600"}),
                dbc.Button([html.I(className="fa fa-play me-2"),"Execute Query"],
                           id="btn-execute",color="success",size="md",
                           style={"fontSize":"0.86rem","fontWeight":"600"}),
            ]),
            # html.Div(
            #     "💡 Execute Query runs directly — no need to Generate SPARQL first",
            #     style={"fontSize":"0.72rem","color":"#888","marginTop":"6px"},
            # ),
        ],style={
            "backgroundColor":C["bg"],
            "borderTop":f"2px solid {C['border']}",
            "borderBottom":f"2px solid {C['border']}",
            "padding":"8px 0 8px 0",
            "textAlign":"center",
            "marginBottom":"12px",
            "boxShadow":"0 2px 8px rgba(46,125,159,0.07)",
        }),

        # ── Unified result card ───────────────────────────────────────────────
        dbc.Row(dbc.Col(
            html.Div(id="result-block",style={"display":"none"},children=[
                dbc.Card([
                    dbc.CardHeader(html.Div([
                        dbc.Tabs([
                            dbc.Tab(label="SPARQL",   tab_id="tab-sparql",
                                    label_style={"fontSize":"0.82rem","padding":"5px 12px"}),
                            dbc.Tab(label="Tabular",  tab_id="tab-table",
                                    label_style={"fontSize":"0.82rem","padding":"5px 12px"}),
                            dbc.Tab(label="Graphical",tab_id="tab-graph",
                                    label_style={"fontSize":"0.82rem","padding":"5px 12px"}),
                        ],id="result-tabs",active_tab="tab-sparql"),
                        html.Div(id="result-info",
                                 style={"fontSize":"0.74rem","color":"#555",
                                        "marginLeft":"auto","paddingRight":"4px"}),
                    ],className="d-flex align-items-center w-100"),
                    style={"backgroundColor":"#eaf3fb","padding":"4px 10px"}),
                    dbc.CardBody([
                        html.Div(id="result-container",
                                 children=html.Div(
                                     "Click Generate SPARQL or Execute Query.",
                                     className="text-muted p-4 text-center")),
                    ],style={"padding":"8px","minHeight":"200px"}),
                ],style={**CARD,"border":f"1px solid {C['border']}"}),
            ]),
        className="mb-3")),
    ],fluid=True),
],id="page-olap",style={"paddingTop":"4px"})

body=html.Div([olap_page, stores])

app.layout=html.Div([navbar,body],
    style={"backgroundColor":C["bg"],"minHeight":"100vh"})

# stub clientside callbacks
app.clientside_callback("function(n,t){return '';}",
    Output("copy-confirm","children"),Input("btn-copy-sparql","n_clicks"),
    State("sparql-text-block","children"),prevent_initial_call=True)

app.clientside_callback(
    """
    function(n, text) {
        if (!n || !text) return "";
        var s = typeof text === 'string' ? text
              : (Array.isArray(text) ? text.join('') : String(text));
        try {
            navigator.clipboard.writeText(s);
        } catch(e) {
            var ta = document.createElement("textarea");
            ta.value = s;
            ta.style.cssText = "position:fixed;top:-9999px;left:-9999px;opacity:0";
            document.body.appendChild(ta);
            ta.focus(); ta.select();
            try { document.execCommand('copy'); } catch(e2) {}
            document.body.removeChild(ta);
        }
        return "✓ Copied!";
    }
    """,
    Output("copy-confirm-inner","children"),
    Input("btn-copy-sparql-inner","n_clicks"),
    State("sparql-store","data"),
    prevent_initial_call=True,
)

# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════
@app.callback(
    Output("upload-tbox-status","children"),
    Output("dd-dataset","options"),Output("dd-dataset","value"),
    Input("upload-tbox","contents"),State("upload-tbox","filename"),
    prevent_initial_call=True,
)
def cb_tbox(contents,filename):
    if not contents: return no_update,no_update,no_update
    try:
        _,b64=contents.split(",",1)
        result=parse_tbox(base64.b64decode(b64))
        with _lock:
            _state["tbox"]=result["graph"]; _state["tbox_name"]=filename
            _state["datasets"]=result["datasets"]
            _state["dim_groups"]=result.get("dim_groups", {})
        opts=[{"label":d["label"],"value":d["value"]} for d in result["datasets"]]
        return ([dbc.Badge("Loaded",color="success",className="me-1"),
                 f"{filename} — {len(opts)} dataset(s)"],
                opts,opts[0]["value"] if opts else None)
    except Exception as e:
        short=str(e).replace("\n"," | ")[:500]
        return ([dbc.Badge("Error",color="danger",className="me-1"),
                 html.Span(short,className="text-danger small d-block",style={"wordBreak":"break-word"})],
                [],None)

@app.callback(
    Output("load-status","children"),Output("load-bar","value"),
    Output("pct-badge","children"),Output("parse-substats","children"),
    Output("upload-abox-status","children",allow_duplicate=True),
    Input("interval-poll","n_intervals"),prevent_initial_call=True,
)
def cb_poll(_n):
    with _lock:
        loading=_state["loading"]; msg=_state["load_progress"]; pct=_state["load_pct"]
        fname=_state.get("abox_name",""); obs_done=_state.get("obs_done",0)
        obs_total=_state.get("obs_total",0); parse_start=_state.get("parse_start",0)
    substats=""
    if parse_start and obs_done>0:
        el=time.time()-parse_start; rate=obs_done/el if el else 0
        rem=(obs_total-obs_done)/rate if rate else 0
        substats=(f"⚡ {rate:,.0f} obs/s  │  ⏱ {int(el//60)}m{int(el%60):02d}s  │  "
                  f"⏳ ~{int(rem//60)}m{int(rem%60):02d}s  │  📊 {obs_done:,}/{obs_total:,}")
    elif msg and "Receiving" in msg: substats="📡 Streaming…"
    elif msg and "label" in msg.lower(): substats="🔍 Building labels…"
    pct_label=f"{pct}%" if pct else ""
    if loading: return msg,pct,pct_label,substats,no_update
    if pct==100:
        el=time.time()-parse_start if parse_start else 0
        fs=f"✅ {obs_total:,} obs in {int(el//60)}m{int(el%60):02d}s" if obs_total else ""
        return msg,100,"100%",fs,[dbc.Badge("Loaded",color="success",className="me-1"),f"{fname} — {msg}"]
    if pct==0 and not msg: return no_update,0,"","",no_update
    return msg,0,"","",\
           [dbc.Badge("Error",color="danger",className="me-1"),html.Span(msg,className="text-danger small")]

@app.callback(
    Output("schema-info","children"),
    Output("dim-tree","children"),
    Output("store-ds-levels","data"),
    Output("store-ds-measures","data"),
    Input("dd-dataset","value"),
    prevent_initial_call=True,
)
def cb_dataset(ds_val):
    if not ds_val: return no_update,no_update,no_update,no_update
    with _lock:
        datasets=_state["datasets"]; obs_df=_state.get("obs_df")
        dim_groups=_state.get("dim_groups") or DIM_GROUPS
    ds=next((d for d in datasets if d["value"]==ds_val),None)
    if not ds:
        return "Dataset not found.",[html.Div("No data",className="small text-muted p-2")],[],[]

    tbox_level_names={lv["name"] for lv in ds["levels"]}
    df_extra=[]
    if obs_df is not None and not obs_df.empty:
        df_extra=[c for c in obs_df.columns if c in ALL_KNOWN_LEVELS and c not in tbox_level_names]
    all_levels=ds["levels"]+[{"uri":"","name":c} for c in df_extra]
    level_set={lv["name"] for lv in all_levels}

    schema=[
        html.Div(f"Schema: mhs:{_localname(ds['cuboid'])}",className="small fw-bold"),
        html.Div(f"{len(all_levels)} levels · {len(ds['measures'])} measures",className="small text-secondary"),
        html.Div(f"{len(obs_df):,} observations" if obs_df is not None else "ABox not loaded",
                 className=f"small {'text-success' if obs_df is not None else 'text-muted'}"),
    ]

    rows=[]
    rows.append(html.Div(
        [html.I(className="fa fa-cubes me-2"),"Dimensions"],
        style={"backgroundColor":C["nav"],"color":"white","padding":"5px 10px",
               "fontWeight":"700","fontSize":"0.78rem","letterSpacing":"0.06em"},
    ))

    for dim_id,(dim_label,hier_label,dim_levels) in dim_groups.items():
        in_this_cube=[lv for lv in dim_levels if lv in level_set]
        if not in_this_cube: continue

        rows.append(html.Div(
            [
                html.I(className="fa fa-angle-right me-1",
                       id={"type":"dim-arrow","index":dim_id},
                       style={"transition":"transform 0.2s","fontSize":"0.8rem",
                              "width":"12px","flexShrink":"0"}),
                html.I(className="fa fa-cubes me-2",
                       style={"color":C["nav"],"fontSize":"0.8rem","width":"14px","flexShrink":"0"}),
                html.Span(f"mhp:{dim_id}",style={"flex":"1","fontFamily":"monospace"}),
            ],
            id={"type":"dim-btn","index":dim_id},
            n_clicks=0,
            style={"display":"flex","alignItems":"center","padding":"4px 10px",
                   "cursor":"pointer","backgroundColor":"#d6e8f5","color":C["nav"],
                   "fontWeight":"600","fontSize":"0.78rem",
                   "borderBottom":f"1px solid {C['border']}",
                   "transition":"background 0.15s"},
        ))

        level_rows=[]
        for lv_name in in_this_cube:
            n_vals=""
            if obs_df is not None and lv_name in obs_df.columns:
                n_vals=f"  ({obs_df[lv_name].nunique():,})"
            level_rows.append(html.Div(
                [
                    html.Span("│  ",style={"color":"#aac4d8","fontFamily":"monospace",
                                           "fontSize":"0.75rem","flexShrink":"0"}),
                    html.Span("├─ " if lv_name!=in_this_cube[-1] else "└─ ",
                              style={"color":"#aac4d8","fontFamily":"monospace",
                                     "fontSize":"0.75rem","flexShrink":"0"}),
                    html.I(className="fa fa-layer-group me-1",
                           style={"color":C["blue"],"width":"12px","flexShrink":"0","fontSize":"0.75rem"}),
                    html.Span(f"mhp:{lv_name}",
                              style={"flex":"1","fontFamily":"monospace","fontSize":"0.78rem"}),
                    html.Span(n_vals,className="text-muted",
                              style={"fontSize":"0.71rem","marginLeft":"4px"}),
                    html.Span("✓",id={"type":"lv-check","index":lv_name},
                              style={"display":"none","color":C["green"],
                                     "fontWeight":"700","marginLeft":"4px","fontSize":"0.9rem"}),
                ],
                id={"type":"lv-btn","index":lv_name},
                n_clicks=0,
                style={"display":"flex","alignItems":"center",
                       "padding":"4px 10px 4px 16px",
                       "cursor":"pointer","borderBottom":f"1px solid {C['border']}",
                       "transition":"background 0.12s"},
            ))

        hier_block=html.Div(
            [
                html.Div(
                    [
                        html.Span("  ",style={"fontFamily":"monospace","flexShrink":"0"}),
                        html.I(className="fa fa-sitemap me-1",
                               style={"color":"#5a8fb0","fontSize":"0.75rem","width":"12px","flexShrink":"0"}),
                        html.Span(f"mhs:{hier_label}",
                                  style={"fontFamily":"monospace","fontSize":"0.77rem",
                                         "color":"#5a8fb0","fontStyle":"italic"}),
                    ],
                    style={"display":"flex","alignItems":"center","padding":"3px 10px 3px 12px",
                           "backgroundColor":"#eaf3fb","borderBottom":f"1px solid {C['border']}"},
                ),
                html.Div(level_rows),
            ],
            id={"type":"dim-collapse","index":dim_id},
            style={"display":"none","borderLeft":f"3px solid {C['blue']}44","marginLeft":"0"},
        )
        rows.append(hier_block)

    rows.append(html.Div(
        [html.I(className="fa fa-chart-bar me-2"),"Measures"],
        style={"backgroundColor":C["red"]+"cc","color":"white","padding":"5px 10px",
               "fontWeight":"700","fontSize":"0.78rem","letterSpacing":"0.06em",
               "marginTop":"2px"},
    ))

    for ms in ds["measures"]:
        agg_str=", ".join(ms["agg"][:4])
        rows.append(html.Div(
            [
                html.I(className="fa fa-circle me-2",
                       style={"color":C["red"],"width":"14px","flexShrink":"0","fontSize":"0.55rem"}),
                html.Div([
                    html.Span(ms["name"],style={"fontWeight":"500"}),
                    html.Br(),
                    html.Span(f"[{agg_str}]",
                              style={"fontSize":"0.7rem","color":"#888","paddingLeft":"0"}),
                ],style={"flex":"1"}),
            ],
            id={"type":"ms-btn","index":ms["name"]},
            n_clicks=0,
            style={"display":"flex","alignItems":"center","padding":"5px 10px",
                   "cursor":"pointer","borderBottom":f"1px solid {C['border']}",
                   "transition":"background 0.12s"},
        ))

    ds_measures_data=[{"name":m["name"],"agg":m["agg"]} for m in ds["measures"]]
    ds_levels_data=[lv["name"] for lv in all_levels]

    return schema, rows, ds_levels_data, ds_measures_data

app.clientside_callback(
    """
    function(n_clicks_list, expanded) {
        var trig = window.dash_clientside && dash_clientside.callback_context
                   ? dash_clientside.callback_context.triggered
                   : null;
        if (!trig || trig.length === 0) return window.dash_clientside.no_update;

        var raw = trig[0].prop_id.replace('.n_clicks','');
        var dimId;
        try { dimId = JSON.parse(raw).index; } catch(e) { return window.dash_clientside.no_update; }

        var expanded2 = Array.isArray(expanded) ? expanded.slice() : [];
        var idx = expanded2.indexOf(dimId);
        if (idx >= 0) {
            expanded2.splice(idx, 1);
        } else {
            expanded2.push(dimId);
        }

        document.querySelectorAll('[id]').forEach(function(el) {
            var raw2 = el.getAttribute('id');
            if (!raw2 || raw2[0] !== '{') return;
            try {
                var id = JSON.parse(raw2);
                if (id.type === 'dim-collapse') {
                    el.style.display = expanded2.indexOf(id.index) >= 0 ? 'block' : 'none';
                }
                if (id.type === 'dim-arrow') {
                    var isOpen = expanded2.indexOf(id.index) >= 0;
                    el.style.transform = isOpen ? 'rotate(90deg)' : 'rotate(0deg)';
                }
                if (id.type === 'dim-btn') {
                    var isOpen2 = expanded2.indexOf(id.index) >= 0;
                    el.style.backgroundColor = isOpen2 ? '#b8d4ec' : '#d6e8f5';
                }
            } catch(e) {}
        });

        return expanded2;
    }
    """,
    Output("store-expanded-dims","data"),
    Input({"type":"dim-btn","index":ALL},"n_clicks"),
    State("store-expanded-dims","data"),
    prevent_initial_call=True,
)


@app.callback(
    Output("store-active-level","data"),
    Input({"type":"lv-btn","index":ALL},"n_clicks"),
    prevent_initial_call=True,
)
def cb_lv_click(n_clicks):
    trig=ctx.triggered_id
    if not trig: return no_update
    return trig["index"]

@app.callback(
    Output("store-active-measure","data"),
    Input({"type":"ms-btn","index":ALL},"n_clicks"),
    prevent_initial_call=True,
)
def cb_ms_click(_):
    trig=ctx.triggered_id
    if not trig: return no_update
    return trig["index"]

app.clientside_callback(
    """
    function(activeLv, selectedLevels, activeMeasure) {
        var selSet = new Set(selectedLevels || []);

        document.querySelectorAll('[id]').forEach(function(el) {
            var raw = el.getAttribute('id');
            if (!raw || raw[0] !== '{') return;
            try {
                var id = JSON.parse(raw);
                if (id.type === 'lv-btn') {
                    var isActive   = id.index === activeLv;
                    var isSelected = selSet.has(id.index);
                    el.style.backgroundColor = isActive
                        ? '#c8e0f5'
                        : isSelected ? '#d4edda' : '';
                    el.style.borderLeft = isActive
                        ? '4px solid #2e7d9f'
                        : isSelected ? '4px solid #28a745' : '4px solid transparent';
                    el.style.fontWeight = (isActive || isSelected) ? '600' : '400';
                }
                if (id.type === 'ms-btn') {
                    var isMsActive = id.index === activeMeasure;
                    el.style.backgroundColor = isMsActive ? '#fde8e8' : '';
                    el.style.borderLeft = isMsActive ? '4px solid #dc3545' : '4px solid transparent';
                    el.style.fontWeight = isMsActive ? '700' : '400';
                }
            } catch(e) {}
        });
        return window.dash_clientside.no_update;
    }
    """,
    Output("dim-tree","data-highlighted"),
    Input("store-active-level","data"),
    Input("store-selected-levels","data"),
    Input("store-active-measure","data"),
    prevent_initial_call=False,
)

@app.callback(
    Output("selected-level-label","children"),
    Output("dd-attr","options"),Output("dd-attr","value"),
    Output("instance-checklist","options"),Output("instance-checklist","value"),
    Output("instance-count","children"),
    Input("store-active-level","data"),
    Input("filter-value","value"),
    Input("dd-filter-op","value"),
    prevent_initial_call=True,
)
def cb_update_middle(level,fval,fop):
    if not level:
        return ("",[], None,[],[],  "")
    label=html.Span([
        html.Span("Selected level: ",style={"color":"#666","fontWeight":"400"}),
        html.Span(f"mhp:{level}",style={"color":C["blue"],"fontWeight":"700"}),
    ])
    attr_name=LEVEL_ATTR_MAP.get(level,level.lower()+"Name")
    attr_opts=[{"label":attr_name,"value":attr_name}]
    with _lock: df=_state.get("obs_df")
    if df is None or df.empty or level not in df.columns:
        return label,attr_opts,attr_name,[],[],"Load ABox to see instances"
    uniq=[str(v) for v in df[level].dropna().unique() if str(v).strip()]
    if fval:
        fv=str(fval).lower()
        if fop=="eq":          uniq=[v for v in uniq if v.lower()==fv]
        elif fop=="contains":  uniq=[v for v in uniq if fv in v.lower()]
        elif fop=="neq":       uniq=[v for v in uniq if v.lower()!=fv]
    uniq_sorted=sorted(uniq)[:300]
    opts=[{"label":v,"value":v} for v in uniq_sorted]
    count=f"{len(uniq_sorted)} of {len(uniq)} values shown"
    return label,attr_opts,attr_name,opts,[],count

@app.callback(
    Output("instance-checklist","value",allow_duplicate=True),
    Input("btn-all","n_clicks"),Input("btn-clear","n_clicks"),
    State("instance-checklist","options"),prevent_initial_call=True,
)
def cb_sel(_a,_b,opts):
    if not opts: return []
    return [o["value"] for o in opts] if ctx.triggered_id=="btn-all" else []

@app.callback(
    Output("store-selected-levels","data"),
    Input("btn-add-level","n_clicks"),
    State("store-active-level","data"),
    State("store-selected-levels","data"),
    prevent_initial_call=True,
)
def cb_add_level(n,level,sel):
    if not n or not level: return no_update
    sel=list(sel or [])
    if level not in sel: sel.append(level)
    return sel

@app.callback(
    Output("store-instances","data"),
    Input("instance-checklist","value"),
    State("store-active-level","data"),
    State("store-instances","data"),
    prevent_initial_call=True,
)
def cb_store_inst(vals,level,current):
    if not level: return no_update
    updated=dict(current or {})
    if vals: updated[level]=vals
    else:    updated.pop(level,None)
    return updated

@app.callback(
    Output("dd-agg","options"),
    Output("dd-agg","value"),
    Input("store-active-measure","data"),
    State("store-ds-measures","data"),
    State("store-agg","data"),
    prevent_initial_call=True,
)
def cb_agg_radio(measure,ds_measures,active_agg):
    if not measure or not ds_measures:
        default=[{"label":f"qb4o:{a}","value":a} for a in ["avg","count","min","max","sum"]]
        return default,"avg"
    ms=next((m for m in ds_measures if m["name"]==measure),None)
    aggs=ms["agg"] if ms else ["avg","count","min","max","sum"]
    val=active_agg if active_agg in aggs else aggs[0]
    opts=[{"label":f"qb4o:{a}","value":a} for a in aggs]
    return opts,val

@app.callback(
    Output("store-agg","data"),
    Input("dd-agg","value"),
    prevent_initial_call=True,
)
def cb_store_agg(val):
    return val or no_update

@app.callback(
    Output("summary-measure-box","children"),
    Input("store-active-measure","data"),
    prevent_initial_call=True,
)
def cb_summary_measure(measure):
    if not measure:
        return html.Span("No measure selected — click a measure in the left panel",
                         className="small text-muted")
    return html.Div([
        html.I(className="fa fa-chart-bar me-1",style={"color":C["red"]}),
        html.Span(f"mhp:{measure}",
                  style={"color":C["red"],"fontWeight":"700","fontSize":"0.86rem"}),
    ])

@app.callback(
    Output("summary-levels-box","children"),
    Input("store-selected-levels","data"),
    Input("store-instances","data"),
    prevent_initial_call=True,
)
def cb_summary_levels(sel_levels,instances):
    if not sel_levels:
        return html.Span("No levels added — select a level and click '+ Add to Group-By'",
                         className="small text-muted")
    cards=[]
    for lv in sel_levels:
        inst=(instances or {}).get(lv,[])
        pills=[dbc.Badge(v,color="primary",className="me-1 mb-1",
                         style={"fontSize":"0.72rem"}) for v in inst[:10]]
        if len(inst)>10: pills.append(dbc.Badge(f"+{len(inst)-10} more",color="secondary"))
        cards.append(dbc.Card([
            html.Div([
                html.I(className="fa fa-layer-group me-1",style={"color":C["blue"],"fontSize":"0.8rem"}),
                html.Span(f"mhp:{lv}",style={"color":C["blue"],"fontWeight":"700","fontSize":"0.82rem"}),
                html.Span(
                    "🗑",id={"type":"remove-lv","index":lv},n_clicks=0,
                    style={"cursor":"pointer","marginLeft":"auto",
                           "color":C["red"],"fontSize":"0.82rem"},
                ),
            ],className="d-flex align-items-center"),
            html.Div(f"attr: mha:{LEVEL_ATTR_MAP.get(lv,lv)}",
                     className="text-muted",style={"fontSize":"0.72rem"}),
            html.Div(pills or [html.Span("All values",className="small text-muted")],
                     className="mt-1"),
        ],body=True,className="mb-1",
          style={"border":f"1px solid {C['blue']}44","borderRadius":"5px",
                 "padding":"5px 7px","backgroundColor":"#f5faff"}))
    return html.Div(cards)

@app.callback(
    Output("store-selected-levels","data",allow_duplicate=True),
    Output("store-instances","data",allow_duplicate=True),
    Input({"type":"remove-lv","index":ALL},"n_clicks"),
    State("store-selected-levels","data"),
    State("store-instances","data"),
    prevent_initial_call=True,
)
def cb_remove_lv(n_clicks,sel,inst):
    trig=ctx.triggered_id
    if not trig or not any(n for n in (n_clicks or []) if n): return no_update,no_update
    lv=trig["index"]
    return [l for l in (sel or []) if l!=lv], {k:v for k,v in (inst or {}).items() if k!=lv}

@app.callback(
    Output("active-filters-display","children"),
    Input("store-instances","data"),
    prevent_initial_call=True,
)
def cb_filter_disp(instances):
    if not instances: return html.Span("No active filters",className="small text-muted")
    items=[]
    for lv,vals in instances.items():
        if not vals: continue
        badges=[dbc.Badge(v,color="info",className="me-1 mb-1",style={"fontSize":"0.72rem"}) for v in vals[:8]]
        if len(vals)>8: badges.append(dbc.Badge(f"+{len(vals)-8}",color="secondary"))
        items.append(html.Div([
            html.B(f"mhp:{lv}: ",className="small",style={"color":C["blue"]}),
            *badges,
        ],className="mb-1"))
    return html.Div(items) if items else html.Span("No active filters",className="small text-muted")

def _build_sparql(levels,measure,agg,instances,ds_val):
    agg_up={"avg":"AVG","sum":"SUM","count":"COUNT","min":"MIN","max":"MAX"}.get(agg or "avg","AVG")
    levels=levels or []; sel_lines=[]; grp_vars=[]; where_lines=[]; bound=set()
    for lv in levels:
        attr=LEVEL_ATTR_MAP.get(lv,lv.lower()+"Name")
        sel_lines.append(f"  ?{lv}_{attr}"); grp_vars.append(f"?{lv}_{attr}")
        where_lines += [f"  ?obs mhp:{lv} ?{lv}_m .",f"  ?{lv}_m mha:{attr} ?{lv}_{attr} ."]
        bound.add(lv)
    m_var=f"?{agg}_{measure or 'value'}"; sel_lines.append(f"  ({agg_up}(?mv) AS {m_var})")
    where_lines.insert(0,f"  ?obs qb:dataSet <{ds_val}> ." if ds_val else "  ?obs a qb:Observation .")
    where_lines.append(f"  ?obs mhp:{measure or 'depressionScore'} ?mv .")
    fblock=[]
    for flevel,fvals in (instances or {}).items():
        if not fvals: continue
        attr=LEVEL_ATTR_MAP.get(flevel,flevel.lower()+"Name")
        vs=", ".join(f'"{v}"' for v in fvals[:50])
        if flevel in bound:
            fblock.append(f"  FILTER(?{flevel}_{attr} IN ({vs}))")
        else:
            fblock+=[f"  ?obs mhp:{flevel} ?{flevel}_fm .",
                     f"  ?{flevel}_fm mha:{attr} ?{flevel}_{attr} .",
                     f"  FILTER(?{flevel}_{attr} IN ({vs}))"]
    grp="GROUP BY "+" ".join(grp_vars) if grp_vars else ""
    sparql=("PREFIX mhp:  <http://mhkg.example.com/datasets/mentalhealth/abox/mdProperty#>\n"
            "PREFIX mha:  <http://mhkg.example.com/datasets/mentalhealth/abox/mdAttribute#>\n"
            "PREFIX qb:   <http://purl.org/linked-data/cube#>\n"
            "PREFIX qb4o: <http://purl.org/qb4olap/cubes#>\n\n"
            "SELECT\n"+"\n".join(sel_lines)+"\nWHERE {\n"+"\n".join(where_lines+fblock)+"\n}\n"
            +(grp+"\n" if grp else "")+f"ORDER BY DESC({m_var})\nLIMIT 1000\n")
    return sparql

# ── Unified callback: Generate SPARQL + Execute Query ────────────────────────
# CHANGE 3: Execute Query always works directly — generates SPARQL internally
# and then immediately runs the query without requiring a prior Generate SPARQL click.
@app.callback(
    Output("store-result","data"),
    Output("result-info","children"),
    Output("result-tabs","active_tab",allow_duplicate=True),
    Output("result-block","style"),
    Output("sparql-store","data"),
    Input("btn-execute","n_clicks"),
    Input("btn-sparql","n_clicks"),
    State("store-selected-levels","data"),
    State("store-active-measure","data"),
    State("store-agg","data"),
    State("store-instances","data"),
    State("dd-dataset","value"),
    prevent_initial_call=True,
)
def cb_action(n_exec,n_sparql,levels,measure,agg,instances,ds_val):
    triggered=ctx.triggered_id

    # Always build SPARQL regardless of which button was pressed
    sparql=_build_sparql(levels, measure or "depressionScore", agg or "avg", instances, ds_val)
    with _lock: _state["last_sparql"]=sparql

    if triggered=="btn-sparql":
        # Just show SPARQL, don't execute
        return ({"sparql":sparql},
                [dbc.Badge("SPARQL ready",color="warning",className="me-1",
                           style={"fontSize":"0.72rem"})],
                "tab-sparql",{"display":"block"},sparql)

    # ── Execute Query (btn-execute) ───────────────────────────────────────────
    if not measure:
        return ({"sparql":sparql},
                dbc.Badge("⚠ Select a measure first (click one in the left panel).",
                          color="warning", style={"fontSize":"0.72rem"}),
                "tab-sparql",{"display":"block"},sparql)

    with _lock: df=_state.get("obs_df")

    if df is None:
        return ({"sparql":sparql},
                dbc.Badge("⚠ Load ABox to run query.",color="danger",
                          style={"fontSize":"0.72rem"}),
                "tab-sparql",{"display":"block"},sparql)

    t0=time.time()
    filters={k:v for k,v in (instances or {}).items() if v}
    res=run_olap_query(df, levels=levels or [], measure=measure, agg_fn=agg or "avg", filters=filters)
    ms_=int((time.time()-t0)*1000)
    fn=" | ".join(f"{k}: {','.join(str(v) for v in vs[:3])}{'…' if len(vs)>3 else ''}"
                  for k,vs in filters.items())
    info=[dbc.Badge(f"{ms_} ms",color="info",className="me-1",style={"fontSize":"0.71rem"}),
          html.Span(f"{len(res):,} rows",style={"fontSize":"0.74rem","color":"#444"}),
          html.Span(f"  {fn}",style={"fontSize":"0.72rem","color":"#888"}) if fn else ""]
    if res.empty:
        return ({"sparql":sparql,"cols":[],"data":[]},info,
                "tab-table",{"display":"block"},sparql)
    return ({"sparql":sparql,"cols":res.columns.tolist(),"data":res.to_dict("records")},
            info,"tab-table",{"display":"block"},sparql)

@app.callback(
    Output("result-container","children"),
    Input("store-result","data"),Input("result-tabs","active_tab"),
    prevent_initial_call=True,
)
def cb_render(data,tab):
    if not data:
        return html.Div("Click Generate SPARQL or Execute Query.",
                        className="text-muted p-4 text-center")

    if tab=="tab-sparql":
        sparql_text=data.get("sparql","— no SPARQL generated yet —")
        return html.Div([
            html.Div([
                dbc.Button([html.I(className="fa fa-copy me-1"),"Copy SPARQL"],
                           id="btn-copy-sparql-inner",size="sm",color="secondary",
                           outline=True,n_clicks=0,
                           style={"fontSize":"0.78rem","padding":"3px 10px","marginBottom":"6px"}),
                html.Span(id="copy-confirm-inner",
                          style={"color":"#28a745","fontSize":"0.77rem","marginLeft":"8px"}),
            ]),
            html.Pre(id="sparql-text-inner",children=sparql_text,
                style={"backgroundColor":"#1e2a38","color":"#7ec8e3",
                       "borderRadius":"6px","padding":"14px",
                       "fontSize":"0.8rem","overflowX":"auto",
                       "whiteSpace":"pre-wrap","wordBreak":"break-word",
                       "margin":"0","maxHeight":"340px","lineHeight":"1.5"}),
        ])

    cols=data.get("cols",[]); rows=data.get("data",[])
    if not rows:
        return html.Div("No results — run Execute Query first.",
                        className="text-muted p-4 text-center")
    df=pd.DataFrame(rows,columns=cols)

    if tab=="tab-table":
        return html.Div([
            dash_table.DataTable(
                columns=[{"name":c,"id":c} for c in cols],data=rows,page_size=25,
                page_action="native",sort_action="native",filter_action="native",
                style_table={"overflowX":"auto","borderRadius":"4px","overflow":"hidden"},
                style_header={"backgroundColor":C["nav"],"color":"white","fontWeight":"700",
                              "fontSize":"0.82rem","padding":"8px 10px",
                              "borderBottom":f"2px solid {C['accent']}"},
                style_cell={"textAlign":"left","padding":"6px 10px","fontSize":"0.82rem",
                            "border":f"1px solid {C['border']}","fontFamily":"inherit"},
                style_data_conditional=[{"if":{"row_index":"odd"},"backgroundColor":"#f7fafd"}],
                style_filter={"fontSize":"0.78rem"},
            ),
            html.Div(f"Rows per page: 25  •  1–{min(25,len(rows))} of {len(rows):,}",
                     style={"fontSize":"0.73rem","color":"#777","textAlign":"right",
                            "marginTop":"5px"}),
        ])

    if tab=="tab-graph":
        num_cols=[c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        cat_cols=[c for c in cols if c not in num_cols]
        if not num_cols:
            return html.Div("No numeric measure to plot.",className="p-4 text-muted")
        mc=num_cols[0]
        if not cat_cols:
            fig=go.Figure(go.Indicator(mode="number",value=df[mc].sum(),title={"text":mc}))
        elif len(cat_cols)==1:
            fig=px.bar(df,x=cat_cols[0],y=mc,color=cat_cols[0],
                       color_discrete_sequence=px.colors.qualitative.Bold,
                       template="plotly_white",title=f"{mc} by {cat_cols[0]}")
        elif len(cat_cols)==2:
            fig=px.bar(df,x=cat_cols[0],y=mc,color=cat_cols[1],barmode="group",
                       template="plotly_white",title=f"{mc} by {cat_cols[0]} / {cat_cols[1]}")
        else:
            fig=px.scatter(df,x=cat_cols[0],y=mc,
                           color=cat_cols[1] if len(cat_cols)>1 else None,
                           template="plotly_white",title=f"{mc} scatter")
        fig.update_layout(plot_bgcolor=C["bg"],paper_bgcolor="white",
                          font={"family":"Segoe UI,sans-serif","color":C["text"]},
                          margin={"l":50,"r":20,"t":50,"b":60})
        return dcc.Graph(figure=fig,style={"height":"420px"})

    return html.Div("Select a tab.")

# ══════════════════════════════════════════════════════════════════════════════
if __name__=="__main__":
    print("\n"+"="*60)
    print("  MHKG OLAP  v3.0  —  http://127.0.0.1:8050")
    print("  Single page · SPARQL below panels · mhp: naming.")
    print("  Execute Query works directly — no Generate SPARQL needed.")
    print("="*60+"\n")
    app.run(debug=False,port=8050)