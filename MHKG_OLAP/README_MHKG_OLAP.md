# MHKG OLAP Interface — Setup & Usage

## Quick Start

```bash
# 1. Install dependencies
pip install dash dash-bootstrap-components rdflib pandas plotly

# 2. Run the app
python mhkg_olap.py

# 3. Open in browser
http://127.0.0.1:8050
```

## Features

| Feature | Details |
|---|---|
| TBox Upload | Drag & drop any QB4OLAP `.ttl` TBox — extracts datasets, cuboids, dimensions, measures |
| ABox Upload | Supports large files (tested to 400 MB) via background-thread streaming parse |
| Instance Filtering | Select any dimension level → filter instances with Equal/Contains/Not-equal |
| OLAP Query | Multi-level GROUP BY with avg/sum/count/min/max aggregation |
| Results — Tabular | Paginated, sortable, filterable data table |
| Results — Graphical | Auto bar/scatter charts with Plotly |
| Results — SPARQL | Generated SPARQL query you can copy to Virtuoso / Fuseki / GraphDB |

## Usage Walkthrough

1. **Upload TBox** (`mhkg_tbox.ttl`) → datasets populate the dropdown
2. **Upload ABox** (`mhkg_abox.ttl` or your 400 MB file) → progress bar
3. **Select Dataset** (e.g. `mhSurveyDataset`)
4. **Dimension Level** → pick e.g. `Country`
5. **Filter** → check Denmark, Germany, Bangladesh etc.
6. **Levels checklist** (right panel) → tick which dimensions to group by
7. **Measure + Aggregate** → e.g. `depressionScore` + `avg`
8. Click **Execute Query** → results appear in Tabular/Graphical/SPARQL tabs

## Large ABox Support

The ABox parser uses rdflib's streaming triple iterator plus immediate
DataFrame conversion so memory is bounded.  For the full 400 MB file:
- Parsing: ~2–4 minutes (single core)
- Memory: ~1–1.5 GB peak
- Query after load: <1 second

## Dependency Versions

```
dash>=2.14
dash-bootstrap-components>=1.5
rdflib>=6.3
pandas>=2.0
plotly>=5.18
```
