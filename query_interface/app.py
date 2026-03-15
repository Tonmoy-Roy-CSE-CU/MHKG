"""
MentalHealthKG - Interactive SPARQL Query Interface
====================================================
Self-contained single file. No templates/ folder needed.

USAGE:
    pip install flask SPARQLWrapper
    python app.py
    open http://localhost:5000
"""

import json
from flask import Flask, request, jsonify, Response
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

app = Flask(__name__)

VIRTUOSO_ENDPOINT = "http://localhost:8890/sparql"
GRAPH_URI         = "http://mhkg.example.com/graph/mentalhealth"
FG                = "FROM <" + GRAPH_URI + ">"

PFX = (
    "PREFIX mhp:     <http://mhkg.example.com/datasets/mentalhealth/abox/mdProperty#>\n"
    "PREFIX mha:     <http://mhkg.example.com/datasets/mentalhealth/abox/mdAttribute#>\n"
    "PREFIX dataset: <http://mhkg.example.com/datasets/mentalhealth/abox/data#>\n"
    "PREFIX qb:      <http://purl.org/linked-data/cube#>\n"
    "PREFIX owl:     <http://www.w3.org/2002/07/owl#>\n"
    "PREFIX xsd:     <http://www.w3.org/2001/XMLSchema#>\n"
    "PREFIX wdt:     <http://www.wikidata.org/prop/direct/>\n"
)

def q(sparql_body):
    return PFX + "\n" + sparql_body.replace("__FG__", FG)

QUERIES = {
    "Roll-up": {
        "color": "#6366f1",
        "queries": {
            "Q01 - Depression by Continent": {
                "id": "Q01", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Average depression score per country, rolled up to continent?",
                "sparql": q("""SELECT ?continent (COUNT(?country) AS ?num_countries)
       (ROUND(AVG(?avg)*100)/100 AS ?avg_depression)
__FG__
WHERE {
  { SELECT ?country ?continent (AVG(xsd:decimal(?dep)) AS ?avg) WHERE {
      ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
           mhp:Country ?cm ; mhp:depressionScore ?dep .
      ?cm mha:countryName ?country ; mha:inRegion ?rm .
      ?rm mha:inContinent ?contm .
      ?contm mha:continentName ?continent .
    } GROUP BY ?country ?continent }
}
GROUP BY ?continent
ORDER BY DESC(?avg_depression)""")},

            "Q02 - Suicide Rate by WHO Region and Year": {
                "id": "Q02", "cuboid": "mhSuicideCuboid", "federated": False,
                "question": "Average suicide rate per WHO region by year?",
                "sparql": q("""SELECT ?region ?year
       (ROUND(AVG(?rate)*100)/100 AS ?avg_suicide_rate)
       (COUNT(DISTINCT ?country) AS ?countries)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
       mhp:Country ?cm ; mhp:Gender ?gm ; mhp:Year ?ym ; mhp:suicideRate ?rate .
  ?gm mha:gender "Both sexes" .
  ?cm mha:inRegion ?rm ; mha:countryName ?country .
  ?rm mha:regionName ?region .
  ?ym mha:yearValue ?year .
}
GROUP BY ?region ?year
ORDER BY ?region ?year""")},

            "Q03 - Anxiety by Life Stage": {
                "id": "Q03", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Average anxiety score for each life stage across all countries?",
                "sparql": q("""SELECT ?life_stage (COUNT(?obs) AS ?respondents)
       (ROUND(AVG(?anx)*100)/100 AS ?avg_anxiety)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:AgeGroup ?am ; mhp:anxietyScore ?anx ;
       mhp:depressionScore ?dep ; mhp:stressLevel ?str .
  ?am mha:inLifeStage ?lm .
  ?lm mha:lifeStageName ?life_stage .
}
GROUP BY ?life_stage
ORDER BY DESC(?avg_anxiety)""")},
        }
    },

    "Drill-down": {
        "color": "#0ea5e9",
        "queries": {
            "Q04 - Stress per Country by Gender": {
                "id": "Q04", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Average stress level per country, drilled down to gender?",
                "sparql": q("""SELECT ?country ?gender (COUNT(?obs) AS ?respondents)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:Country ?cm ; mhp:Gender ?gm ;
       mhp:stressLevel ?str ; mhp:depressionScore ?dep .
  ?cm mha:countryName ?country .
  ?gm mha:gender ?gender .
}
GROUP BY ?country ?gender
ORDER BY ?country ?gender
LIMIT 60""")},

            "Q05 - Suicide Rate Continent to Country": {
                "id": "Q05", "cuboid": "mhSuicideCuboid", "federated": False,
                "question": "Suicide rate per continent, drilled down to country level?",
                "sparql": q("""SELECT ?continent ?country
       (ROUND(AVG(?rate)*100)/100 AS ?avg_rate)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
       mhp:Country ?cm ; mhp:Gender ?gm ; mhp:suicideRate ?rate .
  ?gm mha:gender "Both sexes" .
  ?cm mha:countryName ?country ; mha:inRegion ?rm .
  ?rm mha:inContinent ?contm .
  ?contm mha:continentName ?continent .
}
GROUP BY ?continent ?country
ORDER BY ?continent DESC(?avg_rate)
LIMIT 60""")},

            "Q06 - Depression by Academic Year and CGPA": {
                "id": "Q06", "cuboid": "mhAcademicCuboid", "federated": False,
                "question": "Average depression per academic year, drilled down to CGPA band?",
                "sparql": q("""SELECT ?academic_year ?cgpa_band (COUNT(?obs) AS ?students)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?anx)*100)/100 AS ?avg_anxiety)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:AcademicYear ?aym ; mhp:CGPACategory ?cgpam ;
       mhp:depressionScore ?dep ; mhp:anxietyScore ?anx ; mhp:stressLevel ?str .
  ?aym mha:academicYearName ?academic_year .
  ?cgpam mha:cgpaCategory ?cgpa_band .
}
GROUP BY ?academic_year ?cgpa_band
ORDER BY ?academic_year ?cgpa_band""")},
        }
    },

    "Slice": {
        "color": "#10b981",
        "queries": {
            "Q07 - Female Depression by Country": {
                "id": "Q07", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Average depression score for Female respondents only?",
                "sparql": q("""SELECT ?country (COUNT(?obs) AS ?respondents)
       (ROUND(AVG(xsd:decimal(?dep))*100)/100 AS ?avg_depression)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:Gender ?gm ; mhp:Country ?cm ; mhp:depressionScore ?dep .
  ?gm mha:gender ?g .
  FILTER(LCASE(STR(?g)) = "female")
  ?cm mha:countryName ?country .
}
GROUP BY ?country
ORDER BY DESC(?avg_depression)
LIMIT 20""")},

            "Q08 - Male Suicide Rate Bangladesh All Years": {
                "id": "Q08", "cuboid": "mhSuicideCuboid", "federated": False,
                "question": "Suicide rate for Male in Bangladesh across all years?",
                "sparql": q("""SELECT ?year ?rate ?rate_low ?rate_high ?value_label
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
       mhp:Country ?cm ; mhp:Gender ?gm ; mhp:Year ?ym ;
       mhp:suicideRate ?rate ; mhp:suicideRateLow ?rate_low ;
       mhp:suicideRateHigh ?rate_high ; mhp:valueLabel ?value_label .
  ?cm mha:countryName "Bangladesh" .
  ?gm mha:gender "Male" .
  ?ym mha:yearValue ?year .
}
ORDER BY ?year""")},

            "Q09 - Anxiety for CGPA 3.5 to 4.0 Students": {
                "id": "Q09", "cuboid": "mhAcademicCuboid", "federated": False,
                "question": "Average anxiety score for students with CGPA 3.5-4.0?",
                "sparql": q("""SELECT ?academic_year (COUNT(?obs) AS ?students)
       (ROUND(AVG(?anx)*100)/100 AS ?avg_anxiety)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:CGPACategory ?cgpam ; mhp:AcademicYear ?aym ;
       mhp:anxietyScore ?anx ; mhp:depressionScore ?dep ; mhp:stressLevel ?str .
  ?cgpam mha:cgpaCategory "3.5-4.0" .
  ?aym mha:academicYearName ?academic_year .
}
GROUP BY ?academic_year
ORDER BY ?academic_year""")},
        }
    },

    "Dice": {
        "color": "#f59e0b",
        "queries": {
            "Q10 - Female Age 16-25 in Asia": {
                "id": "Q10", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Depression and anxiety for Female aged 16-25 in Asia?",
                "sparql": q("""SELECT ?country ?age_group (COUNT(?obs) AS ?respondents)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?anx)*100)/100 AS ?avg_anxiety)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:Gender ?gm ; mhp:AgeGroup ?am ; mhp:Country ?cm ;
       mhp:depressionScore ?dep ; mhp:anxietyScore ?anx .
  ?gm mha:gender "Female" .
  ?am mha:ageGroupRange ?age_group .
  FILTER(?age_group IN ("16-20","21-25"))
  ?cm mha:countryName ?country ; mha:inRegion ?rm .
  ?rm mha:inContinent ?contm .
  ?contm mha:continentName "Asia" .
}
GROUP BY ?country ?age_group
ORDER BY DESC(?avg_depression)""")},

            "Q11 - South Asia Suicide by Sex 2010 to 2021": {
                "id": "Q11", "cuboid": "mhSuicideCuboid", "federated": False,
                "question": "Male vs female suicide rate in South Asia 2010-2021?",
                "sparql": q("""SELECT ?country ?year ?sex ?rate
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
       mhp:Country ?cm ; mhp:Gender ?gm ; mhp:Year ?ym ; mhp:suicideRate ?rate .
  ?cm mha:countryName ?country ; mha:inRegion ?rm .
  ?rm mha:regionName "Southern Asia" .
  ?gm mha:gender ?sex .
  ?ym mha:yearValue ?year .
  FILTER(?sex IN ("Male","Female"))
  FILTER(?year >= 2010 && ?year <= 2021)
}
ORDER BY ?country ?year ?sex""")},

            "Q12 - Corporate Employees in Europe": {
                "id": "Q12", "cuboid": "mhWorkCuboid", "federated": False,
                "question": "Stress and depression for Corporate employees in Europe?",
                "sparql": q("""SELECT ?country ?gender (COUNT(?obs) AS ?respondents)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:Occupation ?om ; mhp:Country ?cm ; mhp:Gender ?gm ;
       mhp:stressLevel ?str ; mhp:depressionScore ?dep .
  ?om mha:occupationName "Corporate" .
  ?cm mha:countryName ?country ; mha:inRegion ?rm .
  ?rm mha:inContinent ?contm .
  ?contm mha:continentName "Europe" .
  ?gm mha:gender ?gender .
}
GROUP BY ?country ?gender
ORDER BY DESC(?avg_stress)""")},
        }
    },

    "Inter-cuboid": {
        "color": "#ec4899",
        "queries": {
            "Q13 - High Depression Low Social Support": {
                "id": "Q13", "cuboid": "mhSurveyCuboid", "federated": False,
                "question": "Countries with avg depression > 2.0 AND social support < 3.0?",
                "sparql": q("""SELECT ?country
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?soc)*100)/100 AS ?avg_social_support)
       (COUNT(?obs) AS ?respondents)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
       mhp:Country ?cm ; mhp:depressionScore ?dep ; mhp:socialSupportScore ?soc .
  ?cm mha:countryName ?country .
  FILTER(BOUND(?dep) && BOUND(?soc))
}
GROUP BY ?country
HAVING (AVG(?dep) > 2.0 && AVG(?soc) < 3.0)
ORDER BY DESC(?avg_depression)""")},

            "Q14 - Gender Gap Suicide by Continent": {
                "id": "Q14", "cuboid": "mhSuicideCuboid", "federated": False,
                "question": "Gender gap (Male minus Female) in suicide rate by continent?",
                "sparql": q("""SELECT ?continent
       (ROUND(AVG(?mr)*100)/100 AS ?avg_male_rate)
       (ROUND(AVG(?fr)*100)/100 AS ?avg_female_rate)
       (ROUND((AVG(?mr)-AVG(?fr))*100)/100 AS ?gender_gap)
__FG__
WHERE {
  ?obsM a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
        mhp:Country ?cm ; mhp:Gender ?gm ; mhp:suicideRate ?mr .
  ?gm mha:gender "Male" .
  ?obsF a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
        mhp:Country ?cm ; mhp:Gender ?gf ; mhp:suicideRate ?fr .
  ?gf mha:gender "Female" .
  ?cm mha:inRegion ?rm .
  ?rm mha:inContinent ?contm .
  ?contm mha:continentName ?continent .
}
GROUP BY ?continent
ORDER BY DESC(?gender_gap)""")},

            "Q15 - CGPA Band vs Mental Health": {
                "id": "Q15", "cuboid": "mhAcademicCuboid", "federated": False,
                "question": "Correlation between CGPA band and depression, anxiety, stress?",
                "sparql": q("""SELECT ?cgpa_group (COUNT(?obs) AS ?responses)
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?anx)*100)/100 AS ?avg_anxiety)
       (ROUND(AVG(?str)*100)/100 AS ?avg_stress)
__FG__
WHERE {
  ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset .
  OPTIONAL { ?obs mhp:CGPACategory ?cgpam . ?cgpam mha:cgpaCategory ?cgpa_band . }
  OPTIONAL { ?obs mhp:depressionScore ?dep . }
  OPTIONAL { ?obs mhp:anxietyScore ?anx . }
  OPTIONAL { ?obs mhp:stressLevel ?str . }
  FILTER(BOUND(?cgpa_band) && ?cgpa_band != "Unknown")
  BIND(IF(?cgpa_band="Below 2.0"||?cgpa_band="2.0-2.5","Below 2.5",?cgpa_band) AS ?cgpa_group)
}
GROUP BY ?cgpa_group
ORDER BY ?cgpa_group""")},

            "Q16 - Depression vs Suicide by Gender": {
                "id": "Q16", "cuboid": "Survey+Suicide", "federated": False,
                "question": "Depression and suicide rate by gender cross-cube?",
                "sparql": q("""SELECT ?gender
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?suicide)*100)/100 AS ?avg_suicide_rate)
       (COUNT(DISTINCT ?sObs) AS ?survey_respondents)
__FG__
WHERE {
  ?sObs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
        mhp:Country ?cm ; mhp:Gender ?gm ; mhp:depressionScore ?dep .
  ?gm mha:gender ?gender .
  FILTER(?gender IN ("Male","Female"))
  ?xObs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
        mhp:Country ?cm ; mhp:Gender ?gm2 ; mhp:suicideRate ?suicide .
  ?gm2 mha:gender ?gender .
}
GROUP BY ?gender
ORDER BY ?gender""")},

            "Q17 - High Depression and High Suicide Countries": {
                "id": "Q17", "cuboid": "Survey+Suicide", "federated": False,
                "question": "Countries with both high depression scores and high suicide rates?",
                "sparql": q("""SELECT ?country ?continent
       (ROUND(AVG(?dep)*100)/100 AS ?avg_depression)
       (ROUND(AVG(?suicide)*100)/100 AS ?avg_suicide_rate)
       (COUNT(DISTINCT ?sObs) AS ?respondents)
__FG__
WHERE {
  ?sObs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
        mhp:Country ?cm ; mhp:depressionScore ?dep .
  ?xObs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
        mhp:Country ?cm ; mhp:Gender ?gm ; mhp:suicideRate ?suicide .
  ?gm mha:gender "Both sexes" .
  ?cm mha:countryName ?country ; mha:inRegion ?rm .
  ?rm mha:inContinent ?contm .
  ?contm mha:continentName ?continent .
  FILTER(?dep > 2.6)
  FILTER(?suicide > 11.5)
}
GROUP BY ?country ?continent
ORDER BY DESC(?avg_depression)
LIMIT 25""")},
        }
    },

    "Federated": {
        "color": "#f97316",
        "queries": {
            "Q18 - Depression vs HDI via Wikidata": {
                "id": "Q18", "cuboid": "Survey+Wikidata", "federated": True,
                "question": "Compare average depression score with national HDI via Wikidata?",
                "sparql": q("""SELECT ?country ?avg_depression ?hdi
__FG__
WHERE {
  { SELECT ?cm ?country (ROUND(AVG(?dep)*100)/100 AS ?avg_depression) WHERE {
      ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
           mhp:Country ?cm ; mhp:depressionScore ?dep .
      ?cm mha:countryName ?country .
    } GROUP BY ?cm ?country }
  ?cm owl:sameAs ?wd .
  FILTER(STRSTARTS(STR(?wd),"http://www.wikidata.org/entity/"))
  SERVICE <https://query.wikidata.org/sparql> { OPTIONAL { ?wd wdt:P1081 ?hdi . } }
  FILTER(BOUND(?hdi))
}
ORDER BY DESC(?avg_depression)""")},

            "Q19 - Suicide Rate vs Population Density via Wikidata": {
                "id": "Q19", "cuboid": "Suicide+Wikidata", "federated": True,
                "question": "Suicide rate vs population density per country via Wikidata?",
                "sparql": q("""SELECT ?country ?avg_suicide_rate ?population ?area_km2
       (?population / ?area_km2 AS ?pop_density)
__FG__
WHERE {
  { SELECT ?cm ?country (ROUND(AVG(?rate)*100)/100 AS ?avg_suicide_rate) WHERE {
      ?obs a qb:Observation ; qb:dataSet dataset:mhSuicideDataset ;
           mhp:Country ?cm ; mhp:Gender ?gm ; mhp:suicideRate ?rate .
      ?gm mha:gender "Both sexes" .
      ?cm mha:countryName ?country .
    } GROUP BY ?cm ?country }
  ?cm owl:sameAs ?wd .
  FILTER(STRSTARTS(STR(?wd),"http://www.wikidata.org/entity/"))
  SERVICE <https://query.wikidata.org/sparql> {
    OPTIONAL { ?wd wdt:P1082 ?population . }
    OPTIONAL { ?wd wdt:P2046 ?area_km2 . }
  }
  FILTER(BOUND(?population) && BOUND(?area_km2) && ?area_km2 > 0)
}
ORDER BY DESC(?avg_suicide_rate)""")},

            "Q20 - Stress vs Health Expenditure GDP via Wikidata": {
                "id": "Q20", "cuboid": "Survey+WHO", "federated": True,
                "question": "Stress levels vs WHO health expenditure percent GDP via Wikidata?",
                "sparql": q("""SELECT ?country ?avg_stress ?health_exp_pct_gdp
__FG__
WHERE {
  { SELECT ?cm ?country (ROUND(AVG(?str)*100)/100 AS ?avg_stress) WHERE {
      ?obs a qb:Observation ; qb:dataSet dataset:mhSurveyDataset ;
           mhp:Country ?cm ; mhp:stressLevel ?str .
      ?cm mha:countryName ?country .
    } GROUP BY ?cm ?country }
  ?cm owl:sameAs ?wd .
  FILTER(STRSTARTS(STR(?wd),"http://www.wikidata.org/entity/"))
  SERVICE <https://query.wikidata.org/sparql> {
    OPTIONAL { ?wd wdt:P2352 ?health_exp_pct_gdp . }
  }
  FILTER(BOUND(?health_exp_pct_gdp))
}
ORDER BY DESC(?avg_stress)""")},
        }
    },
}


def get_meta():
    return {
        cat: {
            "color": info["color"],
            "queries": {
                name: {k: v[k] for k in ("id","cuboid","federated","question")}
                for name, v in info["queries"].items()
            }
        }
        for cat, info in QUERIES.items()
    }


def run_sparql(query):
    try:
        s = SPARQLWrapper(VIRTUOSO_ENDPOINT)
        s.setQuery(query)
        s.setReturnFormat(SPARQL_JSON)
        return {"ok": True, "data": s.query().convert()}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# HTML is a module-level string with %%META%% as placeholder
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MentalHealthKG - SPARQL Interface</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg0:#07090f;--bg1:#0d1117;--bg2:#161b27;--bg3:#1e2536;
  --bd:#2d3a52;--bd2:#3d4f6e;
  --tx:#dce4f0;--tx2:#8898b4;--tx3:#4d6080;
  --ac:#38bdf8;--ac2:#0284c7;
  --gr:#22d3a0;--am:#fbbf24;--rd:#f87171;--pu:#a78bfa;
  --font:'Space Grotesk',sans-serif;--mono:'JetBrains Mono',monospace;
}
body{font-family:var(--font);background:var(--bg0);color:var(--tx);min-height:100vh;
  background-image:radial-gradient(ellipse 80% 50% at 50% -20%,rgba(56,189,248,.07),transparent)}
.shell{display:grid;grid-template-columns:280px 1fr;min-height:100vh}

/* ---- sidebar ---- */
.sidebar{background:var(--bg1);border-right:1px solid var(--bd);
  display:flex;flex-direction:column;
  position:sticky;top:0;height:100vh;overflow-y:auto;
  scrollbar-width:thin;scrollbar-color:var(--bd2) transparent}
.sb-top{padding:1.1rem 1rem .9rem;border-bottom:1px solid var(--bd)}
.brand{display:flex;align-items:center;gap:.6rem;margin-bottom:.6rem}
.brand-ico{width:32px;height:32px;border-radius:8px;
  background:linear-gradient(135deg,var(--ac),var(--pu));
  display:grid;place-items:center;font-size:15px}
.brand-name{font-size:.88rem;font-weight:700}
.brand-sub{font-size:.58rem;color:var(--tx3);font-family:var(--mono);
  letter-spacing:.1em;text-transform:uppercase}
.pills{display:flex;gap:.35rem;flex-wrap:wrap;margin-bottom:.65rem}
.pill{font-family:var(--mono);font-size:.6rem;padding:.15rem .42rem;
  border-radius:99px;letter-spacing:.04em}
.pb{background:rgba(56,189,248,.12);color:var(--ac);border:1px solid rgba(56,189,248,.22)}
.pg{background:rgba(34,211,160,.12);color:var(--gr);border:1px solid rgba(34,211,160,.22)}
.pa{background:rgba(251,191,36,.12);color:var(--am);border:1px solid rgba(251,191,36,.22)}
.sg{display:grid;grid-template-columns:1fr 1fr;gap:1px;background:var(--bd);border-radius:7px;overflow:hidden}
.sc{background:var(--bg2);padding:.48rem;text-align:center}
.sn{font-family:var(--mono);font-size:1.1rem;font-weight:600;color:var(--ac);display:block;line-height:1}
.sl{font-size:.6rem;color:var(--tx3);margin-top:.12rem;letter-spacing:.04em;text-transform:uppercase}

.nav{padding:.5rem .55rem}
.nh{font-family:var(--mono);font-size:.6rem;color:var(--tx3);
  letter-spacing:.1em;text-transform:uppercase;padding:.35rem .4rem .2rem}

/* category buttons */
.cb{width:100%;display:flex;align-items:center;gap:.55rem;
  padding:.5rem .6rem;border-radius:6px;background:none;
  border:1px solid transparent;color:var(--tx2);
  font-family:var(--font);font-size:.78rem;font-weight:500;
  cursor:pointer;text-align:left;transition:all .14s}
.cb:hover{background:var(--bg3);color:var(--tx);border-color:var(--bd)}
.cb.on{background:var(--bg3);color:var(--tx);border-color:var(--bd2)}
.cd{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.cl{flex:1}
.cn{font-family:var(--mono);font-size:.6rem;padding:.07rem .3rem;
  border-radius:3px;background:rgba(255,255,255,.06);color:var(--tx3)}

/* query buttons */
.ql{padding:.15rem .55rem .45rem;display:flex;flex-direction:column;gap:.16rem}
.qb{width:100%;display:flex;align-items:flex-start;gap:.42rem;
  padding:.44rem .58rem;border-radius:5px;background:none;
  border:1px solid transparent;color:var(--tx3);
  font-family:var(--font);font-size:.72rem;
  cursor:pointer;text-align:left;transition:all .14s;line-height:1.3}
.qb:hover{background:var(--bg3);color:var(--tx2);border-color:var(--bd)}
.qb.on{background:rgba(56,189,248,.07);color:var(--ac);border-color:rgba(56,189,248,.2)}
.qi{font-family:var(--mono);font-size:.6rem;padding:.03rem .26rem;border-radius:3px;
  background:rgba(56,189,248,.1);color:var(--ac);flex-shrink:0;margin-top:.07rem}
.qf{font-family:var(--mono);font-size:.56rem;padding:.03rem .22rem;border-radius:3px;
  background:rgba(251,191,36,.1);color:var(--am);flex-shrink:0;margin-top:.07rem}

/* ---- main ---- */
.main{display:flex;flex-direction:column;min-height:100vh}
.topbar{padding:.68rem 1.4rem;border-bottom:1px solid var(--bd);
  background:rgba(13,17,23,.88);backdrop-filter:blur(10px);
  position:sticky;top:0;z-index:10;
  display:flex;align-items:center;gap:.8rem}
.tq{flex:1;font-size:.8rem;color:var(--tx2);font-style:italic;
  overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ttags{display:flex;gap:.32rem;flex-shrink:0}
.tag{font-family:var(--mono);font-size:.61rem;padding:.16rem .44rem;border-radius:4px}
.tb2{background:rgba(56,189,248,.1);color:var(--ac);border:1px solid rgba(56,189,248,.2)}
.ta2{background:rgba(251,191,36,.1);color:var(--am);border:1px solid rgba(251,191,36,.2)}

.content{flex:1;padding:1.1rem 1.3rem;display:flex;flex-direction:column;gap:.85rem}
.card{background:var(--bg1);border:1px solid var(--bd);border-radius:9px;overflow:hidden}
.cbar{display:flex;align-items:center;gap:.52rem;
  padding:.5rem .8rem;background:var(--bg2);border-bottom:1px solid var(--bd)}
.ct{font-family:var(--mono);font-size:.62rem;color:var(--tx3);
  letter-spacing:.08em;text-transform:uppercase;flex:1}
.btn{display:inline-flex;align-items:center;gap:.3rem;
  padding:.34rem .72rem;border-radius:6px;font-family:var(--font);
  font-size:.72rem;font-weight:600;cursor:pointer;transition:all .14s;
  border:1px solid transparent;white-space:nowrap;line-height:1}
.brun{background:var(--ac2);color:#fff;border-color:var(--ac);
  box-shadow:0 0 12px rgba(14,132,200,.2)}
.brun:hover{background:var(--ac);box-shadow:0 0 18px rgba(56,189,248,.3)}
.brun:disabled{opacity:.4;cursor:not-allowed}
.bgh{background:none;color:var(--tx2);border-color:var(--bd)}
.bgh:hover{background:var(--bg3);color:var(--tx);border-color:var(--bd2)}
.brd{background:none;color:var(--rd);border-color:rgba(248,113,113,.2)}
.brd:hover{background:rgba(248,113,113,.06)}
.kh{font-family:var(--mono);font-size:.59rem;padding:.09rem .3rem;
  border:1px solid var(--bd2);border-radius:3px;color:var(--tx3)}

/* editor */
.ew{display:flex}
.lnum{padding:.8rem .58rem;background:rgba(0,0,0,.2);border-right:1px solid var(--bd);
  font-family:var(--mono);font-size:.72rem;line-height:1.55;color:var(--tx3);
  text-align:right;user-select:none;min-width:36px;white-space:pre;overflow:hidden}
#qed{flex:1;padding:.8rem;background:transparent;border:none;color:var(--tx);
  font-family:var(--mono);font-size:.72rem;line-height:1.55;
  resize:vertical;min-height:200px;outline:none;tab-size:2;
  white-space:pre;overflow-wrap:normal;overflow-x:auto}
#qed::selection{background:rgba(56,189,248,.18)}
#qed::placeholder{color:var(--tx3)}
.fedbar{display:none;align-items:center;gap:.45rem;padding:.5rem .8rem;
  background:rgba(251,191,36,.05);border-top:1px solid rgba(251,191,36,.14);
  font-size:.71rem;color:var(--am)}
.fedbar.on{display:flex}
.errbar{display:none;align-items:center;gap:.42rem;padding:.58rem .9rem;
  background:rgba(248,113,113,.05);border:1px solid rgba(248,113,113,.18);
  border-radius:8px;font-family:var(--mono);font-size:.7rem;color:var(--rd)}
.errbar.on{display:flex}

/* results */
.rs{overflow-x:auto;max-height:380px;overflow-y:auto}
.empty{padding:2.2rem;text-align:center;color:var(--tx3)}
.ei{font-size:1.7rem;display:block;margin-bottom:.45rem;opacity:.28}
.et{font-family:var(--mono);font-size:.73rem}
table{width:100%;border-collapse:collapse}
thead th{position:sticky;top:0;z-index:2;background:var(--bg3);color:var(--tx2);
  font-family:var(--mono);font-size:.63rem;font-weight:600;text-transform:uppercase;
  letter-spacing:.06em;padding:.5rem .8rem;text-align:left;
  border-bottom:2px solid var(--bd2);white-space:nowrap;
  cursor:pointer;user-select:none;transition:color .14s}
thead th:hover{color:var(--ac)}
.si{margin-left:.2rem;opacity:.28;font-size:.75em}
.si::after{content:'\\21C5'}
thead th.sa .si{opacity:1} thead th.sa .si::after{content:'\\2191'}
thead th.sd .si{opacity:1} thead th.sd .si::after{content:'\\2193'}
tbody td{padding:.4rem .8rem;border-bottom:1px solid rgba(45,58,82,.4);
  color:var(--tx2);font-family:var(--mono);font-size:.69rem;
  max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
tbody tr:hover td{background:rgba(56,189,248,.03);color:var(--tx)}
tbody td:first-child{color:var(--tx);font-weight:500}
.nr{text-align:right}
#rstat{font-family:var(--mono);font-size:.65rem;color:var(--tx2)}

.spin{display:none;width:13px;height:13px;
  border:2px solid rgba(56,189,248,.12);border-top-color:var(--ac);
  border-radius:50%;animation:sp .6s linear infinite}
.spin.on{display:inline-block}
@keyframes sp{to{transform:rotate(360deg)}}
@keyframes fi{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.fi{animation:fi .18s ease}
::-webkit-scrollbar{width:4px;height:4px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:2px}
footer{padding:.6rem 1.3rem;border-top:1px solid var(--bd);
  font-family:var(--mono);font-size:.61rem;color:var(--tx3);text-align:center}
@media(max-width:800px){.shell{grid-template-columns:1fr}.sidebar{position:static;height:auto}}
</style>
</head>
<body>
<div class="shell">

<aside class="sidebar">
  <div class="sb-top">
    <div class="brand">
      <div class="brand-ico">&#x1F9E0;</div>
      <div>
        <div class="brand-name">MentalHealthKG</div>
        <div class="brand-sub">SPARQL Interface</div>
      </div>
    </div>
    <div class="pills">
      <span class="pill pb">QB4OLAP</span>
      <span class="pill pg">20 CQs</span>
      <span class="pill pa">Virtuoso</span>
    </div>
    <div class="sg">
      <div class="sc"><span class="sn">3</span><div class="sl">Cubes</div></div>
      <div class="sc"><span class="sn">20</span><div class="sl">Queries</div></div>
      <div class="sc"><span class="sn">6</span><div class="sl">Types</div></div>
      <div class="sc"><span class="sn">3</span><div class="sl">Federated</div></div>
    </div>
  </div>
  <div class="nav">
    <div class="nh">Query Categories</div>
    <div id="cats"></div>
  </div>
  <div class="nav" id="qlwrap" style="display:none">
    <div class="nh" id="qlhead">Queries</div>
    <div class="ql" id="qlist"></div>
  </div>
</aside>

<div class="main">
  <div class="topbar">
    <div class="tq" id="tq">Select a category, then pick a query from the sidebar</div>
    <div class="ttags" id="ttags"></div>
  </div>
  <div class="content">
    <div class="card">
      <div class="cbar">
        <span class="ct">SPARQL Editor</span>
        <span class="kh">Ctrl+Enter</span>
        <button class="btn bgh" id="bcopy">&#x1F4CB; Copy</button>
        <button class="btn brd" id="bclr">&#x1F5D1; Clear</button>
        <button class="btn brun" id="brun">
          <span class="spin" id="spin"></span>&#9654; Run Query
        </button>
      </div>
      <div class="ew">
        <div class="lnum" id="lnum">1</div>
        <textarea id="qed" spellcheck="false"
          placeholder="-- Select a query from the sidebar, or write your own SPARQL&#10;-- Ctrl+Enter to execute"></textarea>
      </div>
      <div class="fedbar" id="fedbar">
        &#9888; <strong>Federated query</strong> &mdash; requires Wikidata SERVICE enabled in Virtuoso
        (ExternalQuerySource=1 in virtuoso.ini)
      </div>
    </div>

    <div class="errbar" id="errbar"><span>&#10006;</span><span id="errmsg"></span></div>

    <div class="card">
      <div class="cbar">
        <span class="ct">Results</span>
        <span id="rstat"></span>
        <button class="btn bgh" id="bcsv" style="display:none">&#8595; CSV</button>
      </div>
      <div class="rs" id="rbody">
        <div class="empty">
          <span class="ei">&#9889;</span>
          <div class="et">Run a query to see results</div>
        </div>
      </div>
    </div>
  </div>
  <footer>MentalHealthKG &nbsp;&middot;&nbsp; Competency Queries Q01&ndash;Q20 &nbsp;&middot;&nbsp;
    <em>MentalHealthKG: A Mental Health Knowledge Graph for Multidimensional Analytics</em>
  </footer>
</div>
</div>

<script>
var META = %%META%%;
var lastR = null, lastC = null;
var $cats  = document.getElementById('cats');
var $qlw   = document.getElementById('qlwrap');
var $qlh   = document.getElementById('qlhead');
var $ql    = document.getElementById('qlist');
var $tq    = document.getElementById('tq');
var $tt    = document.getElementById('ttags');
var $qed   = document.getElementById('qed');
var $ln    = document.getElementById('lnum');
var $brun  = document.getElementById('brun');
var $bcopy = document.getElementById('bcopy');
var $bclr  = document.getElementById('bclr');
var $bcsv  = document.getElementById('bcsv');
var $spin  = document.getElementById('spin');
var $rb    = document.getElementById('rbody');
var $rs    = document.getElementById('rstat');
var $eb    = document.getElementById('errbar');
var $em    = document.getElementById('errmsg');
var $fb    = document.getElementById('fedbar');

// Build category buttons
Object.keys(META).forEach(function(cat) {
  var info = META[cat];
  var n = Object.keys(info.queries).length;
  var b = document.createElement('button');
  b.className = 'cb';
  b.innerHTML =
    '<span class="cd" style="background:' + info.color + '"></span>' +
    '<span class="cl">' + cat + '</span>' +
    '<span class="cn">' + n + '</span>';
  b.addEventListener('click', function() { openCat(cat, b); });
  $cats.appendChild(b);
});

function openCat(cat, el) {
  document.querySelectorAll('.cb').forEach(function(b) { b.classList.remove('on'); });
  el.classList.add('on');
  $qlh.textContent = cat;
  $ql.innerHTML = '';
  var info = META[cat];
  Object.keys(info.queries).forEach(function(name) {
    var q = info.queries[name];
    var b = document.createElement('button');
    b.className = 'qb';
    b.innerHTML =
      '<span class="qi">' + q.id + '</span>' +
      (q.federated ? '<span class="qf">FED</span>' : '') +
      '<span style="flex:1">' + name.replace(/^Q[0-9]+ - /, '') + '</span>';
    b.addEventListener('click', function() { pickQ(cat, name, b); });
    $ql.appendChild(b);
  });
  $qlw.style.display = '';
}

function pickQ(cat, name, el) {
  document.querySelectorAll('.qb').forEach(function(b) { b.classList.remove('on'); });
  el.classList.add('on');
  fetch('/api/qt', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ cat: cat, name: name })
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (!d.ok) return;
    $qed.value = d.sparql.trim();
    updLn();
    $tq.textContent = d.question;
    var col = META[cat].color;
    $tt.innerHTML =
      '<span class="tag tb2">' + d.id + '</span>' +
      '<span class="tag" style="background:' + col + '18;color:' + col + ';border:1px solid ' + col + '33">' + d.cuboid + '</span>' +
      (d.federated ? '<span class="tag ta2">Federated</span>' : '');
    $fb.classList[d.federated ? 'add' : 'remove']('on');
    hideErr();
    clearRes();
  });
}

function updLn() {
  var lines = $qed.value.split('\\n').length;
  var nums = [];
  for (var i = 1; i <= lines; i++) nums.push(i);
  $ln.textContent = nums.join('\\n');
  $qed.style.minHeight = Math.max(200, lines * 18.5 + 30) + 'px';
}

$qed.addEventListener('input', updLn);
$qed.addEventListener('scroll', function() { $ln.scrollTop = $qed.scrollTop; });
$qed.addEventListener('keydown', function(e) {
  if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); runQ(); }
  if (e.key === 'Tab') {
    e.preventDefault();
    var s = $qed.selectionStart, en = $qed.selectionEnd;
    $qed.value = $qed.value.substring(0, s) + '  ' + $qed.value.substring(en);
    $qed.selectionStart = $qed.selectionEnd = s + 2;
    updLn();
  }
});

function runQ() {
  var query = $qed.value.trim();
  if (!query) return;
  hideErr();
  setLoad(true);
  fetch('/api/run', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: query })
  }).then(function(r) { return r.json(); }).then(function(d) {
    setLoad(false);
    if (d.ok) renderRes(d.data);
    else showErr(d.error);
  }).catch(function(e) { setLoad(false); showErr(e.message); });
}

function renderRes(data) {
  var cols = data.head.vars;
  var rows = data.results.bindings;
  lastC = cols; lastR = rows;
  if (!rows.length) {
    $rb.innerHTML = '<div class="empty"><span class="ei">&#128269;</span><div class="et">0 rows returned</div></div>';
    $rs.textContent = '0 rows';
    $bcsv.style.display = 'none';
    return;
  }
  var tbl = document.createElement('table');
  var thead = document.createElement('thead');
  var hr = document.createElement('tr');
  cols.forEach(function(c) {
    var th = document.createElement('th');
    th.innerHTML = c + ' <span class="si"></span>';
    th.addEventListener('click', function() { sortBy(c, th); });
    hr.appendChild(th);
  });
  thead.appendChild(hr);
  tbl.appendChild(thead);
  var tbody = document.createElement('tbody');
  rows.forEach(function(row) {
    var tr = document.createElement('tr');
    cols.forEach(function(c) {
      var td = document.createElement('td');
      var v = row[c] ? row[c].value : 'NULL';
      td.textContent = v;
      td.title = v;
      if (v !== 'NULL' && !isNaN(parseFloat(v))) td.classList.add('nr');
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  tbl.appendChild(tbody);
  $rb.innerHTML = '';
  $rb.appendChild(tbl);
  $rs.textContent = rows.length + ' rows \u00b7 ' + cols.length + ' cols';
  $bcsv.style.display = '';
}

var _sc = null, _sa = true;
function sortBy(col, th) {
  if (_sc === col) _sa = !_sa; else { _sc = col; _sa = true; }
  document.querySelectorAll('thead th').forEach(function(h) { h.classList.remove('sa','sd'); });
  th.classList.add(_sa ? 'sa' : 'sd');
  var tbody = document.querySelector('tbody');
  var trs = Array.from(tbody.querySelectorAll('tr'));
  var ci = Array.from(document.querySelectorAll('thead th')).indexOf(th);
  trs.sort(function(a, b) {
    var av = a.cells[ci].textContent, bv = b.cells[ci].textContent;
    var an = parseFloat(av), bn = parseFloat(bv);
    var cmp = (!isNaN(an) && !isNaN(bn)) ? an - bn : av.localeCompare(bv);
    return _sa ? cmp : -cmp;
  });
  trs.forEach(function(r) { tbody.appendChild(r); });
}

$bcsv.addEventListener('click', function() {
  if (!lastC) return;
  var lines = [lastC.join(',')];
  lastR.forEach(function(row) {
    lines.push(lastC.map(function(c) {
      var v = row[c] ? row[c].value : '';
      return (v.indexOf(',') >= 0 || v.indexOf('"') >= 0)
        ? '"' + v.replace(/"/g, '""') + '"' : v;
    }).join(','));
  });
  var a = document.createElement('a');
  a.href = URL.createObjectURL(new Blob([lines.join('\\n')], { type: 'text/csv' }));
  a.download = 'mhkg_' + Date.now() + '.csv';
  a.click();
});

$brun.addEventListener('click', runQ);
$bclr.addEventListener('click', clearRes);
$bcopy.addEventListener('click', function() {
  navigator.clipboard.writeText($qed.value).then(function() {
    $bcopy.textContent = '\\u2713 Copied';
    setTimeout(function() { $bcopy.innerHTML = '&#x1F4CB; Copy'; }, 1400);
  });
});

function clearRes() {
  $rb.innerHTML = '<div class="empty"><span class="ei">&#9889;</span><div class="et">Run a query to see results</div></div>';
  $rs.textContent = '';
  $bcsv.style.display = 'none';
  lastR = lastC = null;
}
function setLoad(b) { $brun.disabled = b; $spin.classList[b ? 'add' : 'remove']('on'); }
function showErr(m) { $em.textContent = m; $eb.classList.add('on'); }
function hideErr() { $eb.classList.remove('on'); }

updLn();
</script>
</body>
</html>"""


@app.route('/')
def index():
    page = HTML.replace('%%META%%', json.dumps(get_meta(), ensure_ascii=False))
    return Response(page, mimetype='text/html')

@app.route('/api/qt', methods=['POST'])
def api_qt():
    d    = request.json or {}
    cat  = d.get('cat', '')
    name = d.get('name', '')
    if cat in QUERIES and name in QUERIES[cat]['queries']:
        q2 = QUERIES[cat]['queries'][name]
        return jsonify(ok=True, sparql=q2['sparql'], id=q2['id'],
                       cuboid=q2['cuboid'], federated=q2['federated'],
                       question=q2['question'])
    return jsonify(ok=False, error='Query not found')

@app.route('/api/run', methods=['POST'])
def api_run():
    d = request.json or {}
    query = d.get('query', '').strip()
    if not query:
        return jsonify(ok=False, error='Empty query')
    return jsonify(**run_sparql(query))

if __name__ == '__main__':
    print('=' * 58)
    print('  MentalHealthKG - SPARQL Query Interface')
    print('  Endpoint : ' + VIRTUOSO_ENDPOINT)
    print('  Graph    : ' + GRAPH_URI)
    print('  Open     : http://localhost:5000')
    print('=' * 58)
    app.run(debug=True, port=5000)