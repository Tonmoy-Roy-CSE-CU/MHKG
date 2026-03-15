# Mental Health Knowledge Graph - Interactive Query Interface

Beautiful web-based SPARQL query interface similar to bike-csecu.com

## 🎯 Features

- ✨ **Beautiful UI** - Modern, professional interface with dark theme editor
- 📊 **7 Query Categories** - Slice, Dice, Drill-Down, Roll-Up, Weather, Suicide, Exploratory
- 🚀 **35+ Predefined Queries** - Ready-to-use queries for all your thesis findings
- 💻 **Live Query Execution** - Real-time SPARQL execution on Virtuoso
- 📋 **Copy & Paste** - Easy query copying and editing
- 📈 **Interactive Results** - Sortable tables with hover effects
- ⌨️ **Keyboard Shortcuts** - Ctrl+Enter to run queries

## 📸 Interface Preview

The interface includes:
- **Dropdown menus** for query type and query selection
- **Code editor** with syntax highlighting and line numbers
- **Results table** with sticky headers and hover effects
- **Statistics cards** showing dataset overview
- **Run, Copy, Clear buttons** for easy interaction

## 🛠️ Installation

### Prerequisites

```bash
pip install flask SPARQLWrapper
```

### File Structure

```
mental_health_query_interface/
├── app.py                  # Flask backend
├── templates/
│   └── index.html         # HTML interface
└── README.md              # This file
```

## 🚀 Quick Start

### Step 1: Start Virtuoso

Make sure your Virtuoso server is running with the Mental Health Knowledge Graph loaded:

```bash
# Check if Virtuoso is running
ps aux | grep virtuoso

# If not running, start it
sudo /etc/init.d/virtuoso-opensource-7 start
```

### Step 2: Run the Application

```bash
python app.py
```

You should see:

```
================================================================================
Mental Health Knowledge Graph - Interactive Query Interface
================================================================================

Starting server...
Access the interface at: http://localhost:5000

Press CTRL+C to stop the server
================================================================================
```

### Step 3: Open in Browser

Navigate to: **http://localhost:5000**

## 📋 How to Use

### Running Predefined Queries

1. **Select Query Type** from first dropdown (e.g., "Slice")
2. **Select a Query** from second dropdown (e.g., "Screen Time Impact")
3. Query loads automatically in the editor
4. Click **RUN QUERY** button (or press Ctrl+Enter)
5. Results appear in the table below

### Writing Custom Queries

1. Click in the **Query Editor** text area
2. Write your SPARQL query
3. Click **RUN QUERY** to execute
4. Use **COPY** button to copy query to clipboard

### Query Categories

#### 1. **Slice** (Fix one dimension)
- Gender-based Mental Health Analysis
- Screen Time Impact (STRONGEST FINDING)
- Social Support Impact (Most Reliable)
- Occupation-based Analysis

#### 2. **Dice** (Multiple filters)
- Female Students with High Screen Time
- Severe Depression NOT Seeking Treatment
- Multiple Risk Factors (2+ factors)

#### 3. **Drill Down** (Summary → Detail)
- Country Level Mental Health
- Age Group Breakdown
- Sleep Quality Detailed Analysis

#### 4. **Roll Up** (Detail → Summary)
- Overall Gender Statistics
- Overall Mental Health Statistics

#### 5. **Weather Correlation**
- Mental Health by Temperature
- Seasonal Mental Health Patterns

#### 6. **Suicide Statistics**
- Suicide Rates by Country
- Suicide Trends Over Time
- Country Mental Health & Suicide Integration

#### 7. **Exploratory Query**
- Treatment Seeking by Depression Severity
- Combined Gender and Occupation Analysis
- Data Completeness Assessment

## 🎨 Interface Features

### Editor Features
- **Syntax-highlighted** SPARQL code
- **Line numbers** display
- **Auto-resize** textarea
- **Dark theme** for comfortable reading

### Results Features
- **Sticky header** - stays visible when scrolling
- **Hover effects** - rows highlight on hover
- **Responsive** - works on all screen sizes
- **NULL handling** - shows NULL for missing values

### Buttons
- **▶ RUN QUERY** - Execute SPARQL (Ctrl+Enter)
- **📋 COPY** - Copy query to clipboard
- **🗑️ CLEAR** - Clear results table

## ⚡ Keyboard Shortcuts

| Shortcut | Action |
|----------|--------|
| Ctrl + Enter | Run query |

## 🔧 Configuration

Edit these variables in `app.py` if needed:

```python
VIRTUOSO_ENDPOINT = "http://localhost:8890/sparql"
GRAPH_URI = "http://mentalhealth.org/data"
```

## 📊 Example Queries

### Simple Query - Count Persons
```sparql
PREFIX mh: <http://mentalhealth.org/ontology#>

SELECT (COUNT(?person) AS ?total)
FROM <http://mentalhealth.org/data>
WHERE {
  ?person rdf:type mh:Person .
}
```

### Complex Query - Screen Time Impact
```sparql
PREFIX mh: <http://mentalhealth.org/ontology#>

SELECT ?screenTimeCategory 
       (COUNT(?person) AS ?personCount)
       (AVG(?depression) AS ?avgDepression)
FROM <http://mentalhealth.org/data>
WHERE {
  ?person rdf:type mh:Person ;
          mh:hasLifestyleFactor ?lifestyle ;
          mh:hasMeasurement ?measurement .
  
  ?lifestyle mh:screenTimeHours ?screenTime .
  ?measurement mh:depressionScore ?depression .
  
  BIND(
    IF(?screenTime < 2, "Low (<2h)",
    IF(?screenTime < 4, "Moderate (2-4h)",
    IF(?screenTime < 6, "High (4-6h)", "Very High (6+h)")))
    AS ?screenTimeCategory
  )
  
  FILTER(?screenTime >= 0)
  FILTER(?depression >= 0 && ?depression <= 5)
}
GROUP BY ?screenTimeCategory
ORDER BY ?screenTimeCategory
```

## 🐛 Troubleshooting

### "Connection Refused" Error
**Problem**: Cannot connect to Virtuoso  
**Solution**: 
```bash
# Check if Virtuoso is running
sudo service virtuoso-opensource-7 status

# Start if not running
sudo service virtuoso-opensource-7 start
```

### "No Results" But Query Should Work
**Problem**: Query returns 0 results  
**Solution**: Check:
1. Graph URI is correct (`http://mentalhealth.org/data`)
2. Property paths match your ABox structure
3. Data exists for the filters (e.g., HAVING COUNT >= 10)

### Port 5000 Already in Use
**Problem**: Another app using port 5000  
**Solution**: Change port in `app.py`:
```python
app.run(debug=True, port=5001)  # Use different port
```

## 📈 Performance Tips

1. **Add LIMIT** to large queries during testing
2. **Use OPTIONAL** for potentially NULL fields
3. **Add FILTER** to reduce result sets
4. **Index** frequently queried properties in Virtuoso

## 🎯 Use Cases

### For Thesis Writing
1. Run predefined queries to generate thesis tables
2. Export results to CSV (right-click table → Export)
3. Copy query text for thesis appendix

### For Data Exploration
1. Start with broad queries (Roll-Up)
2. Drill down to specific patterns (Drill-Down)
3. Apply filters for targeted analysis (Dice)

### For Presentations
1. Use as live demo during thesis defense
2. Show real-time data exploration
3. Answer committee questions with custom queries

## 📝 Adding Your Own Queries

Edit `app.py` and add to `QUERY_CATEGORIES`:

```python
QUERY_CATEGORIES = {
    "My Category": {
        "description": "Description of my category",
        "queries": {
            "My Query Name": """
PREFIX mh: <http://mentalhealth.org/ontology#>

SELECT ?myvar
FROM <http://mentalhealth.org/data>
WHERE {
  # Your SPARQL query here
}
"""
        }
    }
}
```

Restart the server to see your new queries.

## 🎓 Academic Use

This interface was created for the thesis:
**"MentalHealthKG: A Mental Health Knowledge Graph for Enabling Multidimensional Analytics"**

Key features demonstrated:
- OLAP operations (Slice, Dice, Drill-Down, Roll-Up)
- Knowledge graph querying with SPARQL
- Integration of heterogeneous data sources
- Interactive data exploration

## 📧 Support

For issues or questions:
1. Check Virtuoso is running and data is loaded
2. Verify graph URI in configuration
3. Test simple COUNT query first
4. Check browser console for JavaScript errors

## 🎉 Features Compared to bike-csecu.com

| Feature | bike-csecu | Our Interface |
|---------|-----------|---------------|
| Query Categories | ✅ | ✅ |
| Predefined Queries | ✅ | ✅ (35+) |
| Query Editor | ✅ | ✅ (Enhanced) |
| Results Table | ✅ | ✅ (Sticky header) |
| Copy Function | ✅ | ✅ |
| Line Numbers | ❌ | ✅ |
| Keyboard Shortcuts | ❌ | ✅ |
| Dark Theme Editor | ❌ | ✅ |
| Statistics Cards | ❌ | ✅ |
| Responsive Design | ✅ | ✅ |

## 🚀 Future Enhancements

Possible improvements:
- [ ] Export results to CSV
- [ ] Save favorite queries
- [ ] Query history
- [ ] Auto-complete in editor
- [ ] Syntax error highlighting
- [ ] Visualization of results (charts)
- [ ] User authentication
- [ ] Query performance metrics

## 📄 License

Created for academic research purposes.

---

**Happy Querying! 🧠📊**
