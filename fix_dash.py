import json
with open('config/grafana/dashboards/StreamMart/streammart.json', 'r') as f:
    d = json.load(f)

for p in d['panels']:
    if p.get('title') == "Today's Conversion Rate" or p.get('id') == 2:
        p['targets'][0]['rawSql'] = "SELECT ROUND(AVG(conversion_rate)*100, 2) as value \nFROM metrics_5min \nWHERE window_start >= NOW() - INTERVAL '1 hour'\nAND event_type = 'purchase'"
    if p.get('title') == "Cart Abandonment Rate" or p.get('id') == 3:
        p['targets'][0]['rawSql'] = "SELECT ROUND(\n  100.0 * SUM(CASE WHEN event_type='abandonment' THEN count ELSE 0 END) \n  / NULLIF(SUM(CASE WHEN event_type='add_to_cart' THEN count ELSE 0 END),0)\n,2) as value\nFROM metrics_5min\nWHERE window_start >= NOW() - INTERVAL '1 hour'"

p8 = {
  "datasource": {"type": "postgres", "uid": "postgres"},
  "fieldConfig": {
    "defaults": {
      "color": {"mode": "thresholds"},
      "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "inspect": False},
      "mappings": [],
      "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]}
    },
    "overrides": []
  },
  "gridPos": {"h": 8, "w": 24, "x": 0, "y": 14},
  "id": 8,
  "options": {
    "cellHeight": "sm",
    "footer": {"countRows": False, "fields": "", "reducer": ["sum"], "show": False},
    "showHeader": True
  },
  "pluginVersion": "10.0.0",
  "targets": [
    {
      "datasource": {"type": "postgres", "uid": "postgres"},
      "editorMode": "code",
      "format": "table",
      "rawQuery": True,
      "rawSql": "SELECT created_at as time, user_id, session_id,\ndevice_type, total_events, converted, revenue\nFROM session_summary\nORDER BY created_at DESC LIMIT 20",
      "refId": "A"
    }
  ],
  "title": "Last 20 Sessions",
  "type": "table"
}
has_p8 = False
for i, p in enumerate(d['panels']):
    if p.get('id') == 8:
        d['panels'][i] = p8
        has_p8 = True

if not has_p8:
    d['panels'].append(p8)

with open('fixed.json', 'w') as f:
    json.dump(d, f, indent=2)