## looker_dashboard_ingestion.py
This tool helps ingest Looker dashboard and chart metadata into datahub.
Currently it creates a separate platform named "looker" and loads all dashboard and chart information into that platform as virtual datasets. This was to workaround datahub's lack of support for dashboard entities, however datahub recently started supporting proper dashboard entities. 

The script assumes you already have run lookml_ingestion.py to scrape view definitions into datahub, this is important because we assign lineage between looker views and looker dashboards/charts where possible.


## Steps:
- Use a version of python >= 3.7
- Make a virtual environment
- pip install -r requirements.txt
- Set env vars: LOOKERSDK_CLIENT_ID, LOOKERSDK_CLIENT_SECRET, LOOKERSDK_BASE_URL
- Configure extra kafka conf in looker_dashboard_ingestion.py

python looker_dashboard_ingestion.py