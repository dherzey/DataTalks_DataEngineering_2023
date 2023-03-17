# Data Engineering Zoomcamp Project
The final project required to finish DataTalks.Club's Data Engineering Zoomcamp.

## Problem Statement

## Data Architecture

## Collecting Data From Sources

### Installing Selenium and BeautifulSoup to scrape the Oscars database
In order to acquire the HTML source of the

```bash
#pip install the selenium package
pip install selenium

#download the Firefox driver from https://github.com/mozilla/geckodriver/releases
wget https://github.com/mozilla/geckodriver/releases/download/v0.32.2/geckodriver-v0.32.2-linux32.tar.gz

#decompress the file
tar -xvzf geckodriver*

#move the executable to /usr/loca/bin/
sudo mv geckodriver /usr/local/bin/

#we also install beautifulsoup for extracting data in our html files
pip install beautifulsoup4
```

### Scraping the Oscars database
Full code found in [ScrapeOscarsDB.py](https://github.com/dherzey/DataTalks_DataEngineering_2023/blob/main/week_7_PROJECT/ingestion/ScrapeOscarsDB.py)

### Scraping Bechdel test movie list
```python
import io
import requests
import pandas as pd

html = requests.get('http://bechdeltest.com/api/v1/getAllMovies').content
df = pd.read_json(io.StringIO(html.decode('utf-8')))
```

## Ingestion of data from source