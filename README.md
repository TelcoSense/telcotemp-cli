# telcotemp-cli
TelcoTemp CLI for temperature predictions based on operational data from CML. It computes temperature maps for every hour and stores them locally in png format. 

The main processes include:

1. **Data ingestion** — reading temperature data from InfluxDB.  
2. **Preprocessing** — cleaning, filtering, and handling missing values.  
3. **Processing** — applying interpolation, spatial operations, or neural-network methods.  
4. **Storing results** — saving processed data locally and into a database  
5. **Configuration & logging** — use config files to control behavior; log steps / errors for traceability.

# HOW TO RUN 
Activate the virtual env and install requirements.txt, e.g.:

```
pip install -r requirements.txt
```

Set the configs: start the MariaDB and InfluxDB instances and provide IPs and passwords/tokens.

Run the calculation:
```
python ./main.py
```

