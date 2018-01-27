# IOT-Adapter
Adapter for the 3D Printer application in the i-Maintenance lab.

## Recommended installation procedure (e.g. on Raspbian Desktop Linux)

```
sudo apt-get install python-pandas
sudo pip install -r requirements.txt
```

## Usage with logging

```
python3 /path/to/IOT-Adapter/adapter.py >> /path/to/IOT-Adapter/logs/logfile.log
```

## Docker Deployment

Run ```fab deploy -H user@host``` for deploying it to a specific server (e.g. ```fab deploy -H user@il060```)

Run ```fab logs -H user@host``` to access the log output.
