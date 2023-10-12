### Usage

```sh

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export ELASTIC_USER=es_user
export ELASTIC_PASSWORD=pass
export ELASTIC_CLOUD_ID=cloudid or export ELASTIC_HOST=localhost
python index_update.py
```
