import requests
from pathlib import Path

partonomy_path = Path(__name__).parent / 'cache/partonomy.jsonld'
if not partonomy_path.exists():
  partonomy_url = 'https://cdn.jsdelivr.net/gh/hubmapconsortium/hubmap-ontology@1.0.0/ccf-partonomy.jsonld'
  partonomy_path.write_text(requests.get(partonomy_url).text)

print(partonomy_path.read_text())

