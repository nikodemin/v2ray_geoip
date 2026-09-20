# V2Ray GeoIp
## Description
This project is V2Ray subscription fetcher with geo localization functionality.
App exposes rest api for fetching V2Ray configs.
It has 2 schedulers: update scheduler which fetches v2ray configs and stores it in sqlite and
recheck scheduler which rechecks connectivity to v2ray subscriptions.
## Api description
### Get subscriptions
#### Request ```curl "localhost:3000/subs?limit=5&country_code=FR&city=Paris&page=1" ```
#### Response
```
ss://Y2hhY2hhMjAtaWV0Zi1wb2x5MTMwNTpjNThkMTZlNTkzZDQ0ZjQy@158.173.221.214:11001#@v2raybaaz ³
ss://Y2hhY2hhMjAtaWV0Zi1wb2x5MTMwNTpjNThkMTZlNTkzZDQ0ZjQy@158.173.221.214:11001#@hex_proxy ⁵
```
### Get map of country codes to cities
#### Request ```curl localhost:3000/dict```
#### Response 
```json
{
  "AL": [
    "Tirana"
  ],
  "CH": [
    "Geneva",
    "Zurich"
  ],
  "RO": [
    "Bucharest"
  ],
  ...
}
```
## Config parameters
```toml
sub_groups=[""] # subscription groups urls
geo_base_url="http://ip-api.com" # geo ip service base url
batch_size=20 # batching setting, which affects parallelism and geo ip service batch size
update_period="30 m" # period between fetching v2ray subscriptions
recheck_period="5 m" # period between rechecking v2ray subscriptions (ping)
retries=7 # retries between fetching geo info
port=3000 # app port
```