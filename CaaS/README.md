query the CaaS (Content Graph) datastore with a python script, and optionally save the results to a csv files

## usage
```
python3 caas_query.py query_results.csv
```
query_results.csv is optional. calling the script without an output file will print out how many results the query returns.

the query that is run by the script is determined by the elasticsearch request
specified in the file 'config/elastic_search_request.json'.
to find the elasticsearch request to paste into that file, type a query into the
search bar at https://search.timeinc.com/search, then click on the "Advanced" tab
and paste the code from there verbatim into the above json file.

config/query_config.json holds all search parameters other than the elasticsearch
request. these params are: 'type', 'provider', 'follow', and 'fields'.
refer to the CaaS API documentation for an explanation of these:
http://docs-caas.timeincapp.com/#search-and-get-examples
(these probably won't be changed very often)

## dependencies

### *python + packages*
install the latest version of python 3.6. for Windows follow [these instructions](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-windows-10). for macos use homebrew.

```
pip3 install requests
pip3 install pyyaml
pip3 install pandas
```

### *create caas_keys.py*
create a file in this directory called ```caas_keys.py``` and inside this file assign your CaaS API key to a variable named ```CAAS_API_PROD_KEY```
