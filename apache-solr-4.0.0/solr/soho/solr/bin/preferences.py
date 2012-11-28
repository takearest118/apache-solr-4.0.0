MONGODB = {}
MONGODB['host'] = "localhost"
MONGODB['port'] = 27017
MONGODB['database'] = "soho"

SOLR = {}
SOLR['host'] = "localhost"
SOLR['port'] = "8983"
SOLR['update_index'] = "http://%s:%s/solr/update?commit=true&wt=json&indent=true" % (SOLR['host'], SOLR['port'])
SOLR['update_header'] = {'Content-Type': 'application/json'}
