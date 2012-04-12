St9 River Plugin for ElasticSearch
==================================

The St9 River plugin allows St9 content to be indexed by elasticsearch.

In order to install the plugin, simply run: `bin/plugin -install sunnygleason/elasticsearch-river-st9/0.0.1`.

    --------------------------------------
    | St9 Plugin | ElasticSearch         |
    --------------------------------------
    | master          | 0.19 -> master   |
    --------------------------------------
    | 0.0.1           | 0.19 -> master   |
    --------------------------------------

St9 River allows elasticsearch to automatically index content via redis pubsub.

Creating the st9 river is as simple as (all configuration parameters are provided, with default values):

	curl -XPUT 'localhost:9200/_river/my_river/_meta' -d '{
	    "type" : "st9",
	    "st9" : {
	        "redisHost" : "localhost", 
	        "redisPort" : 6379,
	        "indexName" : "st9_index"
	    },
	    "index" : {
	        "bulk_size" : 100
	    }
	}'

The river will automatically bulk index queue messages when the queue is overloaded, allowing for faster catch-up with content streamed into the queue.
