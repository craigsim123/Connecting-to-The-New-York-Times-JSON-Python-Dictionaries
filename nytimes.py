"""
A technique to flatten a Python dictionary

Author: Craig Sim
"""

import argparse
import logging
import requests
import json
import collections

log = logging.getLogger(__name__)

class NYTimesSource(object):
    """
    A data loader plugin for the NY Times API.
    """

    def __init__(self):
        pass

    def connect(self, inc_column=None, max_inc_value=None):
        # Nothing to do
        log.debug('Incremental Column: %r', inc_column)
        log.debug('Incremental Last Value: %r', max_inc_value)

    def disconnect(self):
        """Disconnect from the source."""
        # Nothing to do
        pass

    ##########NEW METHOD - flattens a dictionary with a given separator (defaults to .)
    def flatten_result(self, d,sep="."):
        import collections
    
        obj = collections.OrderedDict()
    
        def recurse(t,parent_key=""):
            
            if isinstance(t,list):
                for i in range(len(t)):
                    recurse(t[i],parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(t,dict):
                for k,v in t.items():
                    recurse(v,parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = t
    
        recurse(d)
    
        return obj


    def getDataBatch(self, batch_size):
        """
        Generator - Get data from source on batches.

        :returns One list for each batch. Each of those is a list of
                 dictionaries with the defined rows.
                 'source': 'New York Times',
                        'news_desk': 'Foreign',
                        'type_of_material': 'News'
        """
        #Create URL
        URL = self.args.url + "?q=\'" + self.args.query + "\'&api-key=" + self.args.api_key

        r = requests.get(URL)

        json_array = r.json()
        response = json_array['response']     
        docs_list = list(response['docs'])
        
        results_dictlist = []
        for i in docs_list:
            flattened = self.flatten_result(i)
            item_dict = {}
            item_dict['headline.main'] = flattened['headline.main']
            item_dict['_id'] = flattened['_id']  
            results_dictlist.append(item_dict)
        
        yield results_dictlist


    def getSchema(self):
        """
        Return the schema of the dataset
        :returns a List containing the names of the columns retrieved from the
        source
        """

        schema = [
            'title',
            'body',
            'created_at',
            'id',
            'summary',
            'abstract',
            'keywords'
        ]

        return schema


if __name__ == '__main__':
    config = {
        'api_key': 'Register at NYTimes and Add Your Key Here!',
        'query': 'Silicon Valley',
        'url': 'https://api.nytimes.com/svc/search/v2/articlesearch.json'
    }
    source = NYTimesSource()

    # This looks like an argparse dependency - but the Namespace class is just
    # a simple way to create an object holding attributes.
    source.args = argparse.Namespace(**config)

    for idx, batch in enumerate(source.getDataBatch(10)):
        print('{1} Batch of {0} items'.format(len(batch), idx))
        for item in batch:
            print ('  - {0} - {1}'.format(item['_id'], item['headline.main']))


