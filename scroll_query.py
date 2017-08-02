#! /bin/python
# coding:utf-8

"""
Author: Myrfy001

Description:
This script is designed for easy query any ElasticSearch and get all the
results by scrolling API.
"""


def trim_slash(str_in):
    return str_in if str_in[-1] != "/" else str_in[:-1]


class ESScrollQuerier(object):

    def __init__(self, es_addr, index_and_type, session, cache_time="1m"):
        """
        :param session: a session of requests library
        """
        self.es_addr = trim_slash(es_addr)
        self.index_and_type = index_and_type
        self.session = session
        self.cache_time = cache_time
        self.scroll_addr = "{}/_search/scroll".format(self.es_addr)
        self.query_addr = "{}/{}/_search?scroll={}".format(
            self.es_addr, self.index_and_type, self.cache_time)

    def __enter__(self):
        return self

    def query(self, query_data):
        self.usr_query_data = query_data
        return self

    def __iter__(self):
        return self.do_query()

    def do_query(self):
        """Use scroll API to iter results"""
        resp = self.session.post(
            self.query_addr, json=self.usr_query_data).json()
        print resp
        self.scroll_id = resp["_scroll_id"]
        yield resp

        while self.scroll_id:
            query_data = {
                "scroll": self.cache_time,
                "scroll_id": self.scroll_id,
            }
            resp = self.session.post(self.scroll_addr, json=query_data).json()
            self.scroll_id = resp["_scroll_id"]
            yield resp

    def close(self):
        # Clean up the scroll to let the server free the resourses
        query_data = {
            "scroll_id": self.scroll_id,
        }
        self.session.delete(self.scroll_addr, json=query_data).json()

    def __exit__(self, type, value, trace):
        self.close()
