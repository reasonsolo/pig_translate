# -*- encoding: utf-8 -*-
#! /usr/bin/env python
from select_parser import parse
import collections

QUERY_FIELD = (
    'uri',
    'useragent',
    'mvid',
    'reqType',
    'publisherId',
    'channelId',
    'adspaceId',
    'advertiserId',
    'campaignId',
    'solutionId',
    'creativeId',
    'entityType',
    'solutionType',
    'emptyReturnType',
)



def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

class FieldError(Exception):
    pass

class PigGenerator:
    def __init__(self, sql):
        self.sql = sql
        self.symnum = 0
        self.new_sym = None
        self.last_sym = None
        self.symbols = []
        self.adt = parse(self.sql)

    def get_new_symbol(self, reuse = True):
        self.symnum += 1
        if self.symnum < 26:
            sym = chr(ord('A') + self.symnum - 1)
        else:
            sym = chr(ord('A') + (self.symnum) % 26) + str(self.symnum / 26)
        self.last_sym = self.new_sym
        self.new_sym = sym
        return sym


    def get_new_and_last_symbol(self):
        return self.get_new_symbol(), self.last_sym

    def reuse_symbol(self, symbol):
        self.symbols.append(symbol)


    def check_fields(fields):
        for field in fields:
            if field not in QUERY_FIELD:
                raise FieldError("field %s is not allowed")

    def rename_ident(ident_list):
        pass

    def basic_load(self, date = None, hour = None):
        date_time = ''
        if date:
            date_time += '/' + date
            if hour:
                date_time += '/%s.*' % hour
        load =  self.get_new_symbol() + \
        """ = LOAD '/user/hadoop/ambitionlog%s using com.twitter.elephantbird.pig.load.LzoThriftBlockPigLoader('com.mediav.data.log.CookieDailyLog');\n"""
        load += '%s = FOREACH  %s GENERATE FLATTEN(events);' % self.get_new_and_last_symbol()
        return load % date_time

    def column_generate(self):
        pass

    def where_filter(self):
        try:
            if self.adt.where_expr == None:
                return
        except AttributeError:
            return
        where = '%s = FILTER %s BY '  +  ' '.join(flatten(self.adt.where_expr))
        return where % (self.get_new_and_last_symbol())

    def group_by(self):
        try:
            if self.adt.group_terms == None:
                return
        except AttributeError:
            return
        group = '%s = GROUP %s BY ('  \
                +  ', '.join(flatten(self.adt.group_terms))+ ');' #self.get_new_and_last_symbol()
        return group % self.get_new_and_last_symbol()

    def gen_register(self):
        register = """
register '/opt/pig/lib/ambition-data.jar';
register '/opt/pig/lib/libthrift.jar';
register '/opt/pig/lib/mediavdailylog.jar';
register '/opt/pig/lib/mediav-pig-farm.jar';
register '/opt/pig/lib/mediav-session-log.jar';
register '/opt/pig/lib/elephant-bird.jar';
register '/opt/pig/lib/guava.jar';
register '/opt/pig/lib/hadoop-lzo.jar';
register '/opt/pig/lib/protobuf-java.jar';
register '/opt/pig/lib/slf4j-api.jar';
register '/opt/pig/lib/slf4j-log4j12.jar';
register '/opt/pig/lib/json-simple.jar';
register '/opt/pig/lib/commons-lang-2.5.jar';
    """
        return register

    def get_pig(self):

        print self.gen_register()
        print self.basic_load()
        print self.where_filter()
        print self.group_by()



if __name__ == '__main__':
    query = raw_input()
    gen = PigGenerator(query)
    gen.get_pig()
