#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 09:14:57 2019

@author: mathewrees
"""

import argparse
import yaml
import pandas as pd
import requests
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from pathlib import Path
import os
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import rc
import datetime
from sqlalchemy import create_engine


# run as: 
# python MatchRateEstimator.py config input_numbers output_file countrycode waterfall_mode

class MatchRateEstimator:

    
    def __init__(self):
        
        args = self.args()
        print(args)
        # get CLI args
        config_file = args.config
        input_numbers = args.input_numbers
        self.results_file = args.output_file
        self.country_code = args.country_code
        self.waterfall_mode = args.waterfall_mode
        if self.waterfall_mode not in ['on','off']:
            print('waterfall_mode must either be <on> or <off>')
            quit()
        # make some variables
        self.config = self.load_config(config_file)
        self.sources = self.config['sources']
        
        self.latency_catch = []
        
        #  check no output file exists, so not to overwrite previous results
        cwd = os.getcwd()
        my_file = Path("{0}/{1}".format(cwd, self.results_file))
        if os.path.exists(my_file):
            print('exiting as target output file already exists')
            quit()
        
        # load numbers file as list object
        with open(input_numbers) as num_file:
            self.input_nums = num_file.read().splitlines()
        
        # DataFrame for holding results from different api's
        self.results = pd.DataFrame(self.input_nums, columns=['numbers'])
        
        
        # Connect to AWS resource...
        # Requires $AWS_PROFILE environment variable to be set
        if 'hiya' or 'scp' or 'yelp' in self.config['sources']:
            AWS_PROFILE = self.config['AWS']['AWS_PROFILE']
            AWS_Region = self.config['AWS']['Region']
            self.dynamoDB = self.dynamoConn(AWS_PROFILE, 'dynamodb', AWS_Region)
        
        # check country code input meets criterie
        # atm only check string length is within sql table limit - could do lookup on country code table
        if len(self.country_code) > 2:
            print('country code input must be less that 2 characters')
            quit()
        
    
            
        
    
    def args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("config", type=str,
                    help="config file (YAML format)")
        parser.add_argument("input_numbers", type=str,
                            help="List of input numbers in hiya format")
        parser.add_argument("output_file", type=str,
                            help="Results file target filepath")
        parser.add_argument("country_code", type=str,
                            help="2-letter ISO country code")
        parser.add_argument("waterfall_mode", type=str,
                            help="Waterfall mode on/off. Waterfall mode reduces ID'd numbers from the input_numbers list ")
        args = parser.parse_args()
        return args
        
    
        
    def load_config(self, config_filepath):
        the_yaml = open(config_filepath, 'r')
        config = yaml.load(the_yaml)
        return config
    
    
    
    
    def query_controller(self, config, num_list):
        sources = self.config['sources']
        
        # list of numbers that's reduced after every API batch call so that 
        # only still unidentified numbers are queried to the next API
        self.unknown_numbers = []
        
        for i in sources:
            
            if i=='infobel':              
                print('- - starting infobel')
                self.infobel_latency=[]
                url_base = self.config['url_strings']['infobel']
                user = self.config['credentials']['infobel']['username']
                password = self.config['credentials']['infobel']['password']
                self.infobel_loop(url_base, user, password, num_list)
                self.latency_catch.append(['infobel', np.mean(self.infobel_latency)])
                print('- - infobel done')
                
                
            if i=='telesign':
                print('- - starting telesign')
                self.telesign_latency=[]
                url_base = self.config['url_strings']['telesign']
                user = self.config['credentials']['telesign']['user']
                api_key = self.config['credentials']['telesign']['api_key']
                auth = (user, api_key)
                data = self.config['credentials']['telesign']['data']
                self.telesign_loop(url_base, auth, data, num_list)
                self.latency_catch.append(['telesign', np.mean(self.telesign_latency)])
                print('- - telesign done')
                
        
            if i=='telo':
                print('- - starting telo')
                self.telo_latency=[]
                telo_results=[]
                url_base = self.config['url_strings']['telo']
                a_sid = self.config['credentials']['telo']['account_sid']
                a_token = self.config['credentials']['telo']['authtoken']         
                for num in num_list:
                    url_string = url_base.format(num, a_sid, a_token)
                    name, code = self.telo_call(url_string)
                    telo_results.append([num, code, name])
                
                telo_df = pd.DataFrame(telo_results, columns=['numbers','telo_res_desc','telo_name'])
                 
                if self.waterfall_mode == 'on':
                     new_num_list = self.filter_matched_numbers(telo_results)
                     num_list = new_num_list
                
                self.results = self.results.join(telo_df.set_index('numbers'), on='numbers')
                self.latency_catch.append(['telo', np.mean(self.telo_latency)])
                print('- - telo done')
                
                
            if i=='hiya':
                print('- - starting hiya')               
                hiya_results=[]
                table = self.dynamoDB.Table('cps.prod.unifiedcache')
       
                for num in num_list:
                    name, code, conf = self.hiya_internal(table, num)
                    hiya_results.append([num, code, name, conf])
                    
                hiya_df = pd.DataFrame(hiya_results, columns=['numbers','hiya_res_desc','hiya_name','hiya_conf'])
                
                if self.waterfall_mode == 'on':
                    new_num_list = self.filter_matched_numbers(hiya_results)
                    num_list = new_num_list
                 
                self.results = self.results.join(hiya_df.set_index('numbers'), on='numbers')
                print('- - hiya done')
                
                
            if i=='scp':
                print('- - starting scp')               
                scp_results=[]
                table = self.dynamoDB.Table('cps.prod.unifiedcache')
       
                for num in num_list:
                    name, code = self.hiya_scp(table, num)
                    scp_results.append([num, code, name])
                    
                scp_df = pd.DataFrame(scp_results, columns=['numbers','scp_res_desc','scp_name'])
                
                if self.waterfall_mode == 'on':
                    new_num_list = self.filter_matched_numbers(scp_results)
                    num_list = new_num_list
                 
                self.results = self.results.join(scp_df.set_index('numbers'), on='numbers')
                print('- - scp done')
                
                
                
            if i=='yelp':
                print('- - starting yelp')
                yelp_results=[]
                table = self.dynamoDB.Table('id_data.prod.yelp_cache')
                
                for num in num_list:
                    name, code = self.yelp_internal(table, num)
                    yelp_results.append([num, code, name])
                
                yelp_df = pd.DataFrame(yelp_results, columns=['numbers','yelp_res_desc','yelp_name'])
                if self.waterfall_mode == 'on':
                    new_num_list = self.filter_matched_numbers(yelp_results)
                    num_list = new_num_list
                self.results = self.results.join(yelp_df.set_index('numbers'), on='numbers')
                print('- - yelp done')
                
                
                
            if i=='navagis':
                print('- - starting navagis')
                self.navagis_latency=[]
                navagis_results=[]
                url_base = self.config['url_strings']['navagis']
                api_key = self.config['credentials']['navagis']['api_key']
            
                
                for num in num_list:
                    url_string = url_base.format(num, api_key)
                    name, code = self.navagis_call(url_string)
                    navagis_results.append([num, code, name])
                navagis_df = pd.DataFrame(navagis_results, columns=['numbers','navagis_res_desc','navagis_name'])
                
                if self.waterfall_mode == 'on':
                    new_num_list = self.filter_matched_numbers(navagis_results)
                    num_list = new_num_list
                
                self.results = self.results.join(navagis_df.set_index('numbers'), on='numbers') 
                self.latency_catch.append(['navagis', np.mean(self.navagis_latency)])
                print('- - navagis done')
                
                
                
            if i=='whitepages':
                print('- - starting whitepages')
                whitepages_results = []
                self.whitepages_latency=[]
                url_base = self.config['url_strings']['whitepages']
                api_key = self.config['credentials']['whitepages']['token']
                
                for num in num_list:
                    url_string = url_base.format(num, api_key)
                    name, code = self.whitepages_call(url_string)
                    whitepages_results.append([num, code, name])
                whitepages_df = pd.DataFrame(whitepages_results, columns=['numbers','whitepages_res_desc','whitepages_name'])
                if self.waterfall_mode == 'on':
                    new_num_list = self.filter_matched_numbers(whitepages_results)
                    num_list = new_num_list
                
                self.results = self.results.join(whitepages_df.set_index('numbers'), on='numbers') 
                self.latency_catch.append(['whitepages', np.mean(self.whitepages_latency)])
                print('- - whitepages done')
            
        print(self.results)
        
        
        
            
    def filter_matched_numbers(self, results_list):
        # make a new list of unknown number based on results from previous source responses
        new_name_list = [value[0] for value in results_list if value[1]!='M']
        return new_name_list
    
    
        
    def dynamoConn(self, profile_name, resource, region):
        session = boto3.Session(profile_name=profile_name)
        dynamodb = boto3.resource(resource, region_name=region)
        return dynamodb
    
    
    def check_cps_cache(self, typeKey, phone_num, dynamoTable):
        response = dynamoTable.query(KeyConditionExpression=Key('phone').eq(phone_num)
            & Key('typeKey').eq(typeKey))
        return response
    
        
    
    def hiya_internal(self, dynamoTable, num):
        
        res = self.check_cps_cache('nelv1', num, dynamoTable)
        if res['Items'] != []:
            payload = res['Items'][0]['payload']
            pl = json.loads(payload)
            if pl['type']=='NoIdentity':
                name=''
                code='Name Unavailable, Rep Only(?)'
                conf=''
            else:
                name = pl['name']
                code = 'M'
                conf = pl['confidence']
                print(name)
        else:
            name=''
            code='NM'
            conf=''
        return name, code, conf
                    
                    
                    
    def hiya_scp(self, dynamoTable, num):
        res = self.check_cps_cache('scpv1', num, dynamoTable)
        if res['Items'] != []:
            payload = res['Items'][0]['payload']
            pl = json.loads(payload)
            name = pl['displayName']
            code = 'M'
            print(name)
        else:
            name=''
            code='NM'
        return name, code
        
        
    
    def yelp_internal(self, dynamoTable, num):
        response = dynamoTable.query(KeyConditionExpression=Key('phone').eq(num))
        if response['Items'] != [] and len(response['Items'])>2:
            if response['Items'][0]['name'] is None:
                name=''
                code='No Name, Rep Only(?)'
            else:
                name=response['Items'][0]['name']
                code='M'
                print(name)
        else:
            name=''
            code='NM'
        return name, code
    
    
        
    def infobel_call(self, url):
        r = requests.get(url)
        
        self.infobel_latency.append(r.elapsed.total_seconds())
        
        if r.ok:
            resp = json.loads(r.text)
            name = resp['Result']['FullName']
            if name is None:
                name=''
                code='NM'
            else:
                code='M'
                print(name)
        else:
            name=''
            code='F'
        return name, code
    
    
    def infobel_loop(self, url_base, user, password, num_list):
        infobel_results = []
                
        for num in num_list:              
            num_edit = num.replace("/","")
            url_string = url_base.format(user, password, num_edit)
            name, code = self.infobel_call(url_string)
            infobel_results.append([num, code, name])
                    
        infobel_df = pd.DataFrame(infobel_results, columns=['numbers','infobel_res_desc','infobel_name'])
        if self.waterfall_mode == 'on': 
            new_num_list = self.filter_matched_numbers(infobel_results)
            num_list = new_num_list
        self.results = self.results.join(infobel_df.set_index('numbers'), on='numbers')
    
    
    
    
    def telo_call(self, url):
        r = requests.get(url)
        self.telo_latency.append(r.elapsed.total_seconds())
        if r.ok:
            resp = json.loads(r.text)
            if resp['data']:
                name = resp['data']['name']
                code = 'M'
                print(name)
            else:
                name = ''
                code = 'NM'
        else:
            name = ''
            code = 'F'
        
        return name, code
    
       
    def telesign_loop(self, url_base, auth, data, num_list):
        telesign_results = []
        for num in num_list:              
            num_edit = num.replace("/","")
            url_string = url_base.format(num_edit)
            name, code = self.telesign_call(url_string, auth, data)
            telesign_results.append([num, code, name])
        
        telesign_df = pd.DataFrame(telesign_results, columns=['numbers','telesign_res_desc','telesign_name'])
        if self.waterfall_mode == 'on': 
            new_num_list = self.filter_matched_numbers(telesign_results)
            num_list = new_num_list
        self.results = self.results.join(telesign_df.set_index('numbers'), on='numbers')
        
        
    
    def telesign_call(self, url, auth, data):
        
        r = requests.post(url, auth=auth, data=data)
        self.telesign_latency.append(r.elapsed.total_seconds())
        if r.ok:
            js = json.loads(r.text)
            if js['status']['code'] == 300:
    
                if js['contact']['status']['code'] == 2800:
                    firstname = js['contact']['first_name']
                    surname = js['contact']['last_name']
                    name = '{0} {1}'.format(firstname, surname)
                    code='M'
                    print(name)
                else:
                    name=''
                    code='NM'
            else:
                name=''
                code='F'    
        else: 
            name=''
            code='F'

        return name, code
        
    
    def whitepages_call(self, url):
        r = requests.get(url)
        self.whitepages_latency.append(r.elapsed.total_seconds())
        if r.ok:
            resp = json.loads(r.text)
            
            if resp['belongs_to']:
                if resp['belongs_to']['name']:
                    name = resp['belongs_to']['name']
                    code = 'M'
                    print(name)
                else:
                    name=''
                    code='NM'                    
            else:
                name=''
                code='NM'
        else:
            name=''
            code='F'
        return name, code
    
    


    def navagis_call(self, url):
        r = requests.get(url)
        self.navagis_latency.append(r.elapsed.total_seconds())
        if r.ok:
            resp = json.loads(r.text)
            if resp['status']!='OK':
                name=''
                code='NM'
            else:
                name = resp['candidates'][0]['name']
                code='M'   
                print(name)
        else:
            name=''
            code='F'
        return name, code
    
    
    
    def saveDF(self, DF, fname):
        if ".pkl" in fname:
            DF.to_pickle(fname)
        else:
            DF.to_csv(fname)
            
    
    def make_boolean_matrix(self, infile):
        # this converts the output file into boolean values to do overlap + cost estimation. 
        dt = 1
    

    def make_plot_data(self, DF, sources):
        
        matched_names_by_source = []
        current_dt = datetime.datetime.now()
        
        for source in sources:
            
            col_heading = "{0}_res_desc".format(source)
            #print(DF.numbers.count())
            match_count = DF[DF[col_heading]=='M'].count()[col_heading]
            #print(match_count)
            matched_names_by_source.append([self.country_code, current_dt, source, match_count, DF.numbers.count()])
            
            
        print(matched_names_by_source)
        summary_df = pd.DataFrame(matched_names_by_source,
                                  columns=['country_code', 'created_on','source','matches','records_in_run'])
        print(summary_df)
        # summary_df = summary_df.T
        #summary_df = summary_df.pivot(index=0, columns=1, values=2)
        #summary_df.to_csv("summarydf.csv")
        #print(summary_df)
        
        
        
        conn_string = 'postgresql://mathewrees@localhost/mathewrees'
        engine = create_engine(conn_string)
        connection = engine.connect()
        
        summary_df.to_sql('id_coverage_stats', connection, if_exists='append', index=False)
         
        
            
        
        
        
        
        
if __name__ == "__main__":
    
    mre = MatchRateEstimator()
    mre.query_controller(mre.sources, mre.input_nums)
    
    mre.make_plot_data(mre.results, mre.sources)
    mre.saveDF(mre.results, mre.results_file)
    print( mre.latency_catch)
    
    print('end')
    
    
        
        