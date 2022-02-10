#!/usr/bin/env python
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""This illustrates how to get all account budgets for a Google Ads customer."""

from flask import Flask
import argparse
import sys
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from flask import Flask, render_template, request, redirect, url_for, session
from flask_mysqldb import MySQL
import MySQLdb.cursors
import re
from datetime import datetime
import calendar

app = Flask(__name__)



customer_id = "9175103648"
billing_setup_id="503875174"
today=datetime(2021, 1, 1)
today_date=today.date()
month_start_date=str(today_date.replace(day=1))
month_end_date=str(today_date.replace(day=calendar.monthrange(today.year, today.month)[1]))
app.config['MYSQL_HOST'] = ''
app.config['MYSQL_USER'] = ''
app.config['MYSQL_PASSWORD'] = ''
app.config['MYSQL_DB'] = ''
mysql = MySQL(app)



def _micros_to_currency(micros):
    return micros / 1000000.0 if micros is not None else None


# if __name__ == "__main__":
    

@app.route("/update_ads_report")
def update_ads_report():
    query = f"""
        SELECT
          ad_group_ad.ad.id,
          ad_group_ad.ad.type,
           ad_group_ad.ad.expanded_text_ad.headline_part1,
           ad_group_ad.ad.call_ad.headline1,
           ad_group_ad.ad.call_ad.headline1,
           ad_group_ad.ad.responsive_search_ad.headlines,
           ad_group_ad.ad.expanded_text_ad.headline_part2,
           ad_group_ad.ad.expanded_dynamic_search_ad.description,
           ad_group_ad.ad.expanded_dynamic_search_ad.description2,
           metrics.clicks,
           metrics.impressions,
           metrics.cost_micros,
          ad_group_ad.policy_summary.approval_status,
          ad_group_ad.policy_summary.policy_topic_entries,
          segments.date
        FROM ad_group_ad
        WHERE segments.date > '"""+month_start_date+"""'
        AND segments.date < '"""+month_end_date+"""'
        """
    try:
        result_array=get_google_ads_query_results(query)
        for row in result_array:
            insert_headline1=""
            insert_headline2=""
            if row.ad_group_ad.ad.type_.name=="EXPANDED_TEXT_AD":
                insert_headline1=row.ad_group_ad.ad.expanded_text_ad.headline_part1
                insert_headline2=row.ad_group_ad.ad.expanded_text_ad.headline_part2

            if row.ad_group_ad.ad.type_.name=="CALL_AD":
                insert_headline1=row.ad_group_ad.ad.call_ad.headline1
                insert_headline2=row.ad_group_ad.ad.call_ad.headline1
            if row.ad_group_ad.ad.type_.name=="EXPANDED_DYNAMIC_SEARCH_AD":
                insert_headline1=row.ad_group_ad.ad.expanded_dynamic_search_ad.description
                insert_headline2=row.ad_group_ad.ad.expanded_dynamic_search_ad.description2
            if row.ad_group_ad.ad.type_.name=="RESPONSIVE_SEARCH_AD":
                
                if len(row.ad_group_ad.ad.responsive_search_ad.headlines) == 1:
                    a=row.ad_group_ad.ad.responsive_search_ad.headlines
                    insert_headline1=a.text
                if len(row.ad_group_ad.ad.responsive_search_ad.headlines) == 2:
                    a,b=row.ad_group_ad.ad.responsive_search_ad.headlines[0],row.ad_group_ad.ad.responsive_search_ad.headlines[1]
                    insert_headline1=a.text
                    insert_headline2=b.text
                if len(row.ad_group_ad.ad.responsive_search_ad.headlines) >= 2:
                    a,b,c=row.ad_group_ad.ad.responsive_search_ad.headlines[0],row.ad_group_ad.ad.responsive_search_ad.headlines[1],row.ad_group_ad.ad.responsive_search_ad.headlines[2:]
                    insert_headline1=a.text
                    insert_headline2=b.text

            spent_date=str(row.segments.date)
            insert_headline1=insert_headline1.replace("'", "\\'")
            insert_headline2=insert_headline2.replace("'", "\\'")
            ad_id=str(row.ad_group_ad.ad.id)
            clicks=str(row.metrics.clicks)
            impressions=str(row.metrics.impressions)
            cost=str(_micros_to_currency(row.metrics.cost_micros))
            DateTime = str(datetime.now())
            sql="INSERT INTO ads_report (customer_id,ad_id,spent_date,ad_headline_1,ad_headline_2,clicks,impressions,cost,created_datetime,updated_datetime) VALUES ('"+customer_id+"','"+ad_id+"','"+spent_date+"','"+insert_headline1+"','"+insert_headline2+"','"+clicks+"','"+impressions+"','"+cost+"','"+DateTime+"','"+DateTime+"') ON DUPLICATE KEY UPDATE ad_headline_1='"+insert_headline1+"',ad_headline_2='"+insert_headline2+"',clicks='"+clicks+"',impressions='"+impressions+"',cost='"+cost+"',updated_datetime='"+DateTime+"'"
            query_raw_sql(sql)      
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
    return "completed"
@app.route("/update_keywords_report")
def update_keywords_report():
    query = f"""SELECT
            ad_group_criterion.keyword.text,
            ad_group_criterion.approval_status,
            metrics.impressions,
            metrics.clicks,
            metrics.historical_quality_score,
            metrics.historical_creative_quality_score,
            metrics.cost_micros,
            segments.date
            FROM keyword_view
            WHERE segments.date > '"""+month_start_date+"""'
    AND segments.date < '"""+month_end_date+"""'
            """
    try:
        result_array=get_google_ads_query_results(query)
        for row in result_array:
            spent_date=str(row.segments.date)
            DateTime = str(datetime.now())
            keyword=row.ad_group_criterion.keyword.text.replace("'", "\\'")
            resource_name=row.ad_group_criterion.resource_name
            approval_status=str(row.ad_group_criterion.approval_status.name)
            clicks=str(row.metrics.clicks)
            impressions=str(row.metrics.impressions)
            cost=str(_micros_to_currency(row.metrics.cost_micros))
            quality_score=str(row.metrics.historical_quality_score)
            ad_relevance=str(row.metrics.historical_creative_quality_score.name)

            sql="INSERT INTO keywords_report (customer_id,resource_name,spent_date,keyword,approval_status,clicks,impressions,cost,quality_score,ad_relevance,created_datetime,updated_datetime) VALUES ('"+customer_id+"','"+resource_name+"','"+spent_date+"','"+keyword+"','"+approval_status+"','"+clicks+"','"+impressions+"','"+cost+"','"+quality_score+"','"+ad_relevance+"','"+DateTime+"','"+DateTime+"') ON DUPLICATE KEY UPDATE keyword='"+keyword+"',approval_status='"+approval_status+"',clicks='"+clicks+"',impressions='"+impressions+"',cost='"+cost+"',quality_score='"+quality_score+"',ad_relevance='"+ad_relevance+"',updated_datetime='"+DateTime+"'"
            query_raw_sql(sql)
                       
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)

@app.route("/update_campaign_report")
def update_campaign_report():
    
    query = f""" SELECT
             campaign.name,
                    metrics.impressions, metrics.clicks,campaign.id, metrics.cost_micros,segments.date
            FROM campaign WHERE segments.date > '"""+month_start_date+"""'
    AND segments.date < '"""+month_end_date+"""'
            """
    try:
        result_array=get_google_ads_query_results(query)
        for row in result_array:
            
            DateTime = str(datetime.now())
            campaign_name=row.campaign.name.replace("'", "\\'")
            campaign_id=str(row.campaign.id)
            spent_date=str(row.segments.date)
            clicks=str(row.metrics.clicks)
            impressions=str(row.metrics.impressions)
            cost=str(_micros_to_currency(row.metrics.cost_micros))

            sql="INSERT INTO  campaign_report  (customer_id,campaign_id,spent_date,campaign_name,clicks,impressions,cost,created_datetime,updated_datetime) VALUES ('"+customer_id+"','"+campaign_id+"','"+spent_date+"','"+campaign_name+"','"+clicks+"','"+impressions+"','"+cost+"','"+DateTime+"','"+DateTime+"') ON DUPLICATE KEY UPDATE campaign_name='"+campaign_name+"',clicks='"+clicks+"',impressions='"+impressions+"',cost='"+cost+"',updated_datetime='"+DateTime+"'"
            query_raw_sql(sql)
                       
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
    return "completed"
@app.route("/update_budget_report")
def update_budget_report():
    today_date=today.date()
    month_start_date=str(today_date.replace(day=1))
    month_end_date=str(today_date.replace(day=calendar.monthrange(today.year, today.month)[1]))
    
    query = f"""
        SELECT
          metrics.cost_micros,
          campaign_budget.name,
          campaign_budget.id,
          segments.date
            FROM campaign_budget WHERE segments.date > '"""+month_start_date+"""'
    AND segments.date < '"""+month_end_date+"""'
    """
    try:
        result_array=get_google_ads_query_results(query)
        for row in result_array:
            
            DateTime = str(datetime.now())
            campaign_budget_id=str(row.campaign_budget.id)
            spent_date=row.segments.date
            campaign_budget_name=row.campaign_budget.name.replace("'", "\\'")
            campaign_id=str(row.campaign.id)
            cost=str(_micros_to_currency(row.metrics.cost_micros))

            sql="INSERT INTO  budget_report   (customer_id,campaign_budget_id,campaign_budget_name,date,amount_spent,created_datetime,updated_datetime) VALUES ('"+customer_id+"','"+campaign_budget_id+"','"+campaign_budget_name+"','"+spent_date+"','"+cost+"','"+DateTime+"','"+DateTime+"') ON DUPLICATE KEY UPDATE campaign_budget_name='"+campaign_budget_name+"',amount_spent='"+cost+"',updated_datetime='"+DateTime+"'"
            query_raw_sql(sql)
                       
    except GoogleAdsException as ex:
        print(
            f'Request with ID "{ex.request_id}" failed with status '
            f'"{ex.error.code().name}" and includes the following errors:'
        )
        for error in ex.failure.errors:
            print(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    print(f"\t\tOn field: {field_path_element.field_name}")
        sys.exit(1)
    return "completed"

    
@app.route("/update_all_reports")
def update_all_reports():
    update_budget_report()
    update_campaign_report()
    update_ads_report()
    # update_keywords_report()
    return "completed"

def query_raw_sql(query):
    cursor = mysql.connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(query)
    mysql.connection.commit()
def get_google_ads_query_results(query):
    client = GoogleAdsClient.load_from_storage("./google-ads.yaml",version="v9")
    ga_service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query
    iterator = ga_service.search(request=request)
    return iterator
   
    



    