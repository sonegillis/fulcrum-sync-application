from .models import GarlandValve2017

from django.shortcuts import render, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from rest_framework.parsers import JSONParser
from django.db import models, migrations
from django.apps import apps
from django.contrib.gis.geos import GEOSGeometry

import fulcrumdata.models
from .models import FulcrumAppToSync

import threading
import requests
import datetime
import dateutil.parser
import json

def query_fulcrum_for_data(share_token, fulcrum_id, require_previous_data=False):
    """
        This function queries fulcrum for data using the datashare

        Arguments
        ---------
        share_token: an id that datashare uses to uniquely query data for a particular fulcrum application
        fulcrum_id: the id used to get data for a particular row on the table in the fulcrum app databases
        require_previous_data: a boolean that tells us if we are querying for all the data or a particular data
    """

    # create datashare url to access data for a specific table row in fulcrum
    if require_previous_data:
        datashare_url = "https://web.fulcrumapp.com/shares/"+share_token+".geojson"
        # There might be json decoding error if data fetched by requests is not in json format
        # So wrap in this try - catch block
        try:
            response = requests.get(datashare_url).json()['features']
        except Exception as e:
            response = None
        return response
    else:
        datashare_url = "https://web.fulcrumapp.com/shares/"+share_token+".geojson?fulcrum_id="+fulcrum_id
        # There might be json decoding error if data fetched by requests is not in json format
        # So wrap in this try - catch block
        try:
            response = requests.get(datashare_url).json()['features']
        except Exception as e:
            response = None
        return response

def extract_model_field_values(model, properties):
    # get a dictionary of the model with its fields as keys and data type as values
    fields = {}
    [fields.update({f.name:model._meta.get_field(f.name).get_internal_type()}) for f in model._meta.fields]
    # get the keys of the response dict from fulcrum as list
    keys = list(properties.keys())
    # get the fields as list from fi
    # compare and remove from the response dict keys any entity that is not in the model fields
    field_names = list(fields.keys())

    for key in keys:
        # marker-color is saved as marker_color
        # check if the field has a key called marker-color and replace it with marker color
        if key == 'marker-color':
            properties['marker_color'] = properties[key]
            del properties[key]
            continue
        if key not in field_names:
            properties.pop(key)

    # format the text data for fields with DateTimeField data type as a datetime object
    for key in list(properties.keys()):
        # check if the model field name data type is DateTimeField
        if fields[key] == 'DateTimeField':
            date_string = properties[key]
            # check if the date time from fulcrum is already a python datetime object, then do nothing
            if isinstance(date_string, datetime.datetime):
                pass
            # else try creating a datetime object
            else:
                # the properties can have some datetime fields as none, put in an if statement to avoid dateutil error
                if date_string:
                    date_string = date_string.strip()
                    date_string = 'T'.join(date_string.split(" ")[:-1])
                    properties[key] = dateutil.parser.parse(date_string)
        # check if the model field name data type is DateField
        if fields[key] == 'DateField':
            date_string = properties[key]
            if date_string:
                date_string = date_string.strip()
                year, month, day = date_string.split('-')
                properties[key] = datetime.date(int(year), int(month), int(day))
        # check if the model field name data type is BigIntegerField, then typecast the string to int
        if fields[key] == 'BigIntegerField':
            if properties[key]: properties[key] = int(properties[key])
        # check if the model field name data type is FloatField, then typecast the string to float
        if fields[key] == 'FloatField':
            if properties[key]: properties[key] = float(properties[key])
    # return the the updated response dict keys
    return properties


def run(*args):
    share_token = '4ccb5b6c8b4f0fc0'
    datashare_url = "https://web.fulcrumapp.com/shares/"+share_token+".geojson"
    response_list = query_fulcrum_for_data(share_token, fulcrum_id, True)
    # update the model with the response data
    print(len(response_list))
    if response_list is not None:
        for response in response_list:
            properties = response['properties']
            try:
                geometry = json.dumps(response['geometry'])
                geometry = GEOSGeometry(geometry)
            except:
                geometry = None
            properties = extract_model_field_values(GarlandValve2017, properties)
            properties.update({"geometry": geometry})
            # placing the code below in try catch if by any means an exception is raised
            # due to uniqueness constraints of the fulcrum id
            try:
                model.objects.create(**properties)
            except Exception as e:
                print(e)
