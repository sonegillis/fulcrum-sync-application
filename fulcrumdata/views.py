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

# Create your views here.

@csrf_exempt
def fulcrum_data(request):
    payload = JSONParser().parse(request)
    payload_data = payload['data']
    # synchronisation type could be record.delete, record.update, record.create
    sync_type = payload['type']
    form_id = payload_data.get('form_id', None)
    fulcrum_id = payload_data['id']
    app_to_sync = FulcrumAppToSync.objects.filter(form_id=form_id)

    if app_to_sync.exists():
        thread = threading.Thread(target=update_fulcrum_app_data, args=(app_to_sync, sync_type, fulcrum_id))
        thread.start()

    return HttpResponse("")

def update_fulcrum_app_data(app_to_sync, sync_type, fulcrum_id):
    model_name = app_to_sync[0].model_name
    share_token = app_to_sync[0].share_token
    model = apps.get_model('fulcrumdata', model_name)

    # check if there is any entry in the model/table
    num_of_tb_entries = model.objects.count()
    # if there is then query fulcrum for the particular row with using the fulcrum_id
    if num_of_tb_entries > 0:
        response = query_fulcrum_for_data(share_token, fulcrum_id)
        if response is not None:
            properties = response[0]['properties']
            try:
                geometry = json.dumps(response[0]['geometry'])
                geometry = GEOSGeometry(geometry)
            except:
                geometry = None
            properties.update({"geometry": geometry})
            # Perform further action based on the sync sync_type
            if sync_type == "record.create":
                # create a new entry for the model
                properties = extract_model_field_values(model, properties)
                # placing the code below in try catch if by any means an exception is raised
                # due to uniqueness constraints of the fulcrum id
                try:
                    model.objects.create(**properties)
                except Exception as e:
                    print(e)

            if sync_type == "record.update":
                # update the model with the response data
                queryset = model.objects.filter(fulcrum_id=fulcrum_id)
                properties = extract_model_field_values(model, properties)
                # placing the code below in try catch if by any means an exception is raised
                # due to uniqueness constraints of the fulcrum id
                if queryset.exists():
                    try:
                        queryset.update(**properties)
                    except Exception as e:
                        print(e)
                else:
                    model.objects.create(**properties)
            if sync_type == "record.delete":
                # delete a model entry based on the fulcrum id sent by fulcrum
                queryset = model.objects.filter(fulcrum_id=fulcrum_id)
                if queryset.exists():
                    queryset.delete()

    # if no entry then query fulcrum for all previous data and update the table
    else:
        response_list = query_fulcrum_for_data(share_token, fulcrum_id, True)
        # update the model with the response data
        print("Response List number of entries: ", len(response_list))
        if response_list is not None:
            for response in response_list:
                properties = response['properties']
                try:
                    geometry = json.dumps(response['geometry'])
                    geometry = GEOSGeometry(geometry)
                except:
                    geometry = None
                properties = extract_model_field_values(model, properties)
                properties.update({"geometry": geometry})
                # placing the code below in try catch if by any means an exception is raised
                # due to uniqueness constraints of the fulcrum id
                try:
                    model.objects.create(**properties)
                except Exception as e:
                    print(e)

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
        page = 1
        response_list = []
        while True:
            datashare_url = "https://web.fulcrumapp.com/shares/"+share_token+".geojson?page="+str(page)
            try:
                response = requests.get(datashare_url).json()['features']
            except Exception as e:
                response = None
                print(e)
            if response:
                print("There is a reponse")
                response_list += response
                page += 1
            else:
                print("There is no response")
                break
        print("Left loop")
        return response_list
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
