from django.conf.urls import url
from django.contrib import admin
from .views import fulcrum_data

urlpatterns = [
    url(r'^$', fulcrum_data),
]
