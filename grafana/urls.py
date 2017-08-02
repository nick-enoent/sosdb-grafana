from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.ok),
    url(r'^query', views.query),
    url(r'^search', views.search),
]
