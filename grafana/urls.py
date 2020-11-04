from django.conf.urls import url
from . import views

urlpatterns = [
    url(r'^$', views.ok),
    url(r'^query', views.grafanaView.as_view()),
    url(r'^search', views.search),
    url(r'^annotations', views.annotations)
]
