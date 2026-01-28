from . import views
from django.urls import path

urlpatterns = [
    path('', views.index, name='upload-docs'),
    path('export-csv/', views.export_csv, name='export-csv'),
    path('upload-to-1c/', views.upload_to_1c, name='upload-to-1c'),
]
