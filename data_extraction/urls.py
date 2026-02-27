from . import views
from django.urls import path

urlpatterns = [
    path('', views.index, name='upload-docs'),
    path('export-csv/', views.export_csv, name='export-csv'),
    path('upload-to-1c/', views.upload_to_1c, name='upload-to-1c'),
    path('api/extract-invoice/', views.extract_invoice, name='extract-invoice'),
    path('api/create-documents/', views.create_1c_documents, name='create-documents'),
]
