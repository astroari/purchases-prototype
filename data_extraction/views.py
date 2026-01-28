from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import HttpResponse
from .forms import UploadDocsForm
from .data_extractor import InvoiceExtractor
from . import erp_upload
import tempfile
import os
import csv
import json
from datetime import datetime

def index(request):
    form = UploadDocsForm()
    extraction_results = []
    
    if request.method == 'POST':
        form = UploadDocsForm(request.POST, request.FILES)
        if form.is_valid():
            factory = form.cleaned_data['factory']
            files = request.FILES.getlist('docs')
            
            if not files:
                messages.warning(request, 'Please select at least one file to upload.')
            else:
                extractor = InvoiceExtractor()
                
                # Process each uploaded file
                for uploaded_file in files:
                    
                    if not uploaded_file.name.lower().endswith('.pdf'):
                        messages.warning(request, f'Skipping {uploaded_file.name}: Only PDF files are supported.')
                        continue
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
                        for chunk in uploaded_file.chunks():
                            tmp_file.write(chunk)
                        tmp_file_path = tmp_file.name
                    
                    try:
                        invoice_data, confidence = extractor.extract_from_pdf(tmp_file_path)
                        
                        extraction_results.append({
                            'filename': uploaded_file.name,
                            'factory': factory,
                            'invoice_data': invoice_data,
                            'confidence': confidence,
                            'success': True
                        })
                        
                        messages.success(
                            request, 
                            f'Successfully processed {uploaded_file.name} (Confidence: {confidence:.1%})'
                        )
                    except Exception as e:
                        extraction_results.append({
                            'filename': uploaded_file.name,
                            'factory': factory,
                            'error': str(e),
                            'success': False
                        })
                        messages.error(request, f'Error processing {uploaded_file.name}: {str(e)}')
                    finally:
                        # Clean up temporary file
                        if os.path.exists(tmp_file_path):
                            os.unlink(tmp_file_path)
                
                # Store extraction results in session for CSV export
                if extraction_results:
                    request.session['extraction_results'] = json.dumps(extraction_results, default=str)
    
    context = {
        'form': form,
        'extraction_results': extraction_results
    }
    return render(request, 'data_extraction/upload_docs.html', context)


def export_csv(request):
    """Export extraction results as CSV"""
    # Get extraction results from session
    extraction_results_json = request.session.get('extraction_results')
    
    if not extraction_results_json:
        messages.error(request, 'No extraction results found. Please upload and process files first.')
        return redirect('upload-docs')
    
    try:
        extraction_results = json.loads(extraction_results_json)
    except (json.JSONDecodeError, TypeError):
        messages.error(request, 'Error reading extraction results.')
        return redirect('upload-docs')
    
    # Create CSV response
    response = HttpResponse(content_type='text/csv')
    response['Content-Disposition'] = f'attachment; filename="extraction_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
    
    writer = csv.writer(response)
    
    # Write header
    writer.writerow([
        'Filename',
        'Factory',
        'Invoice Number',
        'Invoice Date',
        'Order Number',
        'Order Date',
        'Position',
        'Nomenclature (CA Code)',
        'Quantity',
        'Unit',
        'Unit Price',
        'Line Total'
    ])
    
    # Write data rows
    for result in extraction_results:
        if result.get('success') and result.get('invoice_data'):
            invoice_data = result['invoice_data']
            filename = result.get('filename', '')
            factory = result.get('factory', '')
            
            nomenclature = invoice_data.get('nomenclature', [])
            
            if nomenclature:
                # Write a row for each line item
                for item in nomenclature:
                    writer.writerow([
                        filename,
                        factory,
                        invoice_data.get('invoice_number', ''),
                        invoice_data.get('invoice_date', ''),
                        item.get('order_number', ''),
                        item.get('order_date', ''),
                        item.get('position', ''),
                        item.get('nomenclature', ''),
                        item.get('quantity', ''),
                        item.get('unit', ''),
                        item.get('unit_price', ''),
                        item.get('line_total', ''),
                    ])
            else:
                # Write a row even if there are no line items (just header info)
                writer.writerow([
                    filename,
                    factory,
                    invoice_data.get('invoice_number', ''),
                    invoice_data.get('invoice_date', ''),
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                    '',
                ])
    
    return response


def upload_to_1c(request):
    """Upload extraction results to 1C API"""
    # Get extraction results from session
    extraction_results_json = request.session.get('extraction_results')
    
    if not extraction_results_json:
        messages.error(request, 'No extraction results found. Please upload and process files first.')
        return redirect('upload-docs')
    
    try:
        extraction_results = json.loads(extraction_results_json)
    except (json.JSONDecodeError, TypeError):
        messages.error(request, 'Error reading extraction results.')
        return redirect('upload-docs')
    
    # Send to 1C API
    success, message, response_data = erp_upload.send_to_1c(extraction_results)
    
    if success:
        messages.success(request, message)
    else:
        messages.error(request, message)
    
    return redirect('upload-docs')