from django.shortcuts import render, redirect
from django.contrib import messages
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from .forms import UploadDocsForm
from .data_extractor import InvoiceExtractor, build_1c_payload
# from . import erp_upload  # TODO: enable when sending data to ERP
import tempfile
import os
import csv
import json
from datetime import datetime
import requests


@csrf_exempt
@require_POST
def extract_invoice(request):
    """API endpoint: POST a PDF file to extract invoice data. Expects multipart/form-data with key 'file'."""
    if 'file' not in request.FILES:
        return JsonResponse({'error': 'No file provided'}, status=400)

    uploaded_file = request.FILES['file']
    if not uploaded_file.name.lower().endswith('.pdf'):
        return JsonResponse({'error': 'Only PDF files are supported'}, status=400)

    extractor = InvoiceExtractor()

    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        for chunk in uploaded_file.chunks():
            tmp.write(chunk)
        tmp_path = tmp.name

    try:
        invoice_data, confidence = extractor.extract_from_pdf(tmp_path)
        invoice_data = extractor.enrich_with_ref_keys(invoice_data)
        order_ref_keys = {
            item["order_number"]: item["order_ref_key"]
            for item in invoice_data.get("nomenclature", [])
            if item.get("order_number")
        }
        document_to_create = build_1c_payload(invoice_data, order_ref_keys)
        return JsonResponse({
            'success': True,
            'confidence': confidence,
            'document_to_create': json.loads(json.dumps(document_to_create, default=str)),
        })
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@csrf_exempt
@require_POST
def create_1c_documents(request):
    """
    API endpoint: POST a single 1C document to create in the external 1C system.
    Expects JSON body: {"document_to_create": { ... }}.
    """
    try:
        data = json.loads(request.body.decode("utf-8"))
    except json.JSONDecodeError:
        return JsonResponse({"success": False, "error": "Invalid JSON payload"}, status=400)

    doc = data.get("document_to_create")

    if not isinstance(doc, dict):
        return JsonResponse(
            {"success": False, "error": "'document_to_create' must be an object"},
            status=400,
        )

    api_token = os.getenv("1C_TOKEN")
    if not api_token:
        return JsonResponse(
            {"success": False, "error": "1C token is not configured on the server"},
            status=500,
        )

    items = doc.get("Товары", [])
    if not isinstance(items, list):
        return JsonResponse(
            {"success": False, "error": "Field 'Товары' must be a list"},
            status=400,
        )

    # If КодСтроки is missing/None, omit it entirely.
    new_items = []
    for item in items:
        new_item = dict(item)
        if new_item.get("КодСтроки") is None:
            new_item.pop("КодСтроки", None)
        new_items.append(new_item)
    doc["Товары"] = new_items

    try:
        response = requests.post(
            "https://api.eman.uz/api/odata/eman_materials/Document_ПриобретениеТоваровУслуг?$format=json",
            json=doc,
            headers={
                "X-API-TOKEN": api_token,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=120,
        )
    except requests.RequestException as exc:
        return JsonResponse(
            {
                "success": False,
                "error": str(exc),
            },
            status=502,
        )

    if response.status_code == 201:
        try:
            payload = response.json()
        except ValueError:
            payload = None

        return JsonResponse(
            {
                "success": True,
                "ref_key": (payload or {}).get("Ref_Key") if isinstance(payload, dict) else None,
                "number": (payload or {}).get("Number") if isinstance(payload, dict) else None,
                "full_response": payload if payload is not None else response.text,
            }
        )
    else:
        try:
            error_payload = response.json()
        except ValueError:
            error_payload = None

        return JsonResponse(
            {
                "success": False,
                "status_code": response.status_code,
                "error": response.text,
                "full_response": error_payload if error_payload is not None else response.text,
            },
            status=502,
        )


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
                        invoice_data = extractor.enrich_with_ref_keys(invoice_data)

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
        'Ref Key (1C UUID)',
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
                        item.get('ref_key', ''),
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
                    '',
                ])
    
    return response


def upload_to_1c(request):
    """
    Placeholder view for future 1C/ERP integration.
    Currently, it does not send anything and just redirects back to index.
    """
    # TODO: re-enable when ready to send data to ERP
    # extraction_results_json = request.session.get('extraction_results')
    # if not extraction_results_json:
    #     messages.error(request, 'No extraction results found. Please upload and process files first.')
    #     return redirect('upload-docs')
    #
    # try:
    #     extraction_results = json.loads(extraction_results_json)
    # except (json.JSONDecodeError, TypeError):
    #     messages.error(request, 'Error reading extraction results.')
    #     return redirect('upload-docs')
    #
    # success, message, response_data = erp_upload.send_to_1c(extraction_results)
    # if success:
    #     messages.success(request, message)
    # else:
    #     messages.error(request, message)

    return redirect('upload-docs')