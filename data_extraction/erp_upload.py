import requests
import json
from dotenv import load_dotenv
import os
from typing import Dict, List, Optional, Tuple

load_dotenv()


API_BASE_URL = os.getenv('1C_API_BASE_URL', 'https://api.eman.uz/api/odata/eman_materials/')
API_TOKEN = os.getenv('1C_TOKEN')
API_ENDPOINT = 'Document_ПриобретениеТоваровУслуг'


def get_headers() -> Dict[str, str]:
    """Get API headers with authentication token"""
    if not API_TOKEN:
        raise ValueError("1C_TOKEN environment variable is not set")
    
    return {
        'X-API-TOKEN': API_TOKEN,
        'Content-Type': 'application/json'
    }


def transform_invoice_data_to_1c_format(extraction_results: List[Dict]) -> List[Dict]:
    """
    Transform extracted invoice data into format expected by 1C API
    
    Args:
        extraction_results: List of extraction results from the invoice extractor
        
    Returns:
        List of documents formatted for 1C API
    """
    documents = []
    
    for result in extraction_results:
        if not result.get('success') or not result.get('invoice_data'):
            continue
            
        invoice_data = result['invoice_data']
        factory = result.get('factory', '')
        
        # Transform each invoice into a 1C document
        document = {
            'InvoiceNumber': invoice_data.get('invoice_number'),
            'InvoiceDate': invoice_data.get('invoice_date'),
            'TotalAmount': invoice_data.get('total_amount'),
            'LineItems': []
        }
        
        # Transform line items
        nomenclature = invoice_data.get('nomenclature', [])
        for item in nomenclature:
            line_item = {
                'Position': item.get('position'),
                'Nomenclature': item.get('nomenclature'),  # CA Code
                'Quantity': item.get('quantity'),
                'Unit': item.get('unit'),
                'UnitPrice': item.get('unit_price'),
                'LineTotal': item.get('line_total'),
                'OrderNumber': item.get('order_number'),
                'OrderDate': item.get('order_date'),
            }
            document['LineItems'].append(line_item)
        
        documents.append(document)
    
    return documents


def send_to_1c(extraction_results: List[Dict]) -> Tuple[bool, str, Optional[Dict]]:
    """
    Send extracted invoice data to 1C API
    
    Args:
        extraction_results: List of extraction results from the invoice extractor
        
    Returns:
        Tuple of (success: bool, message: str, response_data: Optional[Dict])
    """
    try:
        # Validate configuration
        if not API_TOKEN:
            return False, "1C API token is not configured. Please set 1C_TOKEN environment variable.", None
        
        # Transform data
        documents = transform_invoice_data_to_1c_format(extraction_results)
        
        if not documents:
            return False, "No valid extraction results to send to 1C.", None
        
        # Prepare request
        url = f"{API_BASE_URL}{API_ENDPOINT}"
        headers = get_headers()
        
        # Send each document (or batch if API supports it)
        responses = []
        for doc in documents:
            try:
                response = requests.post(url, headers=headers, json=doc, timeout=30)
                response.raise_for_status()
                responses.append({
                    'document': doc,
                    'status_code': response.status_code,
                    'response': response.json() if response.content else None
                })
            except requests.exceptions.RequestException as e:
                return False, f"Error sending document to 1C API: {str(e)}", None
        
        return True, f"Successfully sent {len(responses)} document(s) to 1C.", {'responses': responses}
        
    except Exception as e:
        return False, f"Unexpected error: {str(e)}", None