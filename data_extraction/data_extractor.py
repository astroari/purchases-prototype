"""
Invoice Data Extractor using pdfplumber
Extracts structured data from Hettich-style invoices
"""

import os
import json
import re
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import pdfplumber

# 1C OData API (for nomenclature and order Ref_Key lookup)
ODATA_BASE_URL = os.getenv("1C_API_BASE_URL", "https://api.eman.uz/api/odata/eman_materials").rstrip("/")
ODATA_API_TOKEN = os.getenv("1C_TOKEN")
CATALOG_NOMENCLATURE = "Catalog_Номенклатура"
DOCUMENT_ORDER = "Document_ЗаказПоставщику"

# Static keys for 1C document payloads
STATIC_KEYS = {
    "Партнер_Key": "a5cfdc09-94ec-11ea-a9b0-505dac4282cc",
    "Контрагент_Key": "a5cfdc0b-94ec-11ea-a9b0-505dac4282cc",
    # "Организация_Key": "6e865905-8095-11ea-a9af-505dac4282cc",
    "Склад_Key": "0903e520-9f0b-11f0-8c5a-fff9d53af0ac",
    "Валюта_Key": "a3e66c2c-8095-11ea-a9af-505dac4282cc",
}


def build_1c_payload(invoice_data: dict, order_ref_keys: dict) -> dict:
    """
    Build a single 1C document payload with all nomenclature items.
    order_ref_keys: {order_number: ref_key}
    """
    items = invoice_data.get("nomenclature", [])

    товары = []
    for item in items:
        row = {
            "Номенклатура_Key": item.get("ref_key"),
            "КоличествоУпаковок": item.get("quantity"),
            "Количество": item.get("quantity"),
            "Цена": item.get("unit_price"),
            "Сумма": item.get("line_total"),
            # Prefer already-enriched order ref key; fall back to lookup by order_number.
            "ЗаказПоставщику_Key": item.get("order_ref_key")
            or order_ref_keys.get(item.get("order_number")),
        }

        line_code = item.get("line_number")
        # Omit КодСтроки completely when we don't know it.
        if line_code is not None:
            row["КодСтроки"] = line_code

        товары.append(row)

    return {
        "Date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "ПоступлениеПоЗаказам": True,
        "Posted": False,
        "НомерВагонаЗП": "111111",
        "ХозяйственнаяОперация": "ЗакупкаПоИмпортуТоварыВПути",
        "Комментарий": "TEST TEST!!",
        **STATIC_KEYS,
        "Товары": товары,
    }


def build_1c_payloads(invoice_data: dict, order_ref_keys: dict) -> list:
    """
    Build one 1C document payload per unique order number.
    order_ref_keys: {order_number: ref_key}
    """
    grouped = {}
    for item in invoice_data.get("nomenclature", []):
        order_num = item.get("order_number")
        if order_num not in grouped:
            grouped[order_num] = []
        grouped[order_num].append(item)

    payloads = []
    for order_number, items in grouped.items():
        order_ref_key = order_ref_keys.get(order_number)
        товары = []
        for item in items:
            row = {
                "Номенклатура_Key": item.get("ref_key"),
                "КоличествоУпаковок": item.get("quantity"),
                "Количество": item.get("quantity"),
                "Цена": item.get("unit_price"),
                "Сумма": item.get("line_total"),
                "ЗаказПоставщику_Key": item.get("order_ref_key") or order_ref_key,
            }

            line_code = item.get("line_number")
            if line_code is not None:
                row["КодСтроки"] = line_code

            товары.append(row)

        payload = {
            "Date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            # "ЗаказПоставщику_Key": order_ref_key,
            "ПоступлениеПоЗаказам": True,
            "Posted": False,
            "НомерВагонаЗП": "111111",
            "ХозяйственнаяОперация": "ЗакупкаПоИмпортуТоварыВПути",
            "Комментарий": "TEST TEST!!",
            **STATIC_KEYS,
            "Товары": товары,
        }
        payloads.append(payload)
    return payloads


def get_ref_keys(nomenclature_numbers: List[str], batch_size: int = 20) -> Dict[str, str]:
    """
    Fetch Ref_Key (UUID) from 1C OData for each nomenclature (CA code).
    Returns {nomenclature_number: ref_key}.
    """
    if not nomenclature_numbers:
        return {}
    if not ODATA_API_TOKEN:
        return {}

    result = {}
    headers = {
        "X-API-TOKEN": ODATA_API_TOKEN,
        "Accept": "application/json",
    }
    url = f"{ODATA_BASE_URL}/{CATALOG_NOMENCLATURE}"

    for i in range(0, len(nomenclature_numbers), batch_size):
        batch = nomenclature_numbers[i : i + batch_size]
        # OData: escape single quotes in values by doubling
        def _escape(s: str) -> str:
            return str(s).replace("'", "''")

        filter_parts = " or ".join(f"Артикул eq '{_escape(art)}'" for art in batch)

        try:
            response = requests.get(
                url,
                headers=headers,
                params={
                    "$format": "json",
                    "$filter": filter_parts,
                    "$select": "Ref_Key,Артикул",
                },
                timeout=30,
            )
        except requests.RequestException:
            continue

        if response.status_code != 200:
            continue

        for item in response.json().get("value", []):
            result[item["Артикул"]] = item["Ref_Key"]

    return result


def get_order_ref_keys(order_numbers: List[str], batch_size: int = 20) -> Dict[str, str]:
    """
    Fetch Ref_Key (UUID) from 1C OData for each order number (НомерПоДаннымПоставщика).
    Returns {order_number: ref_key}. Batches requests to avoid overly long filter strings.
    """
    if not order_numbers:
        return {}
    if not ODATA_API_TOKEN:
        return {}

    result = {}
    headers = {
        "X-API-TOKEN": ODATA_API_TOKEN,
        "Accept": "application/json",
    }
    url = f"{ODATA_BASE_URL}/{DOCUMENT_ORDER}"

    def _escape(s: str) -> str:
        return str(s).replace("'", "''")

    for i in range(0, len(order_numbers), batch_size):
        batch = order_numbers[i : i + batch_size]
        filter_parts = " or ".join(
            f"НомерПоДаннымПоставщика eq '{_escape(num)}'" for num in batch
        )

        try:
            response = requests.get(
                url,
                headers=headers,
                params={
                    "$format": "json",
                    "$filter": filter_parts,
                    "$select": "Ref_Key,НомерПоДаннымПоставщика",
                },
                timeout=30,
            )
        except requests.RequestException:
            continue

        if response.status_code != 200:
            continue

        for item in response.json().get("value", []):
            result[item["НомерПоДаннымПоставщика"]] = item["Ref_Key"]

    return result


def get_order_line_numbers(order_ref_key: str) -> Dict[str, str]:
    """
    Fetch order row codes for each nomenclature item inside a 1C order.

    Returns:
        {Номенклатура_Key: КодСтроки}
    """
    if not order_ref_key:
        return {}
    if not ODATA_API_TOKEN:
        return {}

    headers = {
        "X-API-TOKEN": ODATA_API_TOKEN,
        "Accept": "application/json",
    }

    # 1C OData key addressing: Document_ЗаказПоставщику(guid'<uuid>')
    url = f"{ODATA_BASE_URL}/{DOCUMENT_ORDER}(guid'{order_ref_key}')"

    try:
        response = requests.get(
            url,
            headers=headers,
            params={
                "$format": "json",
                "$select": "Товары",
            },
            timeout=30,
        )
    except requests.RequestException:
        return {}

    if response.status_code != 200:
        return {}

    try:
        payload = response.json()
    except ValueError:
        return {}

    items = payload.get("Товары") or []
    # Be defensive: depending on OData wrapper, Товары can be nested.
    if isinstance(items, dict):
        items = items.get("value") or items.get("results") or []

    line_codes: Dict[str, str] = {}
    for line in items or []:
        nomen_key = line.get("Номенклатура_Key")
        code = line.get("КодСтроки")
        if nomen_key is None or code is None:
            continue
        line_codes[str(nomen_key)] = str(code)

    return line_codes


class InvoiceExtractor:
    """Extract structured data from invoice PDFs"""
    
    def __init__(self):
        self.line_item_pattern = r'^(\d{4,})\s+CA:(\d+)\s+([A-Z]{2})\s+([\d.,]+)\s*([A-Z]+)\s+([\d.,]+)\s+(\d+)\s+([\d.,]+)'
        self.order_pattern = r'^Order\s+(\d+)\s+-\s+(\d{2}\.\d{2}\.\d{4})'
    
    def clean_text(self, text: str) -> str:
        """Remove underscores while preserving line structure"""
        lines = text.split('\n')
        cleaned_lines = []
        for line in lines:
            line = re.sub(r'_([a-zA-Z])_', r'\1', line)
            line = re.sub(r'_', '', line)
            cleaned_lines.append(line)
        return '\n'.join(cleaned_lines)
    
    def parse_price(self, price_str: str) -> float:
        """Convert European price format to float: 1.234,56 -> 1234.56"""
        return float(price_str.replace('.', '').replace(',', '.'))
    
    def extract_invoice_header(self, text: str) -> Dict:
        """Extract invoice number and date only"""
        data = {
            "invoice_number": None,
            "invoice_date": None,
        }
        
        # Invoice number
        number_match = re.search(r'Number:\s*(\d+)', text, re.IGNORECASE)
        if number_match:
            data["invoice_number"] = number_match.group(1)
        
        # Invoice date - format: DD.MM.YYYY
        date_match = re.search(r'Date:\s*(\d{2}\.\d{2}\.\d{4})', text, re.IGNORECASE)
        if date_match:
            date_str = date_match.group(1)
            try:
                date_obj = datetime.strptime(date_str, '%d.%m.%Y')
                data["invoice_date"] = date_obj.strftime('%Y-%m-%d')
            except:
                data["invoice_date"] = date_str
        
        return data

    def extract_order_info(self, line: str) -> Optional[Tuple[str, str]]:
        """Extract order number and date from order line"""
        match = re.match(self.order_pattern, line, re.IGNORECASE)
        if match:
            order_number = match.group(1)
            date_str = match.group(2)
            try:
                date_obj = datetime.strptime(date_str, '%d.%m.%Y')
                order_date = date_obj.strftime('%Y-%m-%d')
                return (order_number, order_date)
            except:
                return (order_number, date_str)
        return None
    
    def extract_line_items(self, text: str) -> List[Dict]:
        """Extract line items with CA code only"""
        items = []
        text = self.clean_text(text)
        lines = text.split('\n')

        # Track current order context
        current_order_number = None
        current_order_date = None
            
        i = 0
        while i < len(lines):
            line = lines[i].strip()

            # Check for order line
            order_info = self.extract_order_info(line)
            if order_info:
                current_order_number, current_order_date = order_info
                i += 1
                continue
            match = re.match(self.line_item_pattern, line)
            
            if match:
                position = match.group(1)
                catalog_code = match.group(2)  # This is the CA code
                coo = match.group(3)
                quantity_str = match.group(4)
                unit = match.group(5)
                unit_price_str = match.group(6)
                per = match.group(7)
                line_total_str = match.group(8)
                
                try:
                    quantity = self.parse_price(quantity_str)
                    unit_price = self.parse_price(unit_price_str)
                    line_total = self.parse_price(line_total_str)
                    
                    item = {
                        "position": position,
                        "nomenclature": catalog_code,  # CA code (Артикул in 1C)
                        "quantity": quantity,
                        "unit": unit,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "order_number": current_order_number,
                        "order_date": current_order_date,
                        "ref_key": None,  # 1C nomenclature Ref_Key, set by enrich_with_ref_keys()
                        "order_ref_key": None,  # 1C order document Ref_Key, set by enrich_with_ref_keys()
                    }
                    
                    items.append(item)
                    
                    # Skip description lines - we don't need them
                    j = i + 1
                    while j < len(lines):
                        desc_line = lines[j].strip()
                        if re.match(r'^\d{4,}\s+CA:', desc_line):
                            break
                        if re.match(r'^(Order \d+|Your Order:|Delivery \d+|Ship to)', desc_line, re.I):
                            break
                        if not desc_line:
                            break
                        j += 1
                    
                    i = j - 1
                    
                except (ValueError, IndexError) as e:
                    print(f"Warning: Failed to parse line {i}: {str(e)}")
            
            i += 1
        
        return items
    
    def extract_total_amount(self, text: str) -> Optional[float]:
        """Extract the total invoice amount"""
        total_match = re.search(r'Total amount\s+([\d.,]+)', text, re.IGNORECASE)
        if total_match:
            return self.parse_price(total_match.group(1))
        return None
    
    def calculate_confidence(self, data: Dict) -> float:
        """Calculate extraction confidence score (0-1)"""
        score = 0.0
        total_checks = 0
        
        # Header fields (40% weight)
        header_fields = ['invoice_number', 'invoice_date']
        for field in header_fields:
            total_checks += 1
            if data.get(field):
                score += 0.2
        
        # Line items present (30% weight)
        total_checks += 1
        if data.get('nomenclature') and len(data['nomenclature']) > 0:
            score += 0.3
        
        # Total amount (15% weight)
        total_checks += 1
        if data.get('total_amount'):
            score += 0.15
        
        # All line items have required fields (15% weight)
        if data.get('nomenclature'):
            required_fields = ['position', 'nomenclature', 'quantity', 'unit', 'unit_price', 'line_total']
            all_complete = all(
                all(field in item for field in required_fields)
                for item in data['nomenclature']
            )
            if all_complete:
                score += 0.15
        
        return min(score, 1.0)
    
    def extract_from_pdf(self, pdf_path: str) -> Tuple[Dict, float]:
        """
        Main extraction function using pdfplumber
        
        Returns:
            Tuple of (invoice_data, confidence_score)
        """
        invoice_data = {
            "invoice_number": None,
            "invoice_date": None,
            "nomenclature": [],
            "total_amount": None
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                all_text = []
                
                # Extract text from all pages
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        all_text.append(text)
                
                if not all_text:
                    return invoice_data, 0.0
                
                # Combine all pages
                full_text = '\n'.join(all_text)
                
                # Extract header info (from first page)
                header_data = self.extract_invoice_header(all_text[0])
                invoice_data.update(header_data)
                
                # Extract line items from all pages
                invoice_data["nomenclature"] = self.extract_line_items(full_text)
                
                # Extract total
                invoice_data["total_amount"] = self.extract_total_amount(full_text)
                
                # Calculate confidence
                confidence = self.calculate_confidence(invoice_data)
                
                return invoice_data, confidence
                
        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            return invoice_data, 0.0
    
    def extract_from_text(self, text: str) -> Tuple[Dict, float]:
        """Extract from already extracted text (for testing)"""
        invoice_data = {
            "invoice_number": None,
            "invoice_date": None,
            "nomenclature": [],
            "total_amount": None
        }
        
        # Extract header
        header_data = self.extract_invoice_header(text)
        invoice_data.update(header_data)
        
        # Extract line items
        invoice_data["nomenclature"] = self.extract_line_items(text)
        
        # Extract total
        invoice_data["total_amount"] = self.extract_total_amount(text)
        
        # Calculate confidence
        confidence = self.calculate_confidence(invoice_data)
        
        return invoice_data, confidence

    def enrich_with_ref_keys(self, invoice_data: Dict) -> Dict:
        """
        Enrich each nomenclature line item with 1C Ref_Keys from the OData API:
        - item['ref_key']: nomenclature (Catalog_Номенклатура) Ref_Key
        - item['order_ref_key']: order document (Document_ЗаказПоставщику) Ref_Key
        - item['line_number']: line number of the nomenclature inside its order
        If token is missing or API fails, keys stay None.
        """
        nomenclature_list = invoice_data.get("nomenclature") or []
        if not nomenclature_list:
            return invoice_data

        unique_codes = list(dict.fromkeys(item.get("nomenclature") for item in nomenclature_list if item.get("nomenclature")))
        ref_keys = get_ref_keys(unique_codes)

        unique_orders = list(dict.fromkeys(item.get("order_number") for item in nomenclature_list if item.get("order_number")))
        order_ref_keys = get_order_ref_keys(unique_orders)

        for item in nomenclature_list:
            code = item.get("nomenclature")
            item["ref_key"] = ref_keys.get(code) if code else None
            order_num = item.get("order_number")
            item["order_ref_key"] = order_ref_keys.get(order_num) if order_num else None

        # Step 3: fetch order line numbers and map each invoice line to the order line.
        unique_order_ref_keys = list(
            dict.fromkeys(
                item.get("order_ref_key")
                for item in nomenclature_list
                if item.get("order_ref_key")
            )
        )
        order_line_numbers_by_order_ref_key = {
            order_ref_key: get_order_line_numbers(order_ref_key)
            for order_ref_key in unique_order_ref_keys
        }

        for item in nomenclature_list:
            order_ref_key = item.get("order_ref_key")
            ref_key = item.get("ref_key")
            line_numbers_for_order = order_line_numbers_by_order_ref_key.get(order_ref_key, {})
            item["line_number"] = (
                line_numbers_for_order.get(str(ref_key)) if ref_key else None
            )

        return invoice_data
    
    def to_dataframe(self, invoice_data: Dict) -> pd.DataFrame:
        """Convert nomenclature to pandas DataFrame for easy viewing/export"""
        if not invoice_data.get('nomenclature'):
            return pd.DataFrame()
        
        df = pd.DataFrame(invoice_data['nomenclature'])
        
        # Add invoice header info to each row
        df['invoice_number'] = invoice_data.get('invoice_number')
        df['invoice_date'] = invoice_data.get('invoice_date')
        
        # Reorder columns
        cols = ['invoice_number', 'invoice_date', 'order_number', 'order_date', 'order_ref_key',
                'position', 'nomenclature', 'ref_key', 'quantity', 'unit', 'unit_price', 'line_total']
        # Only include columns that exist in the DataFrame
        cols = [col for col in cols if col in df.columns]
        df = df[cols]
        
        return df


def main():
    """Test the extractor"""
    extractor = InvoiceExtractor()
    
    # Test with extracted text
    text_path = Path('/mnt/project/extracted_text_example_')
    with open(text_path, 'r') as f:
        text = f.read()
    
    invoice_data, confidence = extractor.extract_from_text(text)
    
    print("=" * 80)
    print("INVOICE EXTRACTION RESULTS")
    print("=" * 80)
    print(f"\n📄 Invoice Number: {invoice_data['invoice_number']}")
    print(f"📅 Invoice Date: {invoice_data['invoice_date']}")
    print(f"💰 Total Amount: €{invoice_data['total_amount']:,.2f}" if invoice_data['total_amount'] else "💰 Total Amount: Not found")
    print(f"\n✅ Extracted {len(invoice_data['nomenclature'])} line items")
    print(f"🎯 Confidence Score: {confidence:.1%}")
    
    # Show sample items
    if invoice_data['nomenclature']:
        print("\n" + "-" * 80)
        print("SAMPLE LINE ITEMS (First 5)")
        print("-" * 80)
        for item in invoice_data['nomenclature'][:5]:
            print(f"\n📦 Position {item['position']}:")
            print(f"   CA Code: {item['nomenclature']}")
            print(f"   Quantity: {item['quantity']:,.0f} {item['unit']}")
            print(f"   Unit Price: €{item['unit_price']:,.2f}")
            print(f"   Line Total: €{item['line_total']:,.2f}")
    
    # Convert to DataFrame
    df = extractor.to_dataframe(invoice_data)
    if not df.empty:
        print("\n" + "=" * 80)
        print("DATAFRAME PREVIEW")
        print("=" * 80)
        print(df.head(10).to_string())
    
    # Save results
    output_dir = Path('/mnt/user-data/outputs')
    output_dir.mkdir(exist_ok=True)
    
    # Save JSON
    json_path = output_dir / 'invoice_extraction.json'
    with open(json_path, 'w') as f:
        json.dump({
            'invoice_data': invoice_data,
            'confidence': confidence
        }, f, indent=2, ensure_ascii=False)
    print(f"\n💾 Saved JSON to: {json_path}")
    
    # Save CSV
    if not df.empty:
        csv_path = output_dir / 'invoice_line_items.csv'
        df.to_csv(csv_path, index=False)
        print(f"💾 Saved CSV to: {csv_path}")
        print(f"\n✨ Total line items: {len(df)}")
        print(f"✨ Total invoice value: €{df['line_total'].sum():,.2f}")


if __name__ == "__main__":
    main()