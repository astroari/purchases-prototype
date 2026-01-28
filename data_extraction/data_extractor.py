"""
Invoice Data Extractor using pdfplumber
Extracts structured data from Hettich-style invoices
"""

import pdfplumber
import pandas as pd
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import json


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
                        "nomenclature": catalog_code,  # Just the CA code
                        "quantity": quantity,
                        "unit": unit,
                        "unit_price": unit_price,
                        "line_total": line_total,
                        "order_number": current_order_number,
                        "order_date": current_order_date,
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
    
    def to_dataframe(self, invoice_data: Dict) -> pd.DataFrame:
        """Convert nomenclature to pandas DataFrame for easy viewing/export"""
        if not invoice_data.get('nomenclature'):
            return pd.DataFrame()
        
        df = pd.DataFrame(invoice_data['nomenclature'])
        
        # Add invoice header info to each row
        df['invoice_number'] = invoice_data.get('invoice_number')
        df['invoice_date'] = invoice_data.get('invoice_date')
        
        # Reorder columns
        cols = ['invoice_number', 'invoice_date', 'order_number', 'order_date', 
                'position', 'nomenclature', 'quantity', 'unit', 'unit_price', 'line_total']
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