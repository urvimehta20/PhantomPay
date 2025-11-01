#!/usr/bin/env python3
"""
1. Segregates PDF and TXT files
2. Scrapes JSON data from PDFs
3. Uploads to Convex with vector search support
"""

import os
import json
import re
from pathlib import Path
from typing import Dict, Optional, List
import requests

try:
    import PyPDF2
except ImportError:
    PyPDF2 = None

# Convex Configuration
CONVEX_URL = "https://marvelous-emu-964.convex.cloud"
CONVEX_FUNCTION = "processInvoice"  # This is now an action


def segregate_files(source_dir: Optional[str] = None) -> tuple:
    """Segregate PDFs to data/, TXT files to data/text_data/"""
    if source_dir is None:
        source_dir = os.getcwd()
    
    source_path = Path(source_dir)
    if not source_path.exists():
        raise FileNotFoundError(f"Directory not found: {source_dir}")
    
    data_folder = source_path / "data_2"
    text_data_folder = data_folder / "text_data"
    
    data_folder.mkdir(exist_ok=True)
    text_data_folder.mkdir(exist_ok=True)
    
    txt_count = 0
    pdf_count = 0
    
    for file_path in source_path.iterdir():
        if file_path.is_dir() or file_path.name.startswith('.'):
            continue
        
        if file_path.suffix.lower() == ".txt":
            destination = text_data_folder / file_path.name
            file_path.rename(destination)
            txt_count += 1
        elif file_path.suffix.lower() == ".pdf":
            destination = data_folder / file_path.name
            file_path.rename(destination)
            pdf_count += 1
    
    return txt_count, pdf_count


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from PDF file."""
    if PyPDF2 is None:
        raise ImportError("PyPDF2 required: pip install PyPDF2")
    
    text_content = []
    with open(pdf_path, "rb") as pdf_file:
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        for page in pdf_reader.pages:
            text_content.append(page.extract_text())
    
    return "\n".join(text_content)


def parse_invoice_data(text_content: str) -> Dict:
    """Parse invoice data from text content - supports both INV-2025 and invoice_#### formats."""
    invoice_data = {
        "invoice_number": None,
        "order_number": None,
        "customer": None,
        "email": None,
        "order_date": None,
        "due_date": None,
        "subtotal": None,
        "tax": None,
        "total": None,
        "payment_completed": False,
        "payment_method": None,
        "transaction_id": None,
        "items": [],
        "notes": None
    }
    
    # Multiple patterns for each field to support different invoice formats
    patterns = {
        "invoice_number": [
            r'Invoice\s*#:\s*(INV-\d+-\d+)',  # invoice_#### format
            r'INVOICE:\s*(INV-\d+-\d+)',     # INV-2025 format
            r'(INV-\d+-\d+)',                # Fallback: any INV- pattern
        ],
        "order_number": [
            r'Order\s*#:\s*(ORD-\d+-\d+)',   # invoice_#### format
            r'ORDER:\s*(ORD-\d+-\d+)',       # INV-2025 format
            r'(ORD-\d+-\d+)',                # Fallback: any ORD- pattern
        ],
        "customer": [
            r'Bill\s+To\s*\n\s*([^\n]+)',   # invoice_#### format: "Bill To\nCustomer Name"
            r'Customer:\s*(.+?)(?:\n|$)',    # INV-2025 format
            r'Bill\s+To\s+([A-Z][^\n]+)',    # Alternative Bill To format
        ],
        "email": [
            r'Accounts\s+Payable:\s*([\w\.-]+@[\w\.-]+\.\w+)',  # invoice_#### format
            r'Email:\s*([\w\.-]+@[\w\.-]+\.\w+)',              # INV-2025 format
            r'([\w\.-]+@[\w\.-]+\.\w+)',                        # Fallback: any email pattern
        ],
        "order_date": [
            r'Invoice\s+Date:\s*(\d{4}-\d{2}-\d{2})',  # invoice_#### format
            r'Order\s+date:\s*(\d{4}-\d{2}-\d{2})',     # INV-2025 format
            r'Invoice\s+Date[:\s]+(\d{4}-\d{2}-\d{2})', # Alternative
        ],
        "due_date": [
            r'Due\s+Date:\s*(\d{4}-\d{2}-\d{2})',      # Both formats
            r'Due\s+date:\s*(\d{4}-\d{2}-\d{2})',       # Case variation
        ],
        "subtotal": [
            r'Subtotal:\s*\n\s*\$?([\d,]+\.?\d*)',    # invoice_#### format (Subtotal:\n$13,365.96)
            r'Subtotal:\s*\$?([\d,]+\.?\d*)',          # invoice_#### format ($12,027.58)
            r'Subtotal:\s*([\d.]+)\s*USD',             # INV-2025 format
            r'Subtotal[:\s]+\$?([\d,]+\.?\d*)',        # Alternative
        ],
        "tax": [
            r'Tax\s*\([^)]*\):\s*\n\s*\$?([\d,]+\.?\d*)',  # invoice_#### format (Tax (9%):\n$1,202.94)
            r'Tax\s*\([^)]*\):\s*\$?([\d,]+\.?\d*)',  # invoice_#### format (Tax (9%): $1,082.48)
            r'Tax:\s*([\d.]+)\s*USD',                  # INV-2025 format
            r'Tax[:\s]+\$?([\d,]+\.?\d*)',             # Alternative
        ],
        "total": [
            r'(?:Tax.*?)\nTotal:\s*\n\s*\$?([\d,]+\.?\d*)',  # invoice_#### format (after Tax section)
            r'Total:\s*\n\s*\$?([\d,]+\.?\d*)',        # invoice_#### format (Total:\n$14,568.90)
            r'Total:\s*\$?([\d,]+\.?\d*)',             # invoice_#### format (Total: $13,110.06)
            r'Total:\s*([\d.]+)\s*USD',                # INV-2025 format
            r'Total[:\s]+\$?([\d,]+\.?\d*)',           # Alternative
        ],
        "payment_completed": [
            r'Payment\s+completed:\s*(True|False)',     # INV-2025 format
            r'Status:\s*(Paid|Overdue|Unpaid)',         # invoice_#### format (Paid/Overdue)
            r'Status[:\s]+(Paid)',                      # Paid status check
        ],
        "payment_method": [
            r'Payment\s+method:\s*(.+)',                # INV-2025 format
            r'Payment\s+Method[:\s]+(.+)',             # Case variation
        ],
        "transaction_id": [
            r'Transaction\s+ID:\s*(.+)',                # INV-2025 format
            r'Transaction\s+Id[:\s]+(.+)',             # Case variation
        ],
    }
    
    # Try each pattern until one matches
    for key, pattern_list in patterns.items():
        # Special handling for "total" - use last match (invoice_#### format has lowercase "total:" for subtotal)
        if key == "total":
            all_matches = []
            for pattern in pattern_list:
                matches = list(re.finditer(pattern, text_content, re.IGNORECASE | re.MULTILINE))
                if matches:
                    all_matches.extend(matches)
            if all_matches:
                # Use the last match (should be the actual Total after Tax)
                match = all_matches[-1]
                value = match.group(1).strip()
                # Remove commas and convert to float
                value = value.replace(',', '')
                try:
                    invoice_data[key] = float(value)
                except ValueError:
                    pass
            continue
        
        # For other fields, use first matching pattern
        for pattern in pattern_list:
            match = re.search(pattern, text_content, re.IGNORECASE | re.MULTILINE)
            if match:
                value = match.group(1).strip()
                
                # Clean and convert numeric values
                if key in ["subtotal", "tax"]:
                    # Remove commas and convert to float
                    value = value.replace(',', '')
                    try:
                        invoice_data[key] = float(value)
                    except ValueError:
                        pass
                elif key == "payment_completed":
                    # Handle both True/False and Paid/Overdue formats
                    if value.lower() in ["true", "paid"]:
                        invoice_data[key] = True
                    elif value.lower() in ["false", "overdue", "unpaid"]:
                        invoice_data[key] = False
                elif key in ["payment_method", "transaction_id"]:
                    invoice_data[key] = value if value else None
                else:
                    invoice_data[key] = value
                break  # Use first matching pattern
    
    # Extract items - handle table format (invoice_####) and list format (INV-2025)
    items = []
    
    # Table format extraction (for invoice_#### format)
    # Look for the items table between "Description" header and "Subtotal"
    table_match = re.search(
        r'Description\s+Qty\s+Unit\s+Price\s+Amount\s*\n(.*?)(?=Subtotal:)',
        text_content,
        re.IGNORECASE | re.DOTALL
    )
    if table_match:
        table_text = table_match.group(1)
        # Parse table - items are on separate lines: Description\nQty\nUnit Price\nAmount
        lines = [line.strip() for line in table_text.split('\n') if line.strip()]
        i = 0
        while i < len(lines):
            line = lines[i]
            # Skip header rows
            if any(header.lower() in line.lower() for header in ['Description', 'Qty', 'Unit Price', 'Amount']):
                i += 1
                continue
            
            # Check if this looks like a description (starts with letter, no $ sign)
            if re.match(r'^[A-Za-z]', line) and '$' not in line:
                description = line.strip()
                # Next lines should be: Qty, Unit Price, Amount
                qty = None
                unit_price = None
                amount = None
                
                if i + 1 < len(lines) and re.match(r'^\d+$', lines[i + 1]):
                    qty = lines[i + 1]
                    i += 1
                if i + 1 < len(lines) and '$' in lines[i + 1]:
                    unit_price = lines[i + 1].replace('$', '').replace(',', '').strip()
                    i += 1
                if i + 1 < len(lines) and '$' in lines[i + 1]:
                    amount = lines[i + 1].replace('$', '').replace(',', '').strip()
                    i += 1
                
                # Format item string
                if qty and unit_price and amount:
                    items.append(f"- {description} x{qty} (${unit_price} each = ${amount})")
                elif description:
                    items.append(f"- {description}")
            
            i += 1
    
    # List format extraction (for INV-2025 format) - fallback if table format didn't work
    if not items:
        items_match = re.search(r'Items:(.*?)(?:Notes:|Subtotal:|$)', text_content, re.IGNORECASE | re.DOTALL)
        if items_match:
            items_text = items_match.group(1).strip()
            items = [
                line.strip() for line in items_text.split("\n")
                if line.strip() and line.strip().startswith("-")
            ]
    
    invoice_data["items"] = items
    
    # Extract notes
    notes_match = re.search(r'Notes:\s*(.+?)(?:\n\n|Payment\s+completed|Status:|$)', text_content, re.IGNORECASE | re.DOTALL)
    if notes_match:
        notes_text = notes_match.group(1).strip()
        # Clean up notes - remove trailing status info if present
        notes_text = re.sub(r'\s*Payment\s+completed.*$', '', notes_text, flags=re.IGNORECASE)
        invoice_data["notes"] = notes_text.strip()
    
    return invoice_data


def scrape_pdf(pdf_path: str) -> Dict:
    """Scrape invoice data from PDF file (with TXT fallback)."""
    pdf_path_obj = Path(pdf_path)
    
    # Try PDF extraction
    text_content = ""
    try:
        if PyPDF2:
            text_content = extract_text_from_pdf(pdf_path)
    except Exception:
        pass
    
    # Fallback to TXT file - check multiple possible locations
    if not text_content or not text_content.strip():
        # Try multiple possible locations for TXT file
        possible_txt_paths = [
            pdf_path_obj.parent / "text_data" / pdf_path_obj.with_suffix(".txt").name,
            pdf_path_obj.parent.parent / "text_data" / pdf_path_obj.with_suffix(".txt").name,
            pdf_path_obj.parent.parent.parent / "text_data" / pdf_path_obj.with_suffix(".txt").name,
            pdf_path_obj.with_suffix(".txt"),
            Path("data/text_data") / pdf_path_obj.with_suffix(".txt").name,
        ]
        
        txt_path = None
        for possible_path in possible_txt_paths:
            if possible_path.exists():
                txt_path = possible_path
                break
        
        if txt_path:
            with open(txt_path, "r", encoding="utf-8") as f:
                text_content = f.read()
        else:
            raise ValueError(f"Could not extract text from {pdf_path} and no TXT fallback found. Tried: {[str(p) for p in possible_txt_paths[:3]]}")
    
    # Parse invoice data
    invoice_data = parse_invoice_data(text_content)
    invoice_data["filename"] = pdf_path_obj.name
    invoice_data["file_path"] = str(pdf_path_obj)
    
    return invoice_data


def upload_to_convex(data: Dict, convex_url: str, convex_function: str) -> Dict:
    """Upload invoice data to Convex (uses action endpoint)."""
    endpoint = f"{convex_url.rstrip('/')}/api/action"
    
    payload = {
        "path": convex_function,
        "args": [{"data": data}],
        "format": "json"
    }
    
    response = requests.post(endpoint, json=payload, timeout=60)
    response.raise_for_status()
    
    return response.json()


def convert_pdfs_to_json(data_2_folder: str, output_folder: Optional[str] = None) -> List[Dict]:
    """
    Convert all PDFs in data_2 folder to JSON files.
    
    Args:
        data_2_folder: Path to the data_2 folder containing PDFs
        output_folder: Optional path to save JSON files (default: data_2/json_output)
    
    Returns:
        List of conversion results
    """
    data_2_path = Path(data_2_folder)
    if not data_2_path.exists():
        raise FileNotFoundError(f"Directory not found: {data_2_folder}")
    
    # Set output folder
    if output_folder is None:
        output_folder = data_2_path / "json_output"
    else:
        output_folder = Path(output_folder)
    
    output_folder.mkdir(parents=True, exist_ok=True)
    
    print("=" * 60)
    print("PDF TO JSON CONVERSION PIPELINE")
    print("=" * 60)
    print(f"Source folder: {data_2_path}")
    print(f"Output folder: {output_folder}")
    
    # Find all PDF files in data_2
    pdf_files = list(data_2_path.glob("*.pdf"))
    pdf_files.sort()  # Sort for consistent processing
    
    if not pdf_files:
        print(f"\n⚠ No PDF files found in {data_2_path}")
        return []
    
    print(f"\n[Found {len(pdf_files)} PDF file(s)]")
    print(f"\n[Converting PDFs to JSON...]")
    
    results = []
    successful = 0
    failed = 0
    
    for pdf_file in pdf_files:
        try:
            # Scrape PDF and extract invoice data
            invoice_data = scrape_pdf(str(pdf_file))
            invoice_num = invoice_data.get("invoice_number", pdf_file.stem)
            
            # Create JSON filename
            json_filename = pdf_file.stem + ".json"
            json_path = output_folder / json_filename
            
            # Save to JSON file
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(invoice_data, f, indent=2, default=str, ensure_ascii=False)
            
            successful += 1
            print(f"  ✓ {pdf_file.name} → {json_filename} [{invoice_num}]")
            
            results.append({
                "file": pdf_file.name,
                "json_file": json_filename,
                "invoice_number": invoice_num,
                "status": "converted",
                "output_path": str(json_path)
            })
            
        except Exception as e:
            failed += 1
            print(f"  ✗ {pdf_file.name}: {e}")
            results.append({
                "file": pdf_file.name,
                "status": "error",
                "error": str(e)
            })
    
    # Summary
    print("\n" + "=" * 60)
    print("CONVERSION COMPLETE")
    print("=" * 60)
    print(f"✓ Successfully converted: {successful}")
    print(f"✗ Failed: {failed}")
    print(f"📁 JSON files saved to: {output_folder}")
    
    return results


def run_pipeline(source_dir: Optional[str] = None) -> List[Dict]:
    """
    Complete pipeline: Segregate → Scrape → Upload to Convex
    
    Returns list of results
    """
    if source_dir is None:
        source_dir = os.getcwd()
    
    print("=" * 60)
    print("INVOICE PROCESSING PIPELINE")
    print("=" * 60)
    
    # Step 1: Segregate files
    print("\n[Step 1] Segregating files...")
    txt_count, pdf_count = segregate_files(source_dir)
    print(f"✓ Organized: {txt_count} TXT files, {pdf_count} PDF files")
    
    if pdf_count == 0:
        print("\n⚠ No PDF files found")
        return []
    
    # Step 2: Scrape PDFs
    print(f"\n[Step 2] Scraping {pdf_count} PDF file(s)...")
    data_folder = Path(source_dir) / "data"
    
    # Check if PDFs are in data/ or data/data/ (nested structure)
    pdf_files = list(data_folder.glob("*.pdf"))
    if not pdf_files:
        nested_data = data_folder / "data"
        if nested_data.exists():
            pdf_files = list(nested_data.glob("*.pdf"))
            data_folder = nested_data
    
    results = []
    for pdf_file in pdf_files:
        try:
            invoice_data = scrape_pdf(str(pdf_file))
            invoice_num = invoice_data.get("invoice_number", "Unknown")
            print(f"  ✓ {pdf_file.name} → {invoice_num}")
            
            # Step 3: Upload to Convex
            print(f"    Uploading to Convex...", end=" ")
            try:
                response = upload_to_convex(invoice_data, CONVEX_URL, CONVEX_FUNCTION)
                if isinstance(response, dict) and response.get("success"):
                    print("✓")
                    results.append({
                        "file": pdf_file.name,
                        "invoice_number": invoice_num,
                        "status": "uploaded",
                        "convex_id": response.get("invoiceId")
                    })
                else:
                    print(f"✗ Error: {response.get('errorMessage', 'Unknown')}")
                    results.append({
                        "file": pdf_file.name,
                        "status": "error",
                        "error": response.get("errorMessage", "Unknown error")
                    })
            except Exception as e:
                print(f"✗ {e}")
                results.append({
                    "file": pdf_file.name,
                    "status": "error",
                    "error": str(e)
                })
        except Exception as e:
            print(f"  ✗ {pdf_file.name}: {e}")
            results.append({
                "file": pdf_file.name,
                "status": "error",
                "error": str(e)
            })
    
    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    uploaded = sum(1 for r in results if r.get("status") == "uploaded")
    errors = len(results) - uploaded
    print(f"✓ Uploaded to Convex: {uploaded}")
    print(f"✗ Errors: {errors}")
    
    return results


if __name__ == "__main__":
    import sys
    
    # Check if user wants to convert data_2 PDFs to JSON
    if len(sys.argv) > 1 and sys.argv[1] == "--convert-data2":
        # Convert PDFs in data_2 folder to JSON
        data_2_path = Path("data_2")
        if not data_2_path.exists():
            print(f"Error: {data_2_path} folder not found!")
            sys.exit(1)
        
        output_folder = sys.argv[2] if len(sys.argv) > 2 else None
        results = convert_pdfs_to_json(str(data_2_path), output_folder)
        
        # Save conversion results
        results_file = Path("data_2_conversion_results.json")
        with open(results_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nConversion results saved to: {results_file}")
    else:
        # Original pipeline behavior
        source_dir = sys.argv[1] if len(sys.argv) > 1 else None
        results = run_pipeline(source_dir)
        
        # Save results
        output_file = Path("pipeline_results.json")
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nResults saved to: {output_file}")
