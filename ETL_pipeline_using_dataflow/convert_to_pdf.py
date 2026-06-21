#!/usr/bin/env python3
"""
Convert Markdown to PDF

This script converts PROJECT_DOCUMENTATION.md to PDF format.
It uses markdown2 library for conversion and weasyprint for PDF generation.

Installation:
    pip install markdown2 weasyprint

Usage:
    python convert_to_pdf.py
"""

import os
import sys
from pathlib import Path

def convert_markdown_to_pdf_with_weasyprint():
    """Convert markdown to PDF using weasyprint (supports CSS)"""
    try:
        import markdown2
        from weasyprint import HTML, CSS
        from io import BytesIO
    except ImportError:
        print("❌ Required libraries not found.")
        print("\nInstall with:")
        print("  pip install markdown2 weasyprint")
        return False
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    md_file = script_dir / "PROJECT_DOCUMENTATION.md"
    pdf_file = script_dir / "PROJECT_DOCUMENTATION.pdf"
    
    if not md_file.exists():
        print(f"❌ Error: {md_file} not found")
        return False
    
    try:
        print(f"📖 Reading markdown file: {md_file.name}")
        with open(md_file, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        print("🔄 Converting markdown to HTML...")
        html_content = markdown2.markdown(
            markdown_content,
            extras=['tables', 'fenced-code-blocks', 'break-on-newline']
        )
        
        # Wrap in HTML structure with CSS for PDF styling
        full_html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <title>GCP Dataflow Pipeline Project Documentation</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    margin: 40px;
                    max-width: 900px;
                    background: white;
                }}
                h1 {{
                    color: #1a73e8;
                    border-bottom: 3px solid #1a73e8;
                    padding-bottom: 10px;
                    page-break-after: avoid;
                    margin-top: 40px;
                }}
                h2 {{
                    color: #1a73e8;
                    margin-top: 30px;
                    page-break-after: avoid;
                }}
                h3 {{
                    color: #4285f4;
                    page-break-after: avoid;
                }}
                h4 {{
                    color: #4285f4;
                }}
                p {{
                    margin: 10px 0;
                }}
                ul, ol {{
                    margin: 15px 0;
                }}
                li {{
                    margin: 5px 0;
                }}
                table {{
                    border-collapse: collapse;
                    width: 100%;
                    margin: 20px 0;
                    page-break-inside: avoid;
                }}
                th {{
                    background-color: #f0f0f0;
                    border: 1px solid #ddd;
                    padding: 12px;
                    text-align: left;
                    font-weight: bold;
                }}
                td {{
                    border: 1px solid #ddd;
                    padding: 12px;
                }}
                tr:nth-child(even) {{
                    background-color: #f9f9f9;
                }}
                code {{
                    background-color: #f4f4f4;
                    padding: 2px 5px;
                    border-radius: 3px;
                    font-family: 'Courier New', monospace;
                }}
                pre {{
                    background-color: #f4f4f4;
                    padding: 15px;
                    border-radius: 5px;
                    overflow-x: auto;
                    page-break-inside: avoid;
                }}
                pre code {{
                    background: none;
                    padding: 0;
                }}
                blockquote {{
                    border-left: 4px solid #1a73e8;
                    padding-left: 15px;
                    margin-left: 0;
                    color: #666;
                }}
                .checkmark {{
                    color: #34a853;
                }}
                @page {{
                    size: A4;
                    margin: 2.5cm;
                    @bottom-center {{
                        content: "Page " counter(page) " of " counter(pages);
                    }}
                }}
            </style>
        </head>
        <body>
            {html_content}
        </body>
        </html>
        """
        
        print(f"📝 Generating PDF: {pdf_file.name}")
        HTML(string=full_html).write_pdf(str(pdf_file))
        
        print(f"✅ Success! PDF created: {pdf_file}")
        print(f"📍 Location: {pdf_file}")
        return True
        
    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        return False


def convert_markdown_to_pdf_simple():
    """Simple markdown to PDF conversion using basic HTML to PDF"""
    try:
        import markdown2
        from pathlib import Path
    except ImportError:
        print("❌ Required library 'markdown2' not found.")
        print("\nInstall with:")
        print("  pip install markdown2")
        return False
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    md_file = script_dir / "PROJECT_DOCUMENTATION.md"
    html_file = script_dir / "PROJECT_DOCUMENTATION.html"
    
    if not md_file.exists():
        print(f"❌ Error: {md_file} not found")
        return False
    
    try:
        print(f"📖 Reading markdown file: {md_file.name}")
        with open(md_file, 'r', encoding='utf-8') as f:
            markdown_content = f.read()
        
        print("🔄 Converting markdown to HTML...")
        html_content = markdown2.markdown(
            markdown_content,
            extras=['tables', 'fenced-code-blocks']
        )
        
        # Wrap in HTML structure
        full_html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>GCP Dataflow Pipeline Project Documentation</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 20px;
            color: #333;
        }}
        h1 {{ color: #1a73e8; border-bottom: 2px solid #1a73e8; }}
        h2 {{ color: #1a73e8; margin-top: 30px; }}
        h3 {{ color: #4285f4; }}
        table {{ border-collapse: collapse; width: 100%; margin: 15px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
        th {{ background: #f0f0f0; }}
        code {{ background: #f4f4f4; padding: 2px 5px; }}
        pre {{ background: #f4f4f4; padding: 10px; overflow-x: auto; }}
    </style>
</head>
<body>
    {html_content}
</body>
</html>"""
        
        print(f"📝 Creating HTML file: {html_file.name}")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(full_html)
        
        print(f"✅ HTML file created: {html_file}")
        print(f"\n📌 To convert to PDF:")
        print(f"   1. Open {html_file.name} in your browser")
        print(f"   2. Press Ctrl+P")
        print(f"   3. Click 'Save as PDF'")
        print(f"\n   OR install weasyprint and try again:")
        print(f"   pip install weasyprint")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


if __name__ == "__main__":
    print("=" * 60)
    print("Markdown to PDF Converter")
    print("=" * 60)
    print()
    
    # Try weasyprint method first (better quality)
    if convert_markdown_to_pdf_with_weasyprint():
        sys.exit(0)
    
    # Fallback to HTML method
    print("\n🔄 Trying alternative method...")
    print()
    if convert_markdown_to_pdf_simple():
        sys.exit(0)
    
    print("\n❌ Conversion failed. Please install required packages:")
    print("   pip install markdown2 weasyprint")
    sys.exit(1)
