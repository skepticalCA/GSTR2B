import streamlit as st
import pandas as pd
import io
import re
import numpy as np

# ==========================================
# 1. SMART HEADER MAPPING (The Fix)
# ==========================================
def find_column(df, candidates):
    """
    Searches for a column in the dataframe that matches one of the candidate names.
    It ignores case, spaces, and special characters.
    """
    # 1. Create a normalized map of existing columns
    #    "Invoice Number " -> "invoicenumber"
    existing_cols = {c: str(c).strip().lower().replace(' ', '').replace('\n', '').replace('_', '') for c in df.columns}
    
    # 2. Check candidates
    for cand in candidates:
        clean_cand = cand.strip().lower().replace(' ', '').replace('_', '')
        
        # Look for exact match in cleaned keys
        for original_col, clean_col in existing_cols.items():
            if clean_cand == clean_col:
                return original_col
                
    return None

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def advanced_normalize_invoice(inv_num):
    if pd.isna(inv_num) or str(inv_num).strip() == '': return ""
    s = str(inv_num).upper()
    s = "".join(s.split()) 
    s = re.sub(r'[^A-Z0-9]', '', s)
    s = s.lstrip('0')
    return s if s else "0"

def clean_currency(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    if isinstance(val, (int, float)): return float(val)
    try:
        clean_str = str(val).replace(',', '').replace(' ', '').replace('₹', '')
        return float(clean_str)
    except ValueError:
        return 0.0

def normalize_gstin(gstin):
    if pd.isna(gstin): return ""
    return str(gstin).strip().upper().replace(" ", "")

# ==========================================
# 3. CORE LOGIC
# ==========================================
def run_reconciliation(cis_df, gstr2b_df, col_map_cis, col_map_g2b, tolerance):
    cis_proc = cis_df.copy()
    g2b_proc = gstr2b_df.copy()
    
    # --- PREP CIS ---
    if 'Index CIS' not in cis_proc.columns:
        cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)

    # Normalize Keys using the DYNAMICALLY FOUND columns
    cis_proc['Norm_GSTIN'] = cis_proc[col_map_cis['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_Invoice'] = cis_proc[col_map_cis['INVOICE']].apply(advanced_normalize_invoice)
    
    # Financials
    cis_proc['Taxable_Clean'] = cis_proc[col_map_cis['TAXABLE']].apply(clean_currency)
    cis_proc['Total_Tax'] = (
        cis_proc[col_map_cis['IGST']].apply(clean_currency) + 
        cis_proc[col_map_cis['CGST']].apply(clean_currency) + 
        cis_proc[col_map_cis['SGST']].apply(clean_currency)
    )

    cis_proc['Matching Status'] = "Unmatched"
    cis_proc['Short Remark'] = "Not Found"
    cis_proc['Detailed Remark'] = ""
    cis_proc['GSTR 2B Key'] = ""

    # --- PREP GSTR-2B ---
    if 'INDEX' not in g2b_proc.columns:
        g2b_proc['INDEX'] = g2b_proc.index + 100000 

    g2b_proc['Norm_GSTIN'] = g2b_proc[col_map_g2b['GSTIN']].apply(normalize_gstin)
    g2b_proc['Norm_Invoice'] = g2b_proc[col_map_g2b['INVOICE']].apply(advanced_normalize_invoice)
    
    g2b_proc['Taxable_Clean'] = g2b_proc[col_map_g2b['TAXABLE']].apply(clean_currency)
    g2b_proc['Total_Tax_Clean'] = (
        g2b_proc[col_map_g2b['IGST']].apply(clean_currency) + 
        g2b_proc[col_map_g2b['CGST']].apply(clean_currency) + 
        g2b_proc[col_map_g2b['SGST']].apply(clean_currency)
    )

    g2b_proc['CIS Key'] = ""
    g2b_proc['Matching Status'] = "Unmatched"

    # --- CLUBBING & MATCHING ---
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_Invoice']).agg({
        'Taxable_Clean': 'sum',
        'Total_Tax': 'sum',
        col_map_cis['DATE']: 'first',
        'Index CIS': list
    }).reset_index()

    matched_g2b_indices = set()

    for idx, row_cis_group in cis_grouped.iterrows():
        gstin = row_cis_group['Norm_GSTIN']
        inv_num = row_cis_group['Norm_Invoice']
        
        if not gstin or not inv_num: continue

        candidates = g2b_proc[
            (g2b_proc['Norm_GSTIN'] == gstin) & 
            (g2b_proc['Norm_Invoice'] == inv_num) &
            (~g2b_proc['INDEX'].isin(matched_g2b_indices))
        ]
        
        match_found = False
        short_rem = "Unmatched"
        detail_rem = []
        matched_g2b_idx = None
        
        if not candidates.empty:
            for i, row_g2b in candidates.iterrows():
                diff_tax = abs(row_cis_group['Total_Tax'] - row_g2b['Total_Tax_Clean'])
                diff_taxable = abs(row_cis_group['Taxable_Clean'] - row_g2b['Taxable_Clean'])
                
                # Date Check
                cis_date = pd.to_datetime(row_cis_group[col_map_cis['DATE']], dayfirst=True, errors='coerce')
                g2b_date = pd.to_datetime(row_g2b[col_map_g2b['DATE']], dayfirst=True, errors='coerce')
                
                date_str = ""
                if pd.notna(cis_date) and pd.notna(g2b_date) and cis_date != g2b_date:
                     date_str = f" | Date Diff: {cis_date.strftime('%d-%m')} vs {g2b_date.strftime('%d-%m')}"

                if diff_taxable <= tolerance and diff_tax <= tolerance:
                    match_found = True
                    matched_g2b_idx = row_g2b['INDEX']
                    short_rem = "Matched"
                    if date_str: detail_rem.append(f"Matched w/ Date Diff{date_str}")
                    break
                else:
                    detail_rem.append(f"Value Diff: Taxable {diff_taxable:.2f}, Tax {diff_tax:.2f}")
            
            if not match_found: short_rem = "Value Mismatch"
        else:
            short_rem = "Invoice Not Found"
            detail_rem.append("No invoice number match in GSTR-2B")

        original_cis_indices = row_cis_group['Index CIS']
        
        if match_found:
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'CIS Key'] = ", ".join(map(str, original_cis_indices))
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'Matching Status'] = "Matched"
            matched_g2b_indices.add(matched_g2b_idx)
            
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = matched_g2b_idx
                if detail_rem: cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = "; ".join(detail_rem)
        else:
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Unmatched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                # Update Remarks
                existing = cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks']
                base_rem = str(existing.values[0]) if pd.notna(existing.values[0]) else ""
                final_detail = "; ".join(detail_rem)
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = final_detail
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = f"{base_rem} | {short_rem}: {final_detail}".strip(" |")

    # Time Barred Check
    cutoff_date = pd.Timestamp("2024-03-31")
    cis_proc['Date_Obj'] = pd.to_datetime(cis_proc[col_map_cis['DATE']], dayfirst=True, errors='coerce')
    time_barred_mask = (cis_proc['Date_Obj'] < cutoff_date) & (cis_proc['Date_Obj'].notna())
    cis_proc.loc[time_barred_mask, 'Short Remark'] = cis_proc.loc[time_barred_mask, 'Short Remark'] + " + Time Barred"
    
    # Cleanup
    return cis_proc, g2b_proc

# ==========================================
# 4. STREAMLIT UI
# ==========================================
st.set_page_config(page_title="GST Reconciliation Tool", layout="wide")
st.title("📊 Auto-Reconciliation Tool (Smart Header Fix)")

col1, col2 = st.columns(2)
with col1: cis_file = st.file_uploader("Upload CIS Unmatched File", type=['xlsx'], key="cis")
with col2: g2b_file = st.file_uploader("Upload GSTR-2B File", type=['xlsx'], key="g2b")
tolerance = st.number_input("Financial Tolerance (₹)", min_value=0.0, value=10.0, step=0.1)

if st.button("🚀 Run Reconciliation", type="primary"):
    if cis_file and g2b_file:
        with st.spinner("Processing..."):
            try:
                df_cis = pd.read_excel(cis_file)
                
                # Load GSTR-2B (Search for B2B sheet)
                xl = pd.ExcelFile(g2b_file)
                sheet_name = 'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0]
                
                # Header Search Loop for GSTR-2B
                df_g2b = None
                target_key = "GSTIN"
                found_header = False
                
                # Check first 5 rows for a known column "GSTIN"
                for i in range(5):
                    temp = pd.read_excel(g2b_file, sheet_name=sheet_name, header=i)
                    if find_column(temp, ['GSTIN of supplier', 'GSTIN', 'Supplier GSTIN']):
                        df_g2b = temp
                        found_header = True
                        break
                
                if not found_header:
                    st.error("❌ Could not detect header row in GSTR-2B. Please check the file.")
                    st.stop()

                # --- BUILD DYNAMIC MAPS ---
                # We define lists of "Possible Names" for each required field
                
                # CIS MAPPING
                cis_required = {
                    'GSTIN': ['SupplierGSTIN', 'GSTIN'],
                    'INVOICE': ['DocumentNumber', 'Invoice Number', 'Inv No'],
                    'DATE': ['DocumentDate', 'Invoice Date', 'Date'],
                    'TAXABLE': ['TaxableValue', 'Taxable Value'],
                    'IGST': ['IntegratedTaxAmount', 'Integrated Tax', 'IGST Amount'],
                    'CGST': ['CentralTaxAmount', 'Central Tax', 'CGST Amount'],
                    'SGST': ['StateUT TaxAmount', 'State/UT Tax', 'SGST Amount']
                }
                
                # GSTR2B MAPPING
                g2b_required = {
                    'GSTIN': ['GSTIN of supplier', 'Supplier GSTIN'],
                    'INVOICE': ['Invoice number', 'Invoice No'],
                    'DATE': ['Invoice Date', 'Date'],
                    'TAXABLE': ['Taxable Value (₹)', 'Taxable Value'],
                    'IGST': ['Integrated Tax(₹)', 'Integrated Tax'],
                    'CGST': ['Central Tax(₹)', 'Central Tax'],
                    'SGST': ['State/UT Tax(₹)', 'State/UT Tax']
                }

                final_map_cis = {}
                final_map_g2b = {}
                missing_cols = []

                # Find CIS Cols
                for key, candidates in cis_required.items():
                    found = find_column(df_cis, candidates)
                    if found: final_map_cis[key] = found
                    else: missing_cols.append(f"CIS: {candidates[0]}")

                # Find GSTR2B Cols
                for key, candidates in g2b_required.items():
                    found = find_column(df_g2b, candidates)
                    if found: final_map_g2b[key] = found
                    else: missing_cols.append(f"GSTR-2B: {candidates[0]}")

                if missing_cols:
                    st.error("❌ Missing Columns! The tool could not match these headers:")
                    st.write(missing_cols)
                    st.write("---")
                    st.write("Your GSTR-2B Columns found:", df_g2b.columns.tolist())
                    st.stop()

                # Run Logic with Dynamic Map
                cis_res, g2b_res = run_reconciliation(df_cis, df_g2b, final_map_cis, final_map_g2b, tolerance)
                
                st.success(f"Matched: {len(cis_res[cis_res['Matching Status'] == 'Matched'])}")
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                    g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                
                st.download_button("Download Result", output.getvalue(), "Reconciliation_Output.xlsx")

            except Exception as e:
                st.error(f"Error: {e}")
