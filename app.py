import streamlit as st
import pandas as pd
import io
import re
import numpy as np

# ==========================================
# 1. SMART HEADER STITCHING (Robust Loader)
# ==========================================
def load_gstr2b_with_stitching(file_obj, sheet_name):
    """
    Reads the first 8 rows to identify header rows.
    Stitches split headers (e.g. Row 2 'GSTIN', Row 3 'Invoice No') into one.
    """
    df_raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=8)
    
    idx_gstin = -1
    idx_inv = -1
    
    def row_contains(row_idx, keyword):
        row_vals = df_raw.iloc[row_idx].astype(str).str.lower().values
        return any(keyword.lower() in val for val in row_vals)

    for i in range(len(df_raw)):
        if row_contains(i, 'gstin'): idx_gstin = i
        if row_contains(i, 'invoice number') or row_contains(i, 'invoice no'): idx_inv = i
            
    if idx_gstin == -1: idx_gstin = 0
    if idx_inv == -1: idx_inv = 0
    
    header_end_row = max(idx_gstin, idx_inv)
    final_headers = []
    num_cols = df_raw.shape[1]
    
    for c in range(num_cols):
        val_gstin = str(df_raw.iloc[idx_gstin, c]).strip()
        val_inv = str(df_raw.iloc[idx_inv, c]).strip()
        
        if val_gstin.lower() == 'nan': val_gstin = ""
        if val_inv.lower() == 'nan': val_inv = ""
        
        if val_inv and not val_inv.startswith("Unnamed"):
            final_headers.append(val_inv)
        elif val_gstin and not val_gstin.startswith("Unnamed"):
            final_headers.append(val_gstin)
        else:
            final_headers.append(f"Column_{c}")

    df_final = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_end_row + 1)
    
    current_cols = len(df_final.columns)
    if len(final_headers) >= current_cols:
        df_final.columns = final_headers[:current_cols]
    else:
        df_final.columns = final_headers + [f"Col_{i}" for i in range(current_cols - len(final_headers))]
        
    return df_final

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def find_column(df, candidates):
    existing_cols = {
        str(c).strip().lower().replace(' ', '').replace('\n', '').replace('_', '').replace('(₹)', '').replace('₹', ''): c 
        for c in df.columns
    }
    for cand in candidates:
        clean_cand = cand.strip().lower().replace(' ', '').replace('_', '').replace('(₹)', '').replace('₹', '')
        if clean_cand in existing_cols:
            return existing_cols[clean_cand]
    return None

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
# 3. CORE LOGIC (With Detailed Remarks)
# ==========================================
def run_reconciliation(cis_df, gstr2b_df, col_map_cis, col_map_g2b, tolerance):
    cis_proc = cis_df.copy()
    g2b_proc = gstr2b_df.copy()
    
    # --- PREP CIS ---
    if 'Index CIS' not in cis_proc.columns:
        cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)

    cis_proc['Norm_GSTIN'] = cis_proc[col_map_cis['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_Invoice'] = cis_proc[col_map_cis['INVOICE']].apply(advanced_normalize_invoice)
    cis_proc['Taxable_Clean'] = cis_proc[col_map_cis['TAXABLE']].apply(clean_currency)
    
    igst = cis_proc[col_map_cis['IGST']].apply(clean_currency)
    cgst = cis_proc[col_map_cis['CGST']].apply(clean_currency)
    sgst = cis_proc[col_map_cis['SGST']].apply(clean_currency)
    cis_proc['Total_Tax'] = igst + cgst + sgst

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
    
    igst_g = g2b_proc[col_map_g2b['IGST']].apply(clean_currency)
    cgst_g = g2b_proc[col_map_g2b['CGST']].apply(clean_currency)
    sgst_g = g2b_proc[col_map_g2b['SGST']].apply(clean_currency)
    g2b_proc['Total_Tax_Clean'] = igst_g + cgst_g + sgst_g

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
        final_detail_str = ""
        matched_g2b_idx = None
        
        if not candidates.empty:
            # We have candidate(s) with matching GSTIN & Invoice Number
            # Try to find a financial match among them
            
            best_candidate_idx = -1
            
            for i, row_g2b in candidates.iterrows():
                diff_tax = abs(row_cis_group['Total_Tax'] - row_g2b['Total_Tax_Clean'])
                diff_taxable = abs(row_cis_group['Taxable_Clean'] - row_g2b['Taxable_Clean'])
                
                # Date Check
                cis_date = pd.to_datetime(row_cis_group[col_map_cis['DATE']], dayfirst=True, errors='coerce')
                g2b_date = pd.to_datetime(row_g2b[col_map_g2b['DATE']], dayfirst=True, errors='coerce')
                date_match = (pd.notna(cis_date) and pd.notna(g2b_date) and cis_date == g2b_date)

                # --- BUILD DETAILED REMARK STRING ---
                matched_parts = ["GSTIN", "Invoice Number"]
                mismatched_parts = []

                if diff_taxable <= tolerance:
                    matched_parts.append("Taxable Value")
                else:
                    mismatched_parts.append(f"Taxable Value (Diff: {diff_taxable:.2f})")

                if diff_tax <= tolerance:
                    matched_parts.append("Tax Amount")
                else:
                    mismatched_parts.append(f"Tax Amount (Diff: {diff_tax:.2f})")

                if date_match:
                    matched_parts.append("Date")
                elif pd.notna(cis_date) and pd.notna(g2b_date):
                    mismatched_parts.append(f"Date ({cis_date.strftime('%d-%m')} vs {g2b_date.strftime('%d-%m')})")

                # Construct the remark string for this candidate
                match_str = "Matched: " + ", ".join(matched_parts)
                if mismatched_parts:
                    match_str += " | Mismatch: " + ", ".join(mismatched_parts)
                
                # CHECK IF SUCCESSFUL MATCH
                if diff_taxable <= tolerance and diff_tax <= tolerance:
                    match_found = True
                    matched_g2b_idx = row_g2b['INDEX']
                    short_rem = "Matched"
                    final_detail_str = match_str # Use this string
                    break # Stop looking, we found a match
                else:
                    # If this candidate failed, save its string as the "reason" (in case we don't find a better match)
                    # We usually want the "closest" match, but for simplicity, we take the first/last one
                    final_detail_str = match_str 
            
            if not match_found: 
                short_rem = "Value Mismatch"
                # formatting for partial match
                if not final_detail_str: # Should overlap with loop else, but safety check
                     final_detail_str = "Matched: GSTIN, Invoice Number | Mismatch: Values outside tolerance"
        else:
            short_rem = "Invoice Not Found"
            final_detail_str = "Mismatch: Invoice Number not found in GSTR-2B for this GSTIN"

        # --- UPDATE RECORDS ---
        original_cis_indices = row_cis_group['Index CIS']
        
        if match_found:
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'CIS Key'] = ", ".join(map(str, original_cis_indices))
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'Matching Status'] = "Matched"
            matched_g2b_indices.add(matched_g2b_idx)
            
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = matched_g2b_idx
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = final_detail_str
        else:
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Unmatched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                
                existing = cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks']
                base_rem = str(existing.values[0]) if pd.notna(existing.values[0]) else ""
                
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = final_detail_str
                # Append to original comments column too
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = f"{base_rem} | {final_detail_str}".strip(" |")

    # Time Barred Check
    cutoff_date = pd.Timestamp("2024-03-31")
    cis_proc['Date_Obj'] = pd.to_datetime(cis_proc[col_map_cis['DATE']], dayfirst=True, errors='coerce')
    time_barred_mask = (cis_proc['Date_Obj'] < cutoff_date) & (cis_proc['Date_Obj'].notna())
    
    cis_proc.loc[time_barred_mask, 'Short Remark'] = cis_proc.loc[time_barred_mask, 'Short Remark'] + " + Time Barred"
    cis_proc.loc[time_barred_mask, 'Detailed Remark'] = cis_proc.loc[time_barred_mask, 'Detailed Remark'] + " [Warning: Date < 31 Mar 2024]"
    
    return cis_proc, g2b_proc

# ==========================================
# 4. STREAMLIT UI
# ==========================================
st.set_page_config(page_title="GST Reconciliation Tool", layout="wide")
st.title("📊 Auto-Reconciliation Tool (Detailed Remarks)")

col1, col2 = st.columns(2)
with col1: cis_file = st.file_uploader("Upload CIS Unmatched File", type=['xlsx'], key="cis")
with col2: g2b_file = st.file_uploader("Upload GSTR-2B File", type=['xlsx'], key="g2b")
tolerance = st.number_input("Financial Tolerance (₹)", min_value=0.0, value=10.0, step=0.1)

if st.button("🚀 Run Reconciliation", type="primary"):
    if cis_file and g2b_file:
        with st.spinner("Processing..."):
            try:
                # Load CIS
                df_cis = pd.read_excel(cis_file)
                
                # Load GSTR-2B (Smart Stitching)
                xl = pd.ExcelFile(g2b_file)
                sheet_name = 'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0]
                df_g2b = load_gstr2b_with_stitching(g2b_file, sheet_name)

                # Column Candidates
                cis_map_def = {
                    'GSTIN': ['SupplierGSTIN', 'GSTIN'],
                    'INVOICE': ['DocumentNumber', 'Invoice Number', 'Inv No'],
                    'DATE': ['DocumentDate', 'Invoice Date', 'Date'],
                    'TAXABLE': ['TaxableValue', 'Taxable Value'],
                    'IGST': ['IntegratedTaxAmount', 'Integrated Tax', 'IGST Amount'],
                    'CGST': ['CentralTaxAmount', 'Central Tax', 'CGST Amount'],
                    'SGST': ['StateUT TaxAmount', 'State/UT Tax', 'SGST Amount']
                }
                g2b_map_def = {
                    'GSTIN': ['GSTIN of supplier', 'Supplier GSTIN'],
                    'INVOICE': ['Invoice number', 'Invoice No'],
                    'DATE': ['Invoice Date', 'Date'],
                    'TAXABLE': ['Taxable Value (₹)', 'Taxable Value'],
                    'IGST': ['Integrated Tax(₹)', 'Integrated Tax'],
                    'CGST': ['Central Tax(₹)', 'Central Tax'],
                    'SGST': ['State/UT Tax(₹)', 'State/UT Tax']
                }

                # Mapping Logic
                final_map_cis = {}
                final_map_g2b = {}
                missing_cols = []

                for key, candidates in cis_map_def.items():
                    found = find_column(df_cis, candidates)
                    if found: final_map_cis[key] = found
                    else: missing_cols.append(f"CIS: {candidates[0]}")

                for key, candidates in g2b_map_def.items():
                    found = find_column(df_g2b, candidates)
                    if found: final_map_g2b[key] = found
                    else: missing_cols.append(f"GSTR-2B: {candidates[0]}")

                if missing_cols:
                    st.error("❌ Missing Columns!")
                    st.write(missing_cols)
                    st.stop()

                # Run Logic
                cis_res, g2b_res = run_reconciliation(df_cis, df_g2b, final_map_cis, final_map_g2b, tolerance)
                
                st.success(f"Done! Matched: {len(cis_res[cis_res['Matching Status'] == 'Matched'])}")
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                    g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                
                st.download_button("Download Result", output.getvalue(), "Reconciliation_Output.xlsx")

            except Exception as e:
                st.error(f"Error: {e}")
