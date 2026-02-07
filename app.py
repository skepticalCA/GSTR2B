import streamlit as st
import pandas as pd
import io
import re
import numpy as np

# ==========================================
# 1. CONFIGURATION: EXACT COLUMN MAPPING
# ==========================================
# These are the EXACT headers you provided. 
# We map them to internal names for processing.

CIS_MAPPING = {
    'GSTIN': 'SupplierGSTIN',          # User provided: SupplierGSTIN
    'INVOICE_NUM': 'DocumentNumber',   # User provided: DocumentNumber
    'DATE': 'DocumentDate',            # User provided: DocumentDate
    'TAXABLE': 'TaxableValue',         # User provided: TaxableValue
    'IGST': 'IntegratedTaxAmount',     # User provided: IntegratedTaxAmount
    'CGST': 'CentralTaxAmount',        # User provided: CentralTaxAmount
    'SGST': 'StateUT TaxAmount'        # User provided: StateUT TaxAmount
}

GSTR2B_MAPPING = {
    'GSTIN': 'GSTIN of supplier',      # User provided: GSTIN of supplier
    'INVOICE_NUM': 'Invoice number',   # User provided: Invoice number
    'DATE': 'Invoice Date',            # User provided: Invoice Date
    'TAXABLE': 'Taxable Value (₹)',    # User provided: Taxable Value (₹)
    'IGST': 'Integrated Tax(₹)',       # User provided: Integrated Tax(₹)
    'CGST': 'Central Tax(₹)',          # User provided: Central Tax(₹)
    'SGST': 'State/UT Tax(₹)'          # User provided: State/UT Tax(₹)
}

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================

def advanced_normalize_invoice(inv_num):
    """Normalizes invoice numbers (removes special chars, leading zeros)."""
    if pd.isna(inv_num) or str(inv_num).strip() == '':
        return ""
    s = str(inv_num).upper()
    s = "".join(s.split()) # Remove whitespace
    s = re.sub(r'[^A-Z0-9]', '', s) # Keep only Alphanumeric
    s = s.lstrip('0') # Remove leading zeros
    return s if s else "0"

def clean_currency(val):
    """Parses currency, handling commas and symbols."""
    if pd.isna(val) or str(val).strip() == '':
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
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

def run_reconciliation(cis_df, gstr2b_df, tolerance):
    # --- A. PREPROCESSING CIS DATA ---
    cis_proc = cis_df.copy()
    
    # Verify Columns exist
    for key, col_name in CIS_MAPPING.items():
        if col_name not in cis_proc.columns:
            st.error(f"❌ CIS File Error: Missing column '{col_name}'. Please check your file headers.")
            st.stop()

    if 'Index CIS' not in cis_proc.columns:
        cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)

    # Normalize Keys
    cis_proc['Norm_GSTIN'] = cis_proc[CIS_MAPPING['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_Invoice'] = cis_proc[CIS_MAPPING['INVOICE_NUM']].apply(advanced_normalize_invoice)
    
    # Clean Financials
    cis_proc['Taxable_Clean'] = cis_proc[CIS_MAPPING['TAXABLE']].apply(clean_currency)
    cis_proc['IGST_Clean'] = cis_proc[CIS_MAPPING['IGST']].apply(clean_currency)
    cis_proc['CGST_Clean'] = cis_proc[CIS_MAPPING['CGST']].apply(clean_currency)
    cis_proc['SGST_Clean'] = cis_proc[CIS_MAPPING['SGST']].apply(clean_currency)
    
    cis_proc['Total_Tax'] = cis_proc['IGST_Clean'] + cis_proc['CGST_Clean'] + cis_proc['SGST_Clean']

    # Initialize Output Columns
    cis_proc['Matching Status'] = "Unmatched"
    cis_proc['Short Remark'] = "Not Found"
    cis_proc['Detailed Remark'] = ""
    cis_proc['GSTR 2B Key'] = ""

    # --- B. PREPROCESSING GSTR-2B DATA ---
    g2b_proc = gstr2b_df.copy()
    
    # Verify Columns exist
    for key, col_name in GSTR2B_MAPPING.items():
        if col_name not in g2b_proc.columns:
            st.error(f"❌ GSTR-2B File Error: Missing column '{col_name}'. Please check your file headers.")
            st.stop()

    if 'INDEX' not in g2b_proc.columns:
        g2b_proc['INDEX'] = g2b_proc.index + 100000 

    # Normalize Keys
    g2b_proc['Norm_GSTIN'] = g2b_proc[GSTR2B_MAPPING['GSTIN']].apply(normalize_gstin)
    g2b_proc['Norm_Invoice'] = g2b_proc[GSTR2B_MAPPING['INVOICE_NUM']].apply(advanced_normalize_invoice)
    
    # Clean Financials
    g2b_proc['Taxable_Clean'] = g2b_proc[GSTR2B_MAPPING['TAXABLE']].apply(clean_currency)
    g2b_proc['IGST_Clean'] = g2b_proc[GSTR2B_MAPPING['IGST']].apply(clean_currency)
    g2b_proc['CGST_Clean'] = g2b_proc[GSTR2B_MAPPING['CGST']].apply(clean_currency)
    g2b_proc['SGST_Clean'] = g2b_proc[GSTR2B_MAPPING['SGST']].apply(clean_currency)
    
    g2b_proc['Total_Tax_Clean'] = g2b_proc['IGST_Clean'] + g2b_proc['CGST_Clean'] + g2b_proc['SGST_Clean']

    g2b_proc['CIS Key'] = ""
    g2b_proc['Matching Status'] = "Unmatched"

    # --- C. CLUBBING & AGGREGATION ---
    # Group CIS data
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_Invoice']).agg({
        'Taxable_Clean': 'sum',
        'Total_Tax': 'sum',
        CIS_MAPPING['DATE']: 'first',
        'Index CIS': list
    }).reset_index()

    # --- D. MATCHING ENGINE ---
    matched_g2b_indices = set()

    for idx, row_cis_group in cis_grouped.iterrows():
        gstin = row_cis_group['Norm_GSTIN']
        inv_num = row_cis_group['Norm_Invoice']
        
        if not gstin or not inv_num:
            continue

        # Filter GSTR-2B
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
                diff_taxable = abs(row_cis_group['Taxable_Clean'] - row_g2b['Taxable_Clean'])
                diff_tax = abs(row_cis_group['Total_Tax'] - row_g2b['Total_Tax_Clean'])
                
                # Check Date
                cis_date = pd.to_datetime(row_cis_group[CIS_MAPPING['DATE']], dayfirst=True, errors='coerce')
                g2b_date = pd.to_datetime(row_g2b[GSTR2B_MAPPING['DATE']], dayfirst=True, errors='coerce')
                
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
                    detail_rem.append(f"Value Diff: Taxable {diff_taxable:.2f}, Tax {diff_tax:.2f}{date_str}")
            
            if not match_found:
                 short_rem = "Value Mismatch"
        else:
            short_rem = "Invoice Not Found"
            detail_rem.append("No invoice number match in GSTR-2B")

        # --- E. UPDATE RECORDS ---
        original_cis_indices = row_cis_group['Index CIS']
        
        if match_found:
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'CIS Key'] = ", ".join(map(str, original_cis_indices))
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'Matching Status'] = "Matched"
            matched_g2b_indices.add(matched_g2b_idx)
            
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = matched_g2b_idx
                if detail_rem:
                     cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = "; ".join(detail_rem)
        else:
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Unmatched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                existing = cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks']
                base_rem = str(existing.values[0]) if pd.notna(existing.values[0]) else ""
                final_detail = "; ".join(detail_rem)
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = final_detail
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = f"{base_rem} | {short_rem}: {final_detail}".strip(" |")

    # --- F. TIME BARRED CHECK (Sec 16(4)) ---
    cutoff_date = pd.Timestamp("2024-03-31")
    cis_proc['Date_Obj'] = pd.to_datetime(cis_proc[CIS_MAPPING['DATE']], dayfirst=True, errors='coerce')
    time_barred_mask = (cis_proc['Date_Obj'] < cutoff_date) & (cis_proc['Date_Obj'].notna())
    
    cis_proc.loc[time_barred_mask, 'Short Remark'] = cis_proc.loc[time_barred_mask, 'Short Remark'] + " + Time Barred"
    cis_proc.loc[time_barred_mask, 'Detailed Remark'] = cis_proc.loc[time_barred_mask, 'Detailed Remark'] + "; Inv Date before 31 Mar 2024"

    # Cleanup Output (Keep useful cols + results)
    # We return the modified dataframe but drop temp calculation columns
    cols_to_drop = ['Norm_GSTIN', 'Norm_Invoice', 'Taxable_Clean', 'IGST_Clean', 'CGST_Clean', 'SGST_Clean', 'Total_Tax', 'Date_Obj']
    cis_final = cis_proc.drop(columns=[c for c in cols_to_drop if c in cis_proc.columns])
    
    g2b_drop = ['Norm_GSTIN', 'Norm_Invoice', 'Taxable_Clean', 'IGST_Clean', 'CGST_Clean', 'SGST_Clean', 'Total_Tax_Clean']
    g2b_final = g2b_proc.drop(columns=[c for c in g2b_drop if c in g2b_proc.columns])

    return cis_final, g2b_final

# ==========================================
# 4. STREAMLIT UI
# ==========================================

st.set_page_config(page_title="GST Reconciliation Tool", layout="wide")
st.title("📊 Auto-Reconciliation Tool")

col1, col2 = st.columns(2)
with col1:
    cis_file = st.file_uploader("Upload CIS Unmatched File", type=['xlsx'], key="cis")
with col2:
    g2b_file = st.file_uploader("Upload GSTR-2B File", type=['xlsx'], key="g2b")

tolerance = st.number_input("Financial Tolerance (₹)", min_value=0.0, value=10.0, step=0.1)

if st.button("🚀 Run Reconciliation", type="primary"):
    if cis_file and g2b_file:
        with st.spinner("Processing..."):
            try:
                # Load CIS
                df_cis = pd.read_excel(cis_file)
                df_cis.columns = df_cis.columns.str.strip() # Minimal clean

                # Load GSTR-2B
                xl = pd.ExcelFile(g2b_file)
                sheet_name = 'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0]
                
                # Dynamic Header Search for GSTR-2B
                # Even with exact names, the header might be on Row 2 or 3
                target_col = GSTR2B_MAPPING['GSTIN'] # 'GSTIN of supplier'
                
                df_g2b = None
                found = False
                
                for i in range(5): # Check first 5 rows
                    temp_df = pd.read_excel(g2b_file, sheet_name=sheet_name, header=i)
                    temp_df.columns = temp_df.columns.str.strip()
                    if target_col in temp_df.columns:
                        df_g2b = temp_df
                        found = True
                        break
                
                if not found:
                    st.error(f"Could not find column '{target_col}' in GSTR-2B file (checked first 5 rows).")
                    st.stop()

                cis_res, g2b_res = run_reconciliation(df_cis, df_g2b, tolerance)
                
                st.success(f"Matched: {len(cis_res[cis_res['Matching Status'] == 'Matched'])}")
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                    g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                
                st.download_button("Download Result", output.getvalue(), "Reconciliation_Output.xlsx")

            except Exception as e:
                st.error(f"Error: {e}")
    else:
        st.warning("Please upload both files.")
