import streamlit as st
import pandas as pd
import io
import re
import numpy as np

# ==========================================
# 1. ADVANCED NORMALIZATION FUNCTIONS
# ==========================================

def advanced_normalize_invoice(inv_num):
    """
    Top-tier normalization for invoice numbers.
    Handles: ' INV / 001 ', 'INV-001', '001', 'INV_001' -> ALL become 'INV1'
    """
    if pd.isna(inv_num) or str(inv_num).strip() == '':
        return ""
    
    # Convert to string and upper case
    s = str(inv_num).upper()
    
    # 1. Remove hidden characters (non-breaking spaces, tabs, newlines)
    s = "".join(s.split()) 
    
    # 2. Keep ONLY alphanumeric (A-Z, 0-9). Removes /, -, _, ., etc.
    s = re.sub(r'[^A-Z0-9]', '', s)
    
    # 3. Remove leading zeros, but keep the number 0 if it's just "0"
    s = s.lstrip('0')
    
    return s if s else "0"

def clean_currency(val):
    """Parses currency strings, handling commas and spaces."""
    if pd.isna(val) or str(val).strip() == '':
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        # Remove commas, spaces, and currency symbols
        clean_str = str(val).replace(',', '').replace(' ', '').replace('₹', '')
        return float(clean_str)
    except ValueError:
        return 0.0

def normalize_gstin(gstin):
    """Standardizes GSTIN by stripping whitespace and uppercasing."""
    if pd.isna(gstin):
        return ""
    return str(gstin).strip().upper().replace(" ", "")

# ==========================================
# 2. CORE LOGIC
# ==========================================

def run_reconciliation(cis_df, gstr2b_df, tolerance):
    # --- A. PREPROCESSING CIS DATA ---
    cis_proc = cis_df.copy()
    
    # Ensure tracking ID exists
    if 'Index CIS' not in cis_proc.columns:
        cis_proc['Index CIS'] = range(1, len(cis_proc) + 1) # [cite: 14]

    # Create Normalized Keys
    cis_proc['Norm_GSTIN'] = cis_proc['SupplierGSTIN'].apply(normalize_gstin)
    cis_proc['Norm_Invoice'] = cis_proc['DocumentNumber'].apply(advanced_normalize_invoice)
    
    # Clean Financials
    cis_fin_cols = ['TaxableValue', 'IntegratedTaxAmount', 'CentralTaxAmount', 'StateUT TaxAmount']
    for col in cis_fin_cols:
        cis_proc[col] = cis_proc[col].apply(clean_currency)
        
    # Calculate Total Tax
    cis_proc['Total_Tax'] = (cis_proc['IntegratedTaxAmount'] + 
                             cis_proc['CentralTaxAmount'] + 
                             cis_proc['StateUT TaxAmount'])

    # Initialize Output Columns
    cis_proc['Matching Status'] = "Unmatched"
    cis_proc['Short Remark'] = "Not Found"
    cis_proc['Detailed Remark'] = ""
    cis_proc['GSTR 2B Key'] = "" # [cite: 13]

    # --- B. PREPROCESSING GSTR-2B DATA ---
    g2b_proc = gstr2b_df.copy()
    
    # Ensure INDEX exists
    if 'INDEX' not in g2b_proc.columns:
        g2b_proc['INDEX'] = g2b_proc.index + 100000 

    # Create Normalized Keys
    g2b_proc['Norm_GSTIN'] = g2b_proc['GSTIN of supplier'].apply(normalize_gstin)
    g2b_proc['Norm_Invoice'] = g2b_proc['Invoice number'].apply(advanced_normalize_invoice)
    
    # Map & Clean Financials
    g2b_proc['Taxable_Val_Clean'] = g2b_proc['Taxable Value (₹)'].apply(clean_currency)
    g2b_proc['Total_Tax_Clean'] = (
        g2b_proc['Integrated Tax(₹)'].apply(clean_currency) + 
        g2b_proc['Central Tax(₹)'].apply(clean_currency) + 
        g2b_proc['State/UT Tax(₹)'].apply(clean_currency)
    )

    # Output Columns for GSTR-2B
    g2b_proc['CIS Key'] = ""
    g2b_proc['Matching Status'] = "Unmatched"

    # --- C. CLUBBING & AGGREGATION ---
    # We group CIS data. This handles BOTH clubbed and non-clubbed rows automatically.
    # If there is no clubbing, the group will just contain 1 row.
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_Invoice']).agg({
        'TaxableValue': 'sum',
        'Total_Tax': 'sum',
        'DocumentDate': 'first', # Take first date for comparison
        'Index CIS': list  # Keep list of ALL original IDs 
    }).reset_index()

    # --- D. MATCHING ENGINE ---
    matched_g2b_indices = set()

    for idx, row_cis_group in cis_grouped.iterrows():
        gstin = row_cis_group['Norm_GSTIN']
        inv_num = row_cis_group['Norm_Invoice']
        
        if not gstin or not inv_num:
            continue

        # Filter GSTR-2B for candidates (Same GSTIN + Invoice)
        candidates = g2b_proc[
            (g2b_proc['Norm_GSTIN'] == gstin) & 
            (g2b_proc['Norm_Invoice'] == inv_num) &
            (~g2b_proc['INDEX'].isin(matched_g2b_indices)) # Don't reuse matched records
        ]
        
        match_found = False
        short_rem = "Unmatched"
        detail_rem = []
        matched_g2b_idx = None
        
        if not candidates.empty:
            # Check Financials
            for i, row_g2b in candidates.iterrows():
                diff_taxable = abs(row_cis_group['TaxableValue'] - row_g2b['Taxable_Val_Clean'])
                diff_tax = abs(row_cis_group['Total_Tax'] - row_g2b['Total_Tax_Clean'])
                
                # Check Date
                cis_date = pd.to_datetime(row_cis_group['DocumentDate'], dayfirst=True, errors='coerce')
                g2b_date = pd.to_datetime(row_g2b['Invoice Date'], dayfirst=True, errors='coerce')
                
                date_mismatch_str = ""
                if pd.notna(cis_date) and pd.notna(g2b_date) and cis_date != g2b_date:
                     date_mismatch_str = f" | Date Diff: {cis_date.strftime('%d-%m')} vs {g2b_date.strftime('%d-%m')}"

                if diff_taxable <= tolerance and diff_tax <= tolerance:
                    match_found = True
                    matched_g2b_idx = row_g2b['INDEX']
                    short_rem = "Matched"
                    if date_mismatch_str:
                        detail_rem.append(f"Matched with Date Warning{date_mismatch_str}")
                    break
                else:
                    # Keep track of why it failed (closest candidate)
                    detail_rem.append(f"Value Diff: Taxable {diff_taxable:.2f}, Tax {diff_tax:.2f}{date_mismatch_str}")
            
            if not match_found:
                 short_rem = "Value Mismatch"
        else:
            short_rem = "Invoice Not Found"
            detail_rem.append("No invoice number match in GSTR-2B")

        # --- E. UPDATE RECORDS ---
        original_cis_indices = row_cis_group['Index CIS']
        
        if match_found:
            # Update GSTR-2B [cite: 28]
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'CIS Key'] = ", ".join(map(str, original_cis_indices))
            g2b_proc.loc[g2b_proc['INDEX'] == matched_g2b_idx, 'Matching Status'] = "Matched"
            matched_g2b_indices.add(matched_g2b_idx)
            
            # Update Original CIS Lines [cite: 26, 27]
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = matched_g2b_idx
                if detail_rem:
                     cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = "; ".join(detail_rem)
                
        else:
            # Update Unmatched CIS Lines [cite: 30, 31, 32]
            for cis_id in original_cis_indices:
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Unmatched"
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = short_rem
                
                # Append to existing remarks if any
                existing = cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks']
                base_remark = str(existing.values[0]) if pd.notna(existing.values[0]) else ""
                
                # Combine detailed remarks
                final_detail = "; ".join(detail_rem)
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = final_detail
                
                # Also update the original 'Comments&Remarks' column as requested by user doc
                new_full_remark = f"{base_remark} | {short_rem}: {final_detail}".strip(" |")
                cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = new_full_remark

    # --- F. SECTION 16(4) CHECK ---
    cutoff_date = pd.Timestamp("2024-03-31")
    cis_proc['Date_Obj'] = pd.to_datetime(cis_proc['DocumentDate'], dayfirst=True, errors='coerce')
    time_barred_mask = (cis_proc['Date_Obj'] < cutoff_date) & (cis_proc['Date_Obj'].notna())
    
    # Mark Time Barred [cite: 34]
    cis_proc.loc[time_barred_mask, 'Short Remark'] = cis_proc.loc[time_barred_mask, 'Short Remark'] + " + Time Barred"
    cis_proc.loc[time_barred_mask, 'Detailed Remark'] = cis_proc.loc[time_barred_mask, 'Detailed Remark'] + "; Inv Date before 31 Mar 2024"

    # Cleanup cols
    cis_final = cis_proc.drop(columns=['Norm_GSTIN', 'Norm_Invoice', 'Total_Tax', 'Date_Obj'])
    g2b_final = g2b_proc.drop(columns=['Norm_GSTIN', 'Norm_Invoice', 'Taxable_Val_Clean', 'Total_Tax_Clean'])

    return cis_final, g2b_final

# ==========================================
# 3. STREAMLIT UI
# ==========================================

st.set_page_config(page_title="GST Reconciliation Pro", layout="wide")

st.title("📊 Auto-Reconciliation Tool: CIS vs GSTR-2B")

col1, col2 = st.columns(2)
with col1:
    st.subheader("1. Upload CIS Unmatched File")
    cis_file = st.file_uploader("Upload Excel (.xlsx)", type=['xlsx'], key="cis")
with col2:
    st.subheader("2. Upload GSTR-2B File")
    g2b_file = st.file_uploader("Upload Excel (.xlsx)", type=['xlsx'], key="g2b")

# Configuration [cite: 8]
st.subheader("3. Configuration")
tolerance = st.number_input("Financial Tolerance (₹)", min_value=0.0, value=10.0, step=0.1, help="Allow matches even if tax amounts differ by this amount.")

if st.button("🚀 Run Reconciliation", type="primary"):
    if cis_file and g2b_file:
        with st.spinner("Processing... Finding 'B2B' sheet... Normalizing... Matching..."):
            try:
                # Load CIS
                df_cis = pd.read_excel(cis_file)
                
                # Load GSTR-2B (Smart Sheet Detection) [cite: 10, 11]
                xl = pd.ExcelFile(g2b_file)
                if 'B2B' in xl.sheet_names:
                    df_g2b = pd.read_excel(g2b_file, sheet_name='B2B')
                    st.info("Loaded 'B2B' sheet from GSTR-2B file.")
                else:
                    df_g2b = pd.read_excel(g2b_file, sheet_name=0)
                    st.warning(f"'B2B' sheet not found. Loaded first sheet: '{xl.sheet_names[0]}'. Check file format.")

                # Run Logic
                cis_result, g2b_result = run_reconciliation(df_cis, df_g2b, tolerance)
                
                st.success("Reconciliation Complete!")
                
                # Metrics [cite: 6]
                total = len(cis_result)
                matched = len(cis_result[cis_result['Matching Status'] == 'Matched'])
                st.metric("Reconciliation Status", f"{matched} / {total} Records Matched", f"{round(matched/total*100, 1)}%")

                # Download
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    cis_result.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                    g2b_result.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                
                st.download_button(
                    label="📥 Download Final Report",
                    data=output.getvalue(),
                    file_name="Final_Reconciliation_Output.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"Error: {e}")
    else:
        st.warning("Please upload both files.")
