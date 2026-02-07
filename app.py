import streamlit as st
import pandas as pd
import io
import re
import numpy as np
import difflib # For Layer 7 Fuzzy Matching

# ==========================================
# 1. ROBUST DATA LOADER (Header Stitching)
# ==========================================
def load_gstr2b_with_stitching(file_obj, sheet_name):
    try:
        df_raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=8)
    except:
        xl = pd.ExcelFile(file_obj)
        df_raw = pd.read_excel(file_obj, sheet_name=xl.sheet_names[0], header=None, nrows=8)
    
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

    file_obj.seek(0)
    try:
        df_final = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_end_row + 1)
    except:
        df_final = pd.read_excel(file_obj, sheet_name=0, header=header_end_row + 1)
    
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

def get_pan_from_gstin(gstin):
    """Extracts first 10 chars (PAN) from GSTIN"""
    norm = normalize_gstin(gstin)
    if len(norm) >= 10:
        return norm[:10]
    return norm

# --- NORMALIZERS ---
def normalize_inv_basic(inv):
    if pd.isna(inv): return ""
    s = str(inv).upper()
    s = "".join(s.split())
    s = re.sub(r'[^A-Z0-9]', '', s)
    s = s.lstrip('0')
    return s

def normalize_inv_numeric(inv):
    if pd.isna(inv): return ""
    s = str(inv)
    s = re.sub(r'[^0-9]', '', s)
    s = s.lstrip('0')
    return s

def get_last_4(inv):
    if pd.isna(inv): return ""
    s = str(inv)
    s = re.sub(r'[^0-9]', '', s)
    if len(s) > 4: return s[-4:]
    return s.lstrip('0')

def get_similarity_ratio(str1, str2):
    """Calculates Levenshtein similarity ratio (0.0 to 1.0)"""
    return difflib.SequenceMatcher(None, str(str1), str(str2)).ratio()

# ==========================================
# 3. CORE LOGIC (8 LAYERS)
# ==========================================
def run_8_layer_reconciliation(cis_df, gstr2b_df, col_map_cis, col_map_g2b, tol_std, tol_high):
    
    # --- A. PREPROCESSING ---
    cis_proc = cis_df.copy()
    g2b_proc = gstr2b_df.copy()

    # IDs
    if 'Index CIS' not in cis_proc.columns: cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)
    if 'INDEX' not in g2b_proc.columns: g2b_proc['INDEX'] = g2b_proc.index + 100000 

    # Key Normalization
    cis_proc['Norm_GSTIN'] = cis_proc[col_map_cis['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_PAN'] = cis_proc[col_map_cis['GSTIN']].apply(get_pan_from_gstin)
    
    g2b_proc['Norm_GSTIN'] = g2b_proc[col_map_g2b['GSTIN']].apply(normalize_gstin)
    g2b_proc['Norm_PAN'] = g2b_proc[col_map_g2b['GSTIN']].apply(get_pan_from_gstin)

    # Invoice Normalization
    cis_proc['Inv_Basic'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_basic)
    cis_proc['Inv_Num'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_numeric)
    cis_proc['Inv_Last4'] = cis_proc[col_map_cis['INVOICE']].apply(get_last_4)

    g2b_proc['Inv_Basic'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_basic)
    g2b_proc['Inv_Num'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_numeric)
    g2b_proc['Inv_Last4'] = g2b_proc[col_map_g2b['INVOICE']].apply(get_last_4)

    # Financials
    cis_proc['Taxable'] = cis_proc[col_map_cis['TAXABLE']].apply(clean_currency)
    cis_proc['Tax'] = (cis_proc[col_map_cis['IGST']].apply(clean_currency) + 
                       cis_proc[col_map_cis['CGST']].apply(clean_currency) + 
                       cis_proc[col_map_cis['SGST']].apply(clean_currency))
    cis_proc['Grand_Total'] = cis_proc['Taxable'] + cis_proc['Tax']

    g2b_proc['Taxable'] = g2b_proc[col_map_g2b['TAXABLE']].apply(clean_currency)
    g2b_proc['Tax'] = (g2b_proc[col_map_g2b['IGST']].apply(clean_currency) + 
                       g2b_proc[col_map_g2b['CGST']].apply(clean_currency) + 
                       g2b_proc[col_map_g2b['SGST']].apply(clean_currency))
    g2b_proc['Grand_Total'] = g2b_proc['Taxable'] + g2b_proc['Tax']

    # Initialize Output Columns
    cis_proc['Matching Status'] = "Unmatched"
    cis_proc['Match Category'] = ""
    cis_proc['Detailed Remark'] = ""
    cis_proc['GSTR 2B Key'] = ""
    
    g2b_proc['Matching Status'] = "Unmatched"
    g2b_proc['CIS Key'] = ""

    # --- B. INITIAL GROUPING (CIS) ---
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic']).agg({
        'Taxable': 'sum', 'Tax': 'sum', 'Grand_Total': 'sum',
        'Inv_Num': 'first', 'Inv_Last4': 'first',
        col_map_cis['INVOICE']: 'first', col_map_cis['DATE']: 'first',
        'Index CIS': list
    }).reset_index()
    cis_grouped['Matched_Flag'] = False

    match_stats = {}

    # Helper to commit matches
    def commit_match(layer_name, row_cis, row_g2b, diff_grand, detail_str, is_reverse_club=False, g2b_indices_list=None):
        # Update CIS Group
        if not is_reverse_club:
            # Standard path
            idx = row_cis.name
            cis_grouped.at[idx, 'Matched_Flag'] = True
            cis_indices = row_cis['Index CIS']
            g2b_indices = [row_g2b['INDEX']]
        else:
            # Reverse clubbing path: row_cis is a Grouped Row, row_g2b is a Grouped Row
            cis_indices = row_cis['Index CIS']
            g2b_indices = g2b_indices_list
            # We don't update cis_grouped here because reverse clubbing loop handles it differently

        # Update GSTR-2B
        for g_idx in g2b_indices:
            g2b_proc.loc[g2b_proc['INDEX'] == g_idx, 'Matching Status'] = "Matched"
            g2b_proc.loc[g2b_proc['INDEX'] == g_idx, 'CIS Key'] = ", ".join(map(str, cis_indices))

        # Update CIS Lines
        for cis_id in cis_indices:
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Match Category'] = layer_name
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = ", ".join(map(str, g2b_indices))
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = "Matched"
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = detail_str
            
            existing = str(cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'].values[0])
            if existing == 'nan': existing = ""
            new_rem = f"{existing} | {layer_name}".strip(" |")
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = new_rem

    # --- LAYERS 1-5 (STANDARD) ---
    def run_standard_layer(layer_name, join_col_cis, join_col_g2b, tolerance, strict_tax_split=False, use_pan=False):
        count = 0
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            pan = row_cis['Norm_PAN']
            inv_val = row_cis[join_col_cis]
            
            if not inv_val or len(str(inv_val)) < 2: continue

            # Filter G2B
            mask = (g2b_proc['Matching Status'] == "Unmatched") & (g2b_proc[join_col_g2b] == inv_val)
            if use_pan:
                mask = mask & (g2b_proc['Norm_PAN'] == pan) # Relaxed: Same PAN, diff GSTIN
            else:
                mask = mask & (g2b_proc['Norm_GSTIN'] == gstin) # Strict: Same GSTIN
            
            candidates = g2b_proc[mask]

            if candidates.empty: continue

            for g2b_idx, row_g2b in candidates.iterrows():
                diff_grand = abs(row_cis['Grand_Total'] - row_g2b['Grand_Total'])
                is_match = False
                
                if strict_tax_split:
                    diff_taxable = abs(row_cis['Taxable'] - row_g2b['Taxable'])
                    diff_tax = abs(row_cis['Tax'] - row_g2b['Tax'])
                    if diff_taxable <= tolerance and diff_tax <= tolerance: is_match = True
                else:
                    if diff_grand <= tolerance: is_match = True

                if is_match:
                    detail = f"{layer_name} (Diff: {diff_grand:.2f})"
                    if use_pan and row_cis['Norm_GSTIN'] != row_g2b['Norm_GSTIN']:
                        detail += f" [GSTIN Mismatch: Found under {row_g2b['Norm_GSTIN']}]"
                        
                    commit_match(layer_name, row_cis, row_g2b, diff_grand, detail)
                    count += 1
                    break 
        match_stats[layer_name] = count

    # RUN 1-5
    run_standard_layer("Layer 1: Strict", "Inv_Basic", "Inv_Basic", tol_std, strict_tax_split=True)
    run_standard_layer("Layer 2: Grand Total", "Inv_Basic", "Inv_Basic", tol_std)
    run_standard_layer("Layer 3: High Tolerance", "Inv_Basic", "Inv_Basic", tol_high)
    run_standard_layer("Layer 4: Numeric Only", "Inv_Num", "Inv_Num", tol_std)
    run_standard_layer("Layer 5: Last 4 Digits", "Inv_Last4", "Inv_Last4", tol_std)
    
    # --- LAYER 6: PAN LEVEL ---
    # Matches Invoice + Amount, but ignores GSTIN Suffix (Uses PAN only)
    run_standard_layer("Layer 6: PAN Level", "Inv_Basic", "Inv_Basic", tol_std, use_pan=True)

    # --- LAYER 7: FUZZY (LEVENSHTEIN) ---
    def run_fuzzy_layer():
        count = 0
        layer_name = "Layer 7: Fuzzy"
        
        # Iterate Unmatched CIS
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            cis_inv = str(row_cis['Inv_Basic'])
            if len(cis_inv) < 3: continue

            # Filter Unmatched G2B by GSTIN (Optimization)
            g2b_candidates = g2b_proc[(g2b_proc['Matching Status'] == "Unmatched") & (g2b_proc['Norm_GSTIN'] == gstin)]
            
            if g2b_candidates.empty: continue
            
            best_match = None
            best_score = 0.0

            for g_idx, row_g2b in g2b_candidates.iterrows():
                # 1. Check Amount First (Optimization) - Must match strictly
                diff_grand = abs(row_cis['Grand_Total'] - row_g2b['Grand_Total'])
                if diff_grand > tol_std: continue
                
                # 2. Check String Similarity
                g2b_inv = str(row_g2b['Inv_Basic'])
                score = get_similarity_ratio(cis_inv, g2b_inv)
                
                if score > 0.85 and score > best_score:
                    best_score = score
                    best_match = row_g2b

            if best_match is not None:
                diff_grand = abs(row_cis['Grand_Total'] - best_match['Grand_Total'])
                detail = f"Fuzzy Match: '{cis_inv}' vs '{best_match['Inv_Basic']}' (Score: {int(best_score*100)}%)"
                commit_match(layer_name, row_cis, best_match, diff_grand, detail)
                count += 1
        
        match_stats[layer_name] = count

    run_fuzzy_layer()

    # --- LAYER 8: REVERSE CLUBBING (Many G2B -> One CIS) ---
    def run_reverse_clubbing():
        count = 0
        layer_name = "Layer 8: Reverse Clubbing"
        
        # 1. Get Unmatched G2B
        g2b_unmatched = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"]
        
        # 2. Group G2B by (GSTIN, Invoice)
        # This aggregates multiple G2B lines into one virtual invoice
        g2b_grouped = g2b_unmatched.groupby(['Norm_GSTIN', 'Inv_Basic']).agg({
            'Grand_Total': 'sum',
            'INDEX': list, # Collect IDs
            'Inv_Basic': 'first'
        }).reset_index()
        
        # 3. Compare CIS Group vs G2B Group
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            inv = row_cis['Inv_Basic']
            
            # Find matching G2B Group
            # Note: We assume basic invoice number matches. 
            # If inv numbers differ, it's payment matching (too complex for this tool).
            match_row = g2b_grouped[
                (g2b_grouped['Norm_GSTIN'] == gstin) & 
                (g2b_grouped['Inv_Basic'] == inv)
            ]
            
            if match_row.empty: continue
            
            row_g2b_group = match_row.iloc[0]
            
            # Compare Totals
            diff_grand = abs(row_cis['Grand_Total'] - row_g2b_group['Grand_Total'])
            
            if diff_grand <= tol_std:
                # REVERSE MATCH FOUND
                g2b_indices = row_g2b_group['INDEX'] # List of G2B IDs
                detail = f"Reverse Clubbing: 1 CIS vs {len(g2b_indices)} G2B Records"
                
                # Manually commit since structure differs
                cis_grouped.at[idx, 'Matched_Flag'] = True
                commit_match(layer_name, row_cis, None, diff_grand, detail, is_reverse_club=True, g2b_indices_list=g2b_indices)
                count += 1
                
        match_stats[layer_name] = count

    run_reverse_clubbing()

    # --- CLEANUP ---
    cutoff = pd.Timestamp("2024-03-31")
    cis_proc['D_Obj'] = pd.to_datetime(cis_proc[col_map_cis['DATE']], dayfirst=True, errors='coerce')
    mask = (cis_proc['D_Obj'] < cutoff) & (cis_proc['D_Obj'].notna())
    
    cis_proc.loc[mask, 'Short Remark'] = cis_proc.loc[mask, 'Short Remark'].astype(str) + " + Time Barred"
    cis_proc.loc[mask, 'Detailed Remark'] = cis_proc.loc[mask, 'Detailed Remark'].astype(str) + " [Warning: Date < 31 Mar 2024]"

    drop_cols = ['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic', 'Inv_Num', 'Inv_Last4', 'Taxable', 'Tax', 'Grand_Total', 'D_Obj']
    cis_final = cis_proc.drop(columns=[c for c in drop_cols if c in cis_proc.columns])
    g2b_final = g2b_proc.drop(columns=[c for c in drop_cols if c in g2b_proc.columns])

    return cis_final, g2b_final, match_stats

# ==========================================
# 4. STREAMLIT UI
# ==========================================
st.set_page_config(page_title="GST 8-Layer Reconciliation", layout="wide")
st.title("📊 8-Layer Auto-Reconciliation Tool")

st.markdown("""
**New Advanced Layers:**
* **Layer 6 (PAN):** Matches if vendor filed under Head Office GSTIN.
* **Layer 7 (Fuzzy):** Matches typos (e.g. `9855` vs `9885`) using Levenshtein distance.
* **Layer 8 (Reverse Clubbing):** Matches **1 CIS Entry** against **Multiple GSTR-2B Entries**.
""")

col1, col2 = st.columns(2)
with col1: cis_file = st.file_uploader("1. Upload CIS File (.xlsx)", type=['xlsx'], key="cis")
with col2: g2b_file = st.file_uploader("2. Upload GSTR-2B File (.xlsx)", type=['xlsx'], key="g2b")

c1, c2 = st.columns(2)
tol_std = c1.number_input("Standard Tolerance (₹)", value=2.0)
tol_high = c2.number_input("High Tolerance (Layer 3) (₹)", value=50.0)

if st.button("🚀 Run 8-Layer Algorithm", type="primary"):
    if cis_file and g2b_file:
        with st.spinner("Running Advanced Matching Algorithms..."):
            try:
                # Load
                df_cis = pd.read_excel(cis_file)
                xl = pd.ExcelFile(g2b_file)
                df_g2b = load_gstr2b_with_stitching(g2b_file, 'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0])

                # Map
                cis_map = {'GSTIN': ['SupplierGSTIN','GSTIN'], 'INVOICE': ['DocumentNumber','Invoice Number'], 'DATE': ['DocumentDate','Invoice Date'], 'TAXABLE': ['TaxableValue','Taxable Value'], 'IGST': ['IntegratedTaxAmount','Integrated Tax'], 'CGST': ['CentralTaxAmount','Central Tax'], 'SGST': ['StateUT TaxAmount','State/UT Tax']}
                g2b_map = {'GSTIN': ['GSTIN of supplier','Supplier GSTIN'], 'INVOICE': ['Invoice number','Invoice No'], 'DATE': ['Invoice Date','Date'], 'TAXABLE': ['Taxable Value (₹)','Taxable Value'], 'IGST': ['Integrated Tax(₹)','Integrated Tax'], 'CGST': ['Central Tax(₹)','Central Tax'], 'SGST': ['State/UT Tax(₹)','State/UT Tax']}
                
                final_cis_map = {}
                final_g2b_map = {}
                for k, v in cis_map.items(): 
                    found = find_column(df_cis, v)
                    if found: final_cis_map[k] = found
                    else: st.error(f"Missing CIS: {v[0]}"); st.stop()
                for k, v in g2b_map.items(): 
                    found = find_column(df_g2b, v)
                    if found: final_g2b_map[k] = found
                    else: st.error(f"Missing GSTR-2B: {v[0]}"); st.stop()

                # Run
                cis_res, g2b_res, stats = run_8_layer_reconciliation(df_cis, df_g2b, final_cis_map, final_g2b_map, tol_std, tol_high)
                
                st.success("✅ Reconciliation Complete!")
                st.table(pd.DataFrame(list(stats.items()), columns=['Layer', 'Matches']))
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                    g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                st.download_button("Download Result", output.getvalue(), "Reconciliation_Output.xlsx")

            except Exception as e: st.error(f"Error: {e}")
