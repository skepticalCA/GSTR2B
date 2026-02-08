import streamlit as st
import pandas as pd
import io
import re
import numpy as np
import time

# Try importing rapidfuzz for speed, fallback to difflib if missing
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    import difflib
    USE_RAPIDFUZZ = False

# ==========================================
# 1. ROBUST DATA LOADER
# ==========================================
def load_gstr2b_with_stitching(file_obj, sheet_name):
    try:
        df_raw = pd.read_excel(file_obj, sheet_name=sheet_name, header=None, nrows=8)
    except:
        xl = pd.ExcelFile(file_obj)
        df_raw = pd.read_excel(file_obj, sheet_name=xl.sheet_names[0], header=None, nrows=8)
    
    idx_gstin = -1; idx_inv = -1
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
        
        if val_inv and not val_inv.startswith("Unnamed"): final_headers.append(val_inv)
        elif val_gstin and not val_gstin.startswith("Unnamed"): final_headers.append(val_gstin)
        else: final_headers.append(f"Column_{c}")

    file_obj.seek(0)
    try:
        df_final = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_end_row + 1)
    except:
        df_final = pd.read_excel(file_obj, sheet_name=0, header=header_end_row + 1)
    
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    return df_final

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def find_column(df, candidates):
    existing_cols = {str(c).strip().lower().replace(' ', '').replace('\n', '').replace('_', '').replace('(₹)', '').replace('₹', ''): c for c in df.columns}
    for cand in candidates:
        clean_cand = cand.strip().lower().replace(' ', '').replace('_', '').replace('(₹)', '').replace('₹', '')
        if clean_cand in existing_cols: return existing_cols[clean_cand]
    return None

def clean_currency(val):
    if pd.isna(val) or str(val).strip() == '': return 0.0
    if isinstance(val, (int, float)): return float(val)
    try: return float(str(val).replace(',', '').replace(' ', '').replace('₹', ''))
    except ValueError: return 0.0

def normalize_gstin(gstin):
    if pd.isna(gstin): return ""
    return str(gstin).strip().upper().replace(" ", "")

def get_pan_from_gstin(gstin):
    norm = normalize_gstin(gstin)
    return norm[:10] if len(norm) >= 10 else norm

def get_similarity_score(s1, s2):
    if USE_RAPIDFUZZ: return fuzz.ratio(str(s1), str(s2))
    else: return difflib.SequenceMatcher(None, str(s1), str(s2)).ratio() * 100

def normalize_inv_basic(inv):
    if pd.isna(inv): return ""
    s = str(inv).upper(); s = "".join(s.split()); s = re.sub(r'[^A-Z0-9]', '', s)
    return s.lstrip('0')

def normalize_inv_numeric(inv):
    if pd.isna(inv): return ""
    s = str(inv); s = re.sub(r'[^0-9]', '', s)
    return s.lstrip('0')

def get_last_4(inv):
    if pd.isna(inv): return ""
    s = str(inv); s = re.sub(r'[^0-9]', '', s)
    return s[-4:] if len(s) > 4 else s.lstrip('0')

def normalize_ocr_fix(inv):
    s = normalize_inv_basic(inv)
    return s.replace('O', '0').replace('I', '1').replace('L', '1').replace('S', '5').replace('Z', '2').replace('B', '8')

# ==========================================
# 3. CORE LOGIC: 10-LAYER RECONCILIATION
# ==========================================
def run_10_layer_reconciliation(cis_df, gstr2b_df, col_map_cis, col_map_g2b, tol_std, tol_high, progress_bar, status_text):
    
    # --- A. PREPROCESSING ---
    status_text.text("Preprocessing Data...")
    cis_proc = cis_df.copy(); g2b_proc = gstr2b_df.copy()

    # Nuclear Cleanup
    cis_proc = cis_proc.loc[:, ~cis_proc.columns.duplicated()]
    g2b_proc = g2b_proc.loc[:, ~g2b_proc.columns.duplicated()]
    cols_to_purge = ['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic', 'Inv_Num', 'Inv_Last4', 'Inv_OCR', 'Taxable', 'Tax', 'Grand_Total', 'Matching Status', 'Match Category', 'Detailed Remark', 'GSTR 2B Key', 'CIS Key', 'Index CIS', 'INDEX', 'Matched_Flag']
    cis_proc.drop(columns=[c for c in cols_to_purge if c in cis_proc.columns], inplace=True, errors='ignore')
    g2b_proc.drop(columns=[c for c in cols_to_purge if c in g2b_proc.columns], inplace=True, errors='ignore')

    # Setup
    cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)
    g2b_proc['INDEX'] = g2b_proc.index + 100000 

    # Key Generation
    cis_proc['Norm_GSTIN'] = cis_proc[col_map_cis['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_PAN'] = cis_proc[col_map_cis['GSTIN']].apply(get_pan_from_gstin)
    g2b_proc['Norm_GSTIN'] = g2b_proc[col_map_g2b['GSTIN']].apply(normalize_gstin)
    g2b_proc['Norm_PAN'] = g2b_proc[col_map_g2b['GSTIN']].apply(get_pan_from_gstin)

    cis_proc['Inv_Basic'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_basic)
    cis_proc['Inv_Num'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_numeric)
    cis_proc['Inv_Last4'] = cis_proc[col_map_cis['INVOICE']].apply(get_last_4)
    cis_proc['Inv_OCR'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_ocr_fix)

    g2b_proc['Inv_Basic'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_basic)
    g2b_proc['Inv_Num'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_numeric)
    g2b_proc['Inv_Last4'] = g2b_proc[col_map_g2b['INVOICE']].apply(get_last_4)
    g2b_proc['Inv_OCR'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_ocr_fix)

    # Financials
    cis_proc['Taxable'] = cis_proc[col_map_cis['TAXABLE']].apply(clean_currency)
    cis_proc['Tax'] = (cis_proc[col_map_cis['IGST']].apply(clean_currency) + cis_proc[col_map_cis['CGST']].apply(clean_currency) + cis_proc[col_map_cis['SGST']].apply(clean_currency))
    cis_proc['Grand_Total'] = cis_proc['Taxable'] + cis_proc['Tax']

    g2b_proc['Taxable'] = g2b_proc[col_map_g2b['TAXABLE']].apply(clean_currency)
    g2b_proc['Tax'] = (g2b_proc[col_map_g2b['IGST']].apply(clean_currency) + g2b_proc[col_map_g2b['CGST']].apply(clean_currency) + g2b_proc[col_map_g2b['SGST']].apply(clean_currency))
    g2b_proc['Grand_Total'] = g2b_proc['Taxable'] + g2b_proc['Tax']

    # Init Columns
    cis_proc['Matching Status'] = "Unmatched"; cis_proc['Match Category'] = ""; cis_proc['Detailed Remark'] = ""; cis_proc['GSTR 2B Key'] = ""
    g2b_proc['Matching Status'] = "Unmatched"; g2b_proc['CIS Key'] = ""

    # Grouping
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic']).agg({
        'Taxable': 'sum', 'Tax': 'sum', 'Grand_Total': 'sum',
        'Inv_Num': 'first', 'Inv_Last4': 'first', 'Inv_OCR': 'first',
        col_map_cis['INVOICE']: 'first', col_map_cis['DATE']: 'first',
        'Index CIS': list
    }).reset_index()
    cis_grouped['Matched_Flag'] = False
    match_stats = {}

    # Commit Match Helper
    def commit_match(layer_name, row_cis, row_g2b, diff_grand, detail_str, is_reverse=False, g2b_ids=None):
        cis_indices = row_cis['Index CIS']
        g2b_indices = g2b_ids if is_reverse else [row_g2b['INDEX']]
        if not is_reverse: cis_grouped.at[row_cis.name, 'Matched_Flag'] = True

        for g_idx in g2b_indices:
            g2b_proc.loc[g2b_proc['INDEX'] == g_idx, 'Matching Status'] = "Matched"
            g2b_proc.loc[g2b_proc['INDEX'] == g_idx, 'CIS Key'] = ", ".join(map(str, cis_indices))

        for cis_id in cis_indices:
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Match Category'] = layer_name
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = ", ".join(map(str, g2b_indices))
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = "Matched"
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = detail_str
            ex = str(cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'].values[0])
            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = f"{'' if ex == 'nan' else ex} | {layer_name}".strip(" |")

    # Standard Layer Runner
    def run_standard_layer(layer_name, join_col_cis, join_col_g2b, tolerance, strict_tax_split=False, use_pan=False):
        count = 0
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            gstin = row_cis['Norm_GSTIN']; pan = row_cis['Norm_PAN']; inv_val = row_cis[join_col_cis]
            if not inv_val or len(str(inv_val)) < 2: continue

            mask = (g2b_proc['Matching Status'] == "Unmatched") & (g2b_proc[join_col_g2b] == inv_val)
            if use_pan: mask = mask & (g2b_proc['Norm_PAN'] == pan) 
            else: mask = mask & (g2b_proc['Norm_GSTIN'] == gstin)
            
            candidates = g2b_proc[mask]
            if candidates.empty: continue

            for g2b_idx, row_g2b in candidates.iterrows():
                diff_grand = abs(row_cis['Grand_Total'] - row_g2b['Grand_Total'])
                is_match = False
                if strict_tax_split:
                    if abs(row_cis['Taxable'] - row_g2b['Taxable']) <= tolerance and abs(row_cis['Tax'] - row_g2b['Tax']) <= tolerance: is_match = True
                elif diff_grand <= tolerance: is_match = True

                if is_match:
                    parts = ["GSTIN" if not use_pan else "PAN", "Invoice"]
                    if strict_tax_split: parts.extend(["Taxable", "Tax"])
                    else: parts.append(f"Total (Diff: {diff_grand:.2f})")
                    commit_match(layer_name, row_cis, row_g2b, diff_grand, f"Matched: {', '.join(parts)}")
                    count += 1; break
        match_stats[layer_name] = count

    # --- EXECUTION WITH PROGRESS ---
    layers = [
        ("Layer 1: Strict Match", "Inv_Basic", "Inv_Basic", tol_std, True, False),
        ("Layer 2: Grand Total", "Inv_Basic", "Inv_Basic", tol_std, False, False),
        ("Layer 3: High Tolerance", "Inv_Basic", "Inv_Basic", tol_high, False, False),
        ("Layer 4: Numeric Only", "Inv_Num", "Inv_Num", tol_std, False, False),
        ("Layer 5: Last 4 Digits", "Inv_Last4", "Inv_Last4", tol_std, False, False),
        ("Layer 6: PAN Level", "Inv_Basic", "Inv_Basic", tol_std, False, True),
    ]

    total_steps = 11 # 6 standard + Fuzzy + Reverse + Unique + OCR + Cleanup
    current_step = 0

    # Run Standard Layers 1-6
    for name, c_cis, c_g2b, tol, strict, pan in layers:
        current_step += 1
        progress_bar.progress(current_step / total_steps)
        status_text.text(f"Running {name}...")
        run_standard_layer(name, c_cis, c_g2b, tol, strict, pan)

    # Layer 7: Fuzzy
    current_step += 1
    progress_bar.progress(current_step / total_steps)
    status_text.text("Running Layer 7: Fuzzy Matching (Levenshtein)...")
    
    count = 0; layer_name = "Layer 7: Fuzzy"
    for idx, row_cis in cis_grouped.iterrows():
        if row_cis['Matched_Flag']: continue
        gstin = row_cis['Norm_GSTIN']; cis_inv = str(row_cis['Inv_Basic'])
        if len(cis_inv) < 3: continue
        g2b_candidates = g2b_proc[(g2b_proc['Matching Status'] == "Unmatched") & (g2b_proc['Norm_GSTIN'] == gstin)]
        best_match = None; best_score = 0.0
        for g_idx, row_g2b in g2b_candidates.iterrows():
            if abs(row_cis['Grand_Total'] - row_g2b['Grand_Total']) > tol_std: continue
            score = get_similarity_score(cis_inv, str(row_g2b['Inv_Basic']))
            if score > 85 and score > best_score: best_score = score; best_match = row_g2b
        if best_match is not None:
            diff = abs(row_cis['Grand_Total'] - best_match['Grand_Total'])
            commit_match(layer_name, row_cis, best_match, diff, f"Fuzzy Match: '{cis_inv}' vs '{best_match['Inv_Basic']}' ({int(best_score)}%)")
            count += 1
    match_stats[layer_name] = count

    # Layer 8: Reverse Clubbing
    current_step += 1
    progress_bar.progress(current_step / total_steps)
    status_text.text("Running Layer 8: Reverse Clubbing...")
    
    count = 0; layer_name = "Layer 8: Reverse Clubbing"
    g2b_unmatched = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"]
    g2b_grouped = g2b_unmatched.groupby(['Norm_GSTIN', 'Inv_Basic']).agg({'Grand_Total': 'sum', 'INDEX': list}).reset_index()
    for idx, row_cis in cis_grouped.iterrows():
        if row_cis['Matched_Flag']: continue
        match_row = g2b_grouped[(g2b_grouped['Norm_GSTIN'] == row_cis['Norm_GSTIN']) & (g2b_grouped['Inv_Basic'] == row_cis['Inv_Basic'])]
        if not match_row.empty:
            row_g2b = match_row.iloc[0]; diff = abs(row_cis['Grand_Total'] - row_g2b['Grand_Total'])
            if diff <= tol_std:
                commit_match(layer_name, row_cis, None, diff, f"Reverse Clubbing: 1 CIS vs {len(row_g2b['INDEX'])} G2B", True, row_g2b['INDEX'])
                count += 1
    match_stats[layer_name] = count

    # Layer 9: Unique Amount
    current_step += 1
    progress_bar.progress(current_step / total_steps)
    status_text.text("Running Layer 9: Unique Amount Match...")
    
    count = 0; layer_name = "Layer 9: Unique Amount"
    cis_rem = cis_grouped[~cis_grouped['Matched_Flag']]
    g2b_rem = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"]
    for idx, row_cis in cis_rem.iterrows():
        gstin = row_cis['Norm_GSTIN']; amt = row_cis['Grand_Total']
        cands = g2b_rem[(g2b_rem['Norm_GSTIN'] == gstin) & (np.abs(g2b_rem['Grand_Total'] - amt) <= tol_std)]
        if len(cands) == 1:
            cis_dupes = cis_rem[(cis_rem['Norm_GSTIN'] == gstin) & (np.abs(cis_rem['Grand_Total'] - amt) <= tol_std)]
            if len(cis_dupes) == 1:
                row_g2b = cands.iloc[0]; diff = abs(amt - row_g2b['Grand_Total'])
                commit_match(layer_name, row_cis, row_g2b, diff, f"Unique Amount Match: {row_cis['Inv_Basic']} vs {row_g2b['Inv_Basic']}")
                count += 1; g2b_rem = g2b_rem.drop(row_g2b.name)
    match_stats[layer_name] = count

    # Layer 10: OCR
    current_step += 1
    progress_bar.progress(current_step / total_steps)
    status_text.text("Running Layer 10: OCR Fix...")
    run_standard_layer("Layer 10: OCR Fix", "Inv_OCR", "Inv_OCR", tol_std)

    # Cleanup
    progress_bar.progress(1.0)
    status_text.text("Finalizing Report...")
    
    unmatched = cis_proc['Matching Status'] == "Unmatched"
    cis_proc.loc[unmatched, 'Detailed Remark'] = "Mismatch: Invoice Number not found in GSTR-2B"
    cis_proc.loc[unmatched, 'Short Remark'] = "Not Found"

    cutoff = pd.Timestamp("2024-03-31")
    cis_proc['D_Obj'] = pd.to_datetime(cis_proc[col_map_cis['DATE']], dayfirst=True, errors='coerce')
    mask = (cis_proc['D_Obj'] < cutoff) & (cis_proc['D_Obj'].notna())
    cis_proc.loc[mask, 'Short Remark'] += " + Time Barred"
    cis_proc.loc[mask, 'Detailed Remark'] += " [Warning: Date < 31 Mar 2024]"

    drop_cols = ['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic', 'Inv_Num', 'Inv_Last4', 'Inv_OCR', 'Taxable', 'Tax', 'Grand_Total', 'D_Obj']
    cis_final = cis_proc.drop(columns=[c for c in drop_cols if c in cis_proc.columns])
    g2b_final = g2b_proc.drop(columns=[c for c in drop_cols if c in g2b_proc.columns])

    return cis_final, g2b_final, match_stats

# ==========================================
# 4. STREAMLIT UI
# ==========================================
st.set_page_config(page_title="GST 10-Layer Reconciliation", layout="wide")
st.title("📊 10-Layer Auto-Reconciliation Tool")

st.markdown("""
**Algorithm Layers:**
1.  **Strict:** Exact Match (Inv + Taxable + Tax).
2.  **Grand Total:** Exact Match (Inv + Grand Total).
3.  **High Tolerance:** Exact Match (Inv + Grand Total within Tol).
4.  **Numeric Only:** Strips letters (`GST/01` -> `01`).
5.  **Last 4 Digits:** Matches last 4 digits (`WB-0995` -> `0995`).
6.  **PAN Level:** Matches Head Office PAN (Ignores GSTIN suffix).
7.  **Fuzzy:** Matches typos (`9855` vs `9885`).
8.  **Reverse Clubbing:** 1 CIS Entry vs Many G2B Entries.
9.  **Unique Amount:** Matches unique amounts when Invoice No fails.
10. **Layer 10 (OCR Fix):** Fixes `O` vs `0`, `S` vs `5`, `I` vs `1`.
""")

col1, col2 = st.columns(2)
with col1: cis_file = st.file_uploader("1. Upload CIS File (.xlsx)", type=['xlsx'], key="cis")
with col2: g2b_file = st.file_uploader("2. Upload GSTR-2B File (.xlsx)", type=['xlsx'], key="g2b")

c1, c2 = st.columns(2)
tol_std = c1.number_input("Standard Tolerance (₹)", value=2.0)
tol_high = c2.number_input("High Tolerance (Layer 3) (₹)", value=50.0)

if st.button("🚀 Run 10-Layer Algorithm", type="primary"):
    if cis_file and g2b_file:
        # Progress Bar & Status Text Placeholders
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        start_time = time.time()
        
        try:
            df_cis = pd.read_excel(cis_file)
            df_cis = df_cis.loc[:, ~df_cis.columns.duplicated()] 
            
            xl = pd.ExcelFile(g2b_file)
            df_g2b = load_gstr2b_with_stitching(g2b_file, 'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0])
            df_g2b = df_g2b.loc[:, ~df_g2b.columns.duplicated()] 

            cis_map = {'GSTIN': ['SupplierGSTIN','GSTIN'], 'INVOICE': ['DocumentNumber','Invoice Number'], 'DATE': ['DocumentDate','Invoice Date'], 'TAXABLE': ['TaxableValue','Taxable Value'], 'IGST': ['IntegratedTaxAmount','Integrated Tax'], 'CGST': ['CentralTaxAmount','Central Tax'], 'SGST': ['StateUT TaxAmount','State/UT Tax']}
            g2b_map = {'GSTIN': ['GSTIN of supplier','Supplier GSTIN'], 'INVOICE': ['Invoice number','Invoice No'], 'DATE': ['Invoice Date','Date'], 'TAXABLE': ['Taxable Value (₹)','Taxable Value'], 'IGST': ['Integrated Tax(₹)','Integrated Tax'], 'CGST': ['Central Tax(₹)','Central Tax'], 'SGST': ['State/UT Tax(₹)','State/UT Tax']}
            
            final_cis_map = {}; final_g2b_map = {}
            for k, v in cis_map.items(): 
                found = find_column(df_cis, v)
                if found: final_cis_map[k] = found
                else: st.error(f"Missing CIS: {v[0]}"); st.stop()
            for k, v in g2b_map.items(): 
                found = find_column(df_g2b, v)
                if found: final_g2b_map[k] = found
                else: st.error(f"Missing GSTR-2B: {v[0]}"); st.stop()

            # Run Logic
            cis_res, g2b_res, stats = run_10_layer_reconciliation(df_cis, df_g2b, final_cis_map, final_g2b_map, tol_std, tol_high, progress_bar, status_text)
            
            end_time = time.time()
            elapsed_time = round(end_time - start_time, 2)
            
            status_text.text(f"✅ Process Completed in {elapsed_time} seconds!")
            st.success(f"✅ Reconciliation Complete! (Time: {elapsed_time}s)")
            
            st.table(pd.DataFrame(list(stats.items()), columns=['Layer', 'Matches']))
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
            st.download_button("Download Result", output.getvalue(), "Reconciliation_Output.xlsx")

        except Exception as e:
            status_text.text("Error Occurred!")
            st.error(f"Error: {e}")
