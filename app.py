import streamlit as st
import pandas as pd
import io
import re
import numpy as np
from datetime import datetime
import time

# Try importing rapidfuzz for speed, fallback to difflib if missing
try:
    from rapidfuzz import fuzz
    USE_RAPIDFUZZ = True
except ImportError:
    import difflib
    USE_RAPIDFUZZ = False

# ==========================================
# PAGE CONFIG & CUSTOM CSS
# ==========================================
st.set_page_config(
    page_title="GST Reconciliation",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
    <style>
    .main {
        padding-top: 0rem;
    }
    .stButton>button {
        width: 100%;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        color: white;
        font-weight: 600;
        border: none;
        padding: 0rem;
        border-radius: 6px;
        font-size: 0rem;
    }
    .stButton>button:hover {
        background: linear-gradient(90deg, #764ba2 0%, #667eea 100%);
        border: none;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 0rem;
        border-radius: 7px;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .layer-card {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .success-box {
        background: #d4edda;
        border: 1px solid #c3e6cb;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background: #d1ecf1;
        border: 1px solid #bee5eb;
        border-radius: 8px;
        padding: 1rem;
        margin: 1rem 0;
    }
    h1 {
        color: #667eea;
        font-weight: 700;
    }
    .stProgress > div > div > div {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
    }
    </style>
""", unsafe_allow_html=True)

# ==========================================
# HEADER
# ==========================================
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.title("📊 GST Reconciliation")
    st.markdown("**10-Layer GST Matching Tool**")

st.divider()

# ==========================================
# ROBUST DATA LOADER (Header Stitching)
# ==========================================
def load_gstr2b_with_stitching(file_obj, sheet_name, progress_callback=None):
    """
    Reads first 8 rows to find headers. Stitches split headers if found.
    """
    if progress_callback:
        progress_callback(0.1, "Loading GSTR-2B file...")
    
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
    
    if progress_callback:
        progress_callback(0.2, "Processing GSTR-2B headers...")
    
    try:
        df_final = pd.read_excel(file_obj, sheet_name=sheet_name, header=header_end_row + 1)
    except:
        df_final = pd.read_excel(file_obj, sheet_name=0, header=header_end_row + 1)
    
    # --- SAFETY: DEDUPLICATE COLUMNS IMMEDIATELY ---
    df_final = df_final.loc[:, ~df_final.columns.duplicated()]

    # Assign columns safely
    current_cols = len(df_final.columns)
    if len(final_headers) >= current_cols:
        df_final.columns = final_headers[:current_cols]
    else:
        df_final.columns = final_headers + [f"Col_{i}" for i in range(current_cols - len(final_headers))]
    
    if progress_callback:
        progress_callback(0.3, "GSTR-2B loaded successfully")
        
    return df_final

# ==========================================
# HELPER FUNCTIONS & NORMALIZERS
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
    norm = normalize_gstin(gstin)
    if len(norm) >= 10:
        return norm[:10]
    return norm

def get_similarity_score(s1, s2):
    """Returns similarity 0-100"""
    if USE_RAPIDFUZZ:
        return fuzz.ratio(str(s1), str(s2))
    else:
        return difflib.SequenceMatcher(None, str(s1), str(s2)).ratio() * 100

# --- INVOICE NORMALIZERS ---
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

def normalize_inv_stripped(inv):
    """Enhanced normalization: strips common prefixes and suffixes"""
    if pd.isna(inv): return ""
    s = str(inv).upper()
    s = "".join(s.split())
    
    # Strip common prefixes (year patterns, branch codes)
    prefix_patterns = [
        r'^(FY)?20\d{2}[-/]?',  # FY2024-, 2024-, 2024/
        r'^(FY)?\d{2}[-/]?',     # FY24-, 24-
        r'^(FY)?\d{2}[-]?\d{2}[-/]?',  # 24-25/, FY24-25-
        r'^[A-Z]{2,4}[-/]',      # MUM-, DEL-, HO-, BR1-, BRANCH-
    ]
    for pattern in prefix_patterns:
        s = re.sub(pattern, '', s)
    
    # Strip common suffixes
    suffix_patterns = [
        r'[-/](REV|COR|AMD|A|B|C|INV|INVOICE)$',  # -REV, -COR, -A, /INV
        r'[-/]\d{1}$',  # -1, -2, -3 (revision numbers)
    ]
    for pattern in suffix_patterns:
        s = re.sub(pattern, '', s)
    
    # Remove all non-alphanumeric
    s = re.sub(r'[^A-Z0-9]', '', s)
    s = s.lstrip('0')
    return s

def get_last_4(inv):
    if pd.isna(inv): return ""
    s = str(inv)
    s = re.sub(r'[^0-9]', '', s)
    if len(s) > 4: return s[-4:]
    return s.lstrip('0')

# ==========================================
# CORE LOGIC: 8-LAYER RECONCILIATION
# ==========================================
def run_8_layer_reconciliation(cis_df, gstr2b_df, col_map_cis, col_map_g2b, tol_std, tol_high, progress_callback=None):
    
    if progress_callback:
        progress_callback(0.35, "Starting reconciliation process...")
    
    # --- A. PREPROCESSING ---
    cis_proc = cis_df.copy()
    g2b_proc = gstr2b_df.copy()

    # --- NUCLEAR CLEANUP ---
    cis_proc = cis_proc.loc[:, ~cis_proc.columns.duplicated()]
    g2b_proc = g2b_proc.loc[:, ~g2b_proc.columns.duplicated()]

    cols_to_purge = [
        'Norm_GSTIN', 'Norm_PAN', 'Inv_Basic', 'Inv_Num', 'Inv_Last4', 'Inv_Stripped',
        'Taxable', 'Tax', 'Grand_Total', 'Matching Status', 'Match Category',
        'Detailed Remark', 'GSTR 2B Key', 'CIS Key', 'Index CIS', 'INDEX', 'Matched_Flag'
    ]
    cis_proc.drop(columns=[c for c in cols_to_purge if c in cis_proc.columns], inplace=True, errors='ignore')
    g2b_proc.drop(columns=[c for c in cols_to_purge if c in g2b_proc.columns], inplace=True, errors='ignore')

    if progress_callback:
        progress_callback(0.4, "Preprocessing data...")

    # IDs
    cis_proc['Index CIS'] = range(1, len(cis_proc) + 1)
    g2b_proc['INDEX'] = g2b_proc.index + 100000 

    # Keys: GSTIN & PAN
    cis_proc['Norm_GSTIN'] = cis_proc[col_map_cis['GSTIN']].apply(normalize_gstin)
    cis_proc['Norm_PAN'] = cis_proc[col_map_cis['GSTIN']].apply(get_pan_from_gstin)
    
    g2b_proc['Norm_GSTIN'] = g2b_proc[col_map_g2b['GSTIN']].apply(normalize_gstin)
    g2b_proc['Norm_PAN'] = g2b_proc[col_map_g2b['GSTIN']].apply(get_pan_from_gstin)

    # Keys: Invoices
    cis_proc['Inv_Basic'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_basic)
    cis_proc['Inv_Num'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_numeric)
    cis_proc['Inv_Last4'] = cis_proc[col_map_cis['INVOICE']].apply(get_last_4)
    cis_proc['Inv_Stripped'] = cis_proc[col_map_cis['INVOICE']].apply(normalize_inv_stripped)

    g2b_proc['Inv_Basic'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_basic)
    g2b_proc['Inv_Num'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_numeric)
    g2b_proc['Inv_Last4'] = g2b_proc[col_map_g2b['INVOICE']].apply(get_last_4)
    g2b_proc['Inv_Stripped'] = g2b_proc[col_map_g2b['INVOICE']].apply(normalize_inv_stripped)

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

    if progress_callback:
        progress_callback(0.45, "Creating invoice groups...")

    # --- B. GROUPING (Standard Clubbing) - OPTIMIZED ---
    cis_grouped = cis_proc.groupby(['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic'], sort=False).agg({
        'Taxable': 'sum', 'Tax': 'sum', 'Grand_Total': 'sum',
        'Inv_Num': 'first', 'Inv_Last4': 'first', 'Inv_Stripped': 'first',
        col_map_cis['INVOICE']: 'first', col_map_cis['DATE']: 'first',
        'Index CIS': list
    }).reset_index()
    cis_grouped['Matched_Flag'] = False

    match_stats = {}

    # --- C. HELPER: COMMIT MATCH ---
    def commit_match(layer_name, row_cis, row_g2b, diff_grand, detail_str, is_reverse=False, g2b_ids=None):
        
        if is_reverse:
            cis_indices = row_cis['Index CIS']
            g2b_indices = g2b_ids
        else:
            cis_indices = row_cis['Index CIS']
            g2b_indices = [row_g2b['INDEX']]
            cis_grouped.at[row_cis.name, 'Matched_Flag'] = True

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

    # --- D. STANDARD LAYERS (1-6) - OPTIMIZED ---
    def run_standard_layer(layer_name, join_col_cis, join_col_g2b, tolerance, strict_tax_split=False, use_pan=False):
        count = 0
        
        # Create indices for faster lookups
        if use_pan:
            g2b_index = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"].set_index(['Norm_PAN', join_col_g2b])
        else:
            g2b_index = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"].set_index(['Norm_GSTIN', join_col_g2b])
        
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            pan = row_cis['Norm_PAN']
            inv_val = row_cis[join_col_cis]
            
            if not inv_val or len(str(inv_val)) < 2: continue

            # Use index for faster lookup
            try:
                if use_pan:
                    candidates = g2b_index.loc[(pan, inv_val)]
                else:
                    candidates = g2b_index.loc[(gstin, inv_val)]
                    
                if isinstance(candidates, pd.Series):
                    candidates = candidates.to_frame().T
                    
            except KeyError:
                continue

            for g2b_idx, row_g2b in candidates.iterrows():
                # Quick amount check first (fastest)
                diff_grand = abs(row_cis['Grand_Total'] - row_g2b['Grand_Total'])
                if diff_grand > (tolerance if not strict_tax_split else tolerance):
                    continue
                
                # Compare
                is_match = False
                if strict_tax_split:
                    diff_taxable = abs(row_cis['Taxable'] - row_g2b['Taxable'])
                    diff_tax = abs(row_cis['Tax'] - row_g2b['Tax'])
                    if diff_taxable <= tolerance and diff_tax <= tolerance: 
                        is_match = True
                else:
                    is_match = True

                if is_match:
                    # Build Remark
                    matched_parts = ["GSTIN" if not use_pan else "PAN"]
                    if join_col_cis == "Inv_Basic": matched_parts.append("Invoice Number")
                    elif join_col_cis == "Inv_Num": matched_parts.append(f"Numeric Invoice ({row_cis[col_map_cis['INVOICE']]} vs {row_g2b[col_map_g2b['INVOICE']]})")
                    elif join_col_cis == "Inv_Last4": matched_parts.append(f"Last 4 Digits ({row_cis[col_map_cis['INVOICE']]} vs {row_g2b[col_map_g2b['INVOICE']]})")

                    if strict_tax_split:
                        matched_parts.extend(["Taxable Value", "Tax Amount"])
                    else:
                        matched_parts.append(f"Grand Total (Diff: {diff_grand:.2f})")

                    # Date check
                    cis_date = pd.to_datetime(row_cis[col_map_cis['DATE']], dayfirst=True, errors='coerce')
                    g2b_date = pd.to_datetime(row_g2b[col_map_g2b['DATE']], dayfirst=True, errors='coerce')
                    if pd.notna(cis_date) and pd.notna(g2b_date) and cis_date == g2b_date:
                        matched_parts.append("Date")

                    detail_str = "Matched: " + ", ".join(matched_parts)
                    if use_pan and row_cis['Norm_GSTIN'] != row_g2b['Norm_GSTIN']:
                        detail_str += f" | Note: Matched under different GSTIN {row_g2b['Norm_GSTIN']}"

                    commit_match(layer_name, row_cis, row_g2b, diff_grand, detail_str)
                    count += 1
                    break
        match_stats[layer_name] = count

    # --- RUN STANDARD LAYERS WITH PROGRESS ---
    layers = [
        ("Layer 1: Strict", "Inv_Basic", "Inv_Basic", tol_std, True, False, 0.50),
        ("Layer 2: Grand Total", "Inv_Basic", "Inv_Basic", tol_std, False, False, 0.55),
        ("Layer 3: High Tolerance", "Inv_Basic", "Inv_Basic", tol_high, False, False, 0.60),
        ("Layer 4: Stripped Pattern", "Inv_Stripped", "Inv_Stripped", tol_std, False, False, 0.63),
        ("Layer 5: Numeric Only", "Inv_Num", "Inv_Num", tol_std, False, False, 0.66),
        ("Layer 6: Last 4 Digits", "Inv_Last4", "Inv_Last4", tol_std, False, False, 0.70),
        ("Layer 7: PAN Level", "Inv_Basic", "Inv_Basic", tol_std, False, True, 0.75),
    ]
    
    for layer_name, col_cis, col_g2b, tol, strict, use_pan, prog in layers:
        if progress_callback:
            progress_callback(prog, f"Running {layer_name}...")
        run_standard_layer(layer_name, col_cis, col_g2b, tol, strict, use_pan)

    # --- LAYER 8: FUZZY (LEVENSHTEIN) ---
    def run_fuzzy_layer():
        count = 0
        layer_name = "Layer 8: Fuzzy"
        
        # Pre-filter and index G2B
        g2b_unmatched = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"].copy()
        
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            cis_inv = str(row_cis['Inv_Basic'])
            if len(cis_inv) < 3: continue

            # Filter by GSTIN and amount first
            g2b_candidates = g2b_unmatched[
                (g2b_unmatched['Norm_GSTIN'] == gstin) &
                (abs(g2b_unmatched['Grand_Total'] - row_cis['Grand_Total']) <= tol_std)
            ]
            
            if g2b_candidates.empty: continue
            
            best_match = None
            best_score = 0.0

            for g_idx, row_g2b in g2b_candidates.iterrows():
                # String Similarity
                g2b_inv = str(row_g2b['Inv_Basic'])
                score = get_similarity_score(cis_inv, g2b_inv)
                
                if score > 85 and score > best_score:
                    best_score = score
                    best_match = row_g2b

            if best_match is not None:
                diff_grand = abs(row_cis['Grand_Total'] - best_match['Grand_Total'])
                detail = f"Matched: GSTIN, Grand Total | Fuzzy Invoice: '{cis_inv}' vs '{best_match['Inv_Basic']}' (Score: {int(best_score)}%)"
                commit_match(layer_name, row_cis, best_match, diff_grand, detail)
                count += 1
        
        match_stats[layer_name] = count

    if progress_callback:
        progress_callback(0.78, "Running Layer 8: Fuzzy Matching...")
    run_fuzzy_layer()

    # --- LAYER 9: FORWARD CLUBBING (Many CIS → One G2B) ---
    def run_forward_clubbing():
        count = 0
        layer_name = "Layer 9: Forward Clubbing"
        
        # Get all unmatched CIS records
        unmatched_cis_indices = cis_grouped[~cis_grouped['Matched_Flag']].index.tolist()
        
        if not unmatched_cis_indices:
            match_stats[layer_name] = 0
            return
        
        # Get unmatched G2B records
        g2b_unmatched = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"].copy()
        
        if g2b_unmatched.empty:
            match_stats[layer_name] = 0
            return
        
        # Group G2B by GSTIN for faster lookup
        g2b_by_gstin = g2b_unmatched.groupby('Norm_GSTIN')
        
        # Try to find combinations of CIS invoices that match single G2B invoices
        for gstin, g2b_group in g2b_by_gstin:
            # Get unmatched CIS records for this GSTIN
            cis_for_gstin = cis_grouped[
                (cis_grouped.index.isin(unmatched_cis_indices)) & 
                (cis_grouped['Norm_GSTIN'] == gstin)
            ]
            
            if len(cis_for_gstin) < 2:
                continue  # Need at least 2 CIS records to club
            
            # For each G2B record, try to find combination of CIS records
            for g2b_idx, row_g2b in g2b_group.iterrows():
                g2b_amount = row_g2b['Grand_Total']
                g2b_inv = row_g2b['Inv_Basic']
                
                # Find CIS records with same invoice base (likely same invoice split in CIS)
                matching_inv_cis = cis_for_gstin[cis_for_gstin['Inv_Basic'] == g2b_inv]
                
                if len(matching_inv_cis) >= 2:
                    # Check if sum of these CIS records matches G2B amount
                    total_cis_amount = matching_inv_cis['Grand_Total'].sum()
                    diff = abs(total_cis_amount - g2b_amount)
                    
                    if diff <= tol_std:
                        # Match found!
                        cis_indices_list = []
                        for _, cis_row in matching_inv_cis.iterrows():
                            cis_indices_list.extend(cis_row['Index CIS'])
                            cis_grouped.at[cis_row.name, 'Matched_Flag'] = True
                        
                        g2b_indices = [row_g2b['INDEX']]
                        
                        # Update G2B
                        g2b_proc.loc[g2b_proc['INDEX'] == row_g2b['INDEX'], 'Matching Status'] = "Matched"
                        g2b_proc.loc[g2b_proc['INDEX'] == row_g2b['INDEX'], 'CIS Key'] = ", ".join(map(str, cis_indices_list))
                        
                        # Update CIS records
                        detail = f"Matched: GSTIN, Invoice Number | Forward Clubbing: {len(matching_inv_cis)} CIS vs 1 G2B Record (Total Diff: {diff:.2f})"
                        for cis_id in cis_indices_list:
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Matching Status'] = "Matched"
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Match Category'] = layer_name
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'GSTR 2B Key'] = ", ".join(map(str, g2b_indices))
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Short Remark'] = "Matched"
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Detailed Remark'] = detail
                            
                            existing = str(cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'].values[0])
                            if existing == 'nan': existing = ""
                            new_rem = f"{existing} | {layer_name}".strip(" |")
                            cis_proc.loc[cis_proc['Index CIS'] == cis_id, 'Comments&Remarks'] = new_rem
                        
                        count += 1
                        # Remove from unmatched list
                        unmatched_cis_indices = [idx for idx in unmatched_cis_indices if idx not in matching_inv_cis.index]
        
        match_stats[layer_name] = count
    
    if progress_callback:
        progress_callback(0.83, "Running Layer 9: Forward Clubbing...")
    run_forward_clubbing()

    # --- LAYER 10: REVERSE CLUBBING ---
    def run_reverse_clubbing():
        count = 0
        layer_name = "Layer 10: Reverse Clubbing"
        
        # Group Unmatched G2B
        g2b_unmatched = g2b_proc[g2b_proc['Matching Status'] == "Unmatched"]
        
        g2b_grouped = g2b_unmatched.groupby(['Norm_GSTIN', 'Inv_Basic'], sort=False).agg({
            'Grand_Total': 'sum',
            'INDEX': list
        }).reset_index()
        
        for idx, row_cis in cis_grouped.iterrows():
            if row_cis['Matched_Flag']: continue
            
            gstin = row_cis['Norm_GSTIN']
            inv = row_cis['Inv_Basic']
            
            match_row = g2b_grouped[
                (g2b_grouped['Norm_GSTIN'] == gstin) & 
                (g2b_grouped['Inv_Basic'] == inv)
            ]
            
            if match_row.empty: continue
            
            row_g2b_group = match_row.iloc[0]
            diff_grand = abs(row_cis['Grand_Total'] - row_g2b_group['Grand_Total'])
            
            if diff_grand <= tol_std:
                g2b_indices = row_g2b_group['INDEX']
                detail = f"Matched: GSTIN, Invoice Number | Reverse Clubbing: 1 CIS vs {len(g2b_indices)} G2B Records (Total Diff: {diff_grand:.2f})"
                cis_grouped.at[idx, 'Matched_Flag'] = True
                commit_match(layer_name, row_cis, None, diff_grand, detail, is_reverse=True, g2b_ids=g2b_indices)
                count += 1
                
        match_stats[layer_name] = count

    if progress_callback:
        progress_callback(0.87, "Running Layer 10: Reverse Clubbing...")
    run_reverse_clubbing()

    if progress_callback:
        progress_callback(0.90, "Finalizing results...")

    # --- CLEANUP & TIME BARRED ---
    unmatched_mask = cis_proc['Matching Status'] == "Unmatched"
    if unmatched_mask.any():
        cis_proc.loc[unmatched_mask, 'Detailed Remark'] = "Mismatch: Invoice Number not found in GSTR-2B"
        cis_proc.loc[unmatched_mask, 'Short Remark'] = "Not Found"

    cutoff = pd.Timestamp("2024-03-31")
    cis_proc['D_Obj'] = pd.to_datetime(cis_proc[col_map_cis['DATE']], dayfirst=True, errors='coerce')
    mask = (cis_proc['D_Obj'] < cutoff) & (cis_proc['D_Obj'].notna())
    
    cis_proc.loc[mask, 'Short Remark'] = cis_proc.loc[mask, 'Short Remark'].astype(str) + " + Time Barred"
    cis_proc.loc[mask, 'Detailed Remark'] = cis_proc.loc[mask, 'Detailed Remark'].astype(str) + " [Warning: Date < 31 Mar 2024]"

    drop_cols = ['Norm_GSTIN', 'Norm_PAN', 'Inv_Basic', 'Inv_Num', 'Inv_Last4', 'Inv_Stripped', 'Taxable', 'Tax', 'Grand_Total', 'D_Obj']
    cis_final = cis_proc.drop(columns=[c for c in drop_cols if c in cis_proc.columns])
    g2b_final = g2b_proc.drop(columns=[c for c in drop_cols if c in g2b_proc.columns])

    if progress_callback:
        progress_callback(1.0, "Reconciliation complete!")

    return cis_final, g2b_final, match_stats

# ==========================================
# STREAMLIT UI
# ==========================================

# Sidebar with instructions
with st.sidebar:
    st.markdown("### 📋 Instructions")
    st.markdown("""
    1. **Upload Files**: Upload both CIS and GSTR-2B Excel files
    2. **Set Tolerances**: Adjust amount matching tolerances
    3. **Run Algorithm**: Click the button to start reconciliation
    4. **Download Results**: Get reconciled Excel file
    """)
    
    st.divider()
    
    st.markdown("### 🎯 Algorithm Layers")
    layers_info = [
        ("1️⃣", "Strict Match", "GSTIN + Exact Invoice + Exact Taxable + Exact Tax"),
        ("2️⃣", "Grand Total", "Exact invoice + total amount"),
        ("3️⃣", "High Tolerance", "With higher tolerance"),
        ("4️⃣", "Stripped Pattern", "Removes prefixes/suffixes"),
        ("5️⃣", "Numeric Only", "Strips letters from invoice"),
        ("6️⃣", "Last 4 Digits", "Matches last 4 digits"),
        ("7️⃣", "PAN Level", "Head office matching"),
        ("8️⃣", "Fuzzy Match", "Handles typos"),
        ("9️⃣", "Forward Club", "Many CIS vs 1 G2B"),
        ("🔟", "Reverse Club", "1 CIS vs many G2B")
    ]
    
    for emoji, name, desc in layers_info:
        st.markdown(f"""
        <div style='background: #f8f9fa; padding: 0.5rem; border-radius: 5px; margin: 0.3rem 0;'>
            <strong>{emoji} {name}</strong><br>
            <small style='color: #666;'>{desc}</small>
        </div>
        """, unsafe_allow_html=True)

# File upload section
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 📄 CIS File")
    cis_file = st.file_uploader("Upload CIS Excel file", type=['xlsx'], key="cis", label_visibility="collapsed")
    if cis_file:
        st.success(f"✅ {cis_file.name}")

with col2:
    st.markdown("#### 📄 GSTR-2B File")
    g2b_file = st.file_uploader("Upload GSTR-2B Excel file", type=['xlsx'], key="g2b", label_visibility="collapsed")
    if g2b_file:
        st.success(f"✅ {g2b_file.name}")

st.divider()

# Settings section
st.markdown("#### ⚙️ Matching Parameters")
col1, col2, col3 = st.columns([1, 1, 1])
with col1:
    tol_std = st.number_input("Standard Tolerance (₹)", value=10.0, min_value=0.0, step=0.5, help="Tolerance for most matching layers")
with col2:
    tol_high = st.number_input("High Tolerance (₹)", value=50.0, min_value=0.0, step=5.0, help="Higher tolerance for Layer 3")
with col3:
    st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)
    if st.button("🔄 Reset to Defaults"):
        st.rerun()

st.divider()

# Run button
if st.button("🚀 Start Reconciliation Process", type="primary", use_container_width=True):
    if cis_file and g2b_file:
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        def update_progress(progress, message):
            progress_bar.progress(progress)
            status_text.markdown(f"**{message}**")
        
        start_time = time.time()
        
        try:
            # Load files
            update_progress(0.05, "📂 Loading CIS file...")
            df_cis = pd.read_excel(cis_file)
            df_cis = df_cis.loc[:, ~df_cis.columns.duplicated()]
            
            update_progress(0.15, "📂 Loading GSTR-2B file...")
            xl = pd.ExcelFile(g2b_file)
            df_g2b = load_gstr2b_with_stitching(
                g2b_file, 
                'B2B' if 'B2B' in xl.sheet_names else xl.sheet_names[0],
                progress_callback=update_progress
            )
            df_g2b = df_g2b.loc[:, ~df_g2b.columns.duplicated()]

            # Map columns
            cis_map = {
                'GSTIN': ['SupplierGSTIN','GSTIN'], 
                'INVOICE': ['DocumentNumber','Invoice Number'], 
                'DATE': ['DocumentDate','Invoice Date'], 
                'TAXABLE': ['TaxableValue','Taxable Value'], 
                'IGST': ['IntegratedTaxAmount','Integrated Tax'], 
                'CGST': ['CentralTaxAmount','Central Tax'], 
                'SGST': ['StateUT TaxAmount','State/UT Tax']
            }
            
            g2b_map = {
                'GSTIN': ['GSTIN of supplier','Supplier GSTIN'], 
                'INVOICE': ['Invoice number','Invoice No'], 
                'DATE': ['Invoice Date','Date'], 
                'TAXABLE': ['Taxable Value (₹)','Taxable Value'], 
                'IGST': ['Integrated Tax(₹)','Integrated Tax'], 
                'CGST': ['Central Tax(₹)','Central Tax'], 
                'SGST': ['State/UT Tax(₹)','State/UT Tax']
            }
            
            final_cis_map = {}
            final_g2b_map = {}
            
            for k, v in cis_map.items(): 
                found = find_column(df_cis, v)
                if found: 
                    final_cis_map[k] = found
                else: 
                    st.error(f"❌ Missing CIS column: {v[0]}")
                    st.stop()
                    
            for k, v in g2b_map.items(): 
                found = find_column(df_g2b, v)
                if found: 
                    final_g2b_map[k] = found
                else: 
                    st.error(f"❌ Missing GSTR-2B column: {v[0]}")
                    st.stop()

            # Run reconciliation
            cis_res, g2b_res, stats = run_8_layer_reconciliation(
                df_cis, df_g2b, 
                final_cis_map, final_g2b_map, 
                tol_std, tol_high,
                progress_callback=update_progress
            )
            
            elapsed_time = time.time() - start_time
            
            # Clear progress
            progress_bar.empty()
            status_text.empty()
            
            # Success message
            st.markdown(f"""
            <div class='success-box'>
                <h3>✅ Reconciliation Complete!</h3>
                <p>Processed in <strong>{elapsed_time:.2f} seconds</strong></p>
            </div>
            """, unsafe_allow_html=True)
            
            # Statistics
            st.markdown("### 📊 Matching Results")
            
            # Calculate totals
            total_cis = len(cis_res)
            total_matched = (cis_res['Matching Status'] == 'Matched').sum()
            total_unmatched = total_cis - total_matched
            match_rate = (total_matched / total_cis * 100) if total_cis > 0 else 0
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class='metric-card'>
                    <div class='metric-label'>Total Records</div>
                    <div class='metric-value'>{total_cis:,}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class='metric-card' style='background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);'>
                    <div class='metric-label'>Matched</div>
                    <div class='metric-value'>{total_matched:,}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"""
                <div class='metric-card' style='background: linear-gradient(135deg, #ee0979 0%, #ff6a00 100%);'>
                    <div class='metric-label'>Unmatched</div>
                    <div class='metric-value'>{total_unmatched:,}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class='metric-card' style='background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);'>
                    <div class='metric-label'>Match Rate</div>
                    <div class='metric-value'>{match_rate:.1f}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Layer-wise breakdown
            st.markdown("### 📈 Layer-wise Breakdown")
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Create DataFrame for display
                stats_df = pd.DataFrame([
                    {"Layer": k, "Matches": v, "Percentage": f"{(v/total_matched*100):.1f}%" if total_matched > 0 else "0%"} 
                    for k, v in stats.items()
                ])
                st.dataframe(stats_df, use_container_width=True, hide_index=True)
            
            with col2:
                st.markdown("#### Top Performers")
                sorted_stats = sorted(stats.items(), key=lambda x: x[1], reverse=True)[:3]
                for i, (layer, count) in enumerate(sorted_stats, 1):
                    emoji = "🥇" if i == 1 else "🥈" if i == 2 else "🥉"
                    st.markdown(f"{emoji} **{layer.split(':')[1].strip()}**: {count}")
            
            # Download section
            st.divider()
            st.markdown("### 💾 Download Results")
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                cis_res.to_excel(writer, sheet_name='CIS_Reconciled', index=False)
                g2b_res.to_excel(writer, sheet_name='GSTR2B_Mapped', index=False)
                
                # Add summary sheet
                summary_data = {
                    'Metric': ['Total CIS Records', 'Matched Records', 'Unmatched Records', 'Match Rate', 'Processing Time'],
                    'Value': [total_cis, total_matched, total_unmatched, f"{match_rate:.2f}%", f"{elapsed_time:.2f}s"]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.download_button(
                    label="📥 Download Reconciliation Report",
                    data=output.getvalue(),
                    file_name=f"GST_Reconciliation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )

        except Exception as e:
            progress_bar.empty()
            status_text.empty()
            st.error(f"❌ **Error occurred:** {str(e)}")
            st.exception(e)
    else:
        st.warning("⚠️ Please upload both CIS and GSTR-2B files to proceed.")

# Footer
st.divider()
st.markdown("""
<div style='text-align: center; color: #666; padding: 0rem;'>
    <p>GST Reconciliation Tool v1.0</p>
    <p><small>For support, contact arvind.mehta@nlcindia.in</small></p>
</div>
""", unsafe_allow_html=True)
