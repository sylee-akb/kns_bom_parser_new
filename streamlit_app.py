import streamlit as st
import io
import warnings
warnings.simplefilter(action='ignore')
import pandas as pd
from datetime import datetime,timezone
import numpy as np
# from natsort import index_natsorted #for version sorting of hierarchical numbers
import requests, msal
import json
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

tenant_id = st.secrets['bc_tenant_id']
environment = 'Production'
client_id = st.secrets['bc_client_id']
client_secret = st.secrets['bc_client_secret']

def parse_oracle_bom(bom_file_obj):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        bom_df = pd.read_excel(bom_file_obj,sheet_name=0,engine="openpyxl",skiprows=0,usecols=[*range(0, 20, 1)] ,converters={
            'BOM_LEVEL':str,
            'ITEM':str,
            'MANUFACTURING_ITEM':str,
            'QTY':float,
            'POS':str,
        })
    bom_df = bom_df.dropna(subset=['BOM_LEVEL'])
    bom_df['System No.'] = np.nan
    bom_df['Drawing Reference'] = ''
    bom_df['Unit Cost [SGD]'] = np.nan
    bom_df['Total Cost [SGD]'] = np.nan
    bom_df['WIP or Released'] = 'WIP'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'Hierarchical No.'] = '1'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'Obsolete'] = 'N'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'QTY'] = 1.0
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'UOM'] = 'PCS'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'Parent'] = 'None'

    # Populate hierarchical number
    for i in bom_df.index:
        bom_df = populate_hier_num(bom_df,i)

    # Determine parent
    bom_df['Parent'] = bom_df['Hierarchical No.'].apply(lambda s: '.'.join(s.split('.')[:-1]))
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'Parent'] = 'None'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'System No.'] = 'CMMKNS'
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ', 'ITEM']

    # Formulate part number (description 1)
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'ITEM'] + 'REV' + bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'REV']
    bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'MANUFACTURER_PART_NUMBER']

    # Get Item Master from BC
    item_master_df = get_item_master(tenant_id,environment,client_id,client_secret)
    def match_sys_no_description(description):
        '''
        Finds system number of a given BOM line given its description (aka MPN)
        Returns str System Number if uniquely matched
        Returns None otherwise (not found or non-unique match)
        '''
        description_matches_df = item_master_df[item_master_df['Description'] == description].reset_index(drop=True)
        
        if len(description_matches_df) == 1: #uniquely matched
            return description_matches_df.at[0,'System No.']
        else: #non-unique match
            return None

    # Assign best-guess manufacturer
    bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'Manufacturer'] = bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'MANUFACTURER_NAME']
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ','Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('ASSY') ,'Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.contains('CABLE COMPLEMENT') ,'Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('CBL_') ,'Manufacturer'] = 'AKRIBIS CABLING'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('TERM_') ,'Manufacturer'] = 'AKRIBIS CABLING'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['Hierarchical No.'].apply(lambda s: not (s in set(bom_df['Parent']))),'Manufacturer'] = 'AKRIBIS FAB'

    # Match system number
    bom_df['System No.'] = bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(match_sys_no_description)

    # Fill unit costs for known items
    bom_df.loc[bom_df['System No.'].isin(item_master_df['System No.'].unique()),'Unit Cost [SGD]'] = bom_df.loc[bom_df['System No.'].isin(item_master_df['System No.'].unique()),'System No.'].apply(lambda s: item_master_df.loc[item_master_df['System No.']==s,'Unit Cost'].iloc[0]) 

    # Assign system number categories
    bom_df.loc[bom_df['System No.'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('ASSY') ,'System No.'] = 'SASSSM'
    bom_df.loc[bom_df['System No.'].isna() & bom_df['ITEM_DESCRIPTION'].str.contains('CABLE COMPLEMENT') ,'System No.'] = 'SASSSM'
    bom_df.loc[bom_df['System No.'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('CBL_') ,'System No.'] = 'AACACW'
    bom_df.loc[bom_df['System No.'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('TERM_') ,'System No.'] = 'AACACW'
    bom_df.loc[bom_df['System No.'].isna() & (bom_df['Manufacturer'] == 'AKRIBIS FAB'),'System No.'] = 'FAB'
    bom_df.loc[bom_df['System No.'].isna() & (bom_df['ITEM_DESCRIPTION'].str.startswith('WIRE')),'System No.'] = 'EEPCNW'
    bom_df.loc[bom_df['System No.'].isna() & (bom_df['ITEM_DESCRIPTION'].str.startswith('SCREW')),'System No.'] = 'MEPFSC'
    bom_df.loc[bom_df['System No.'].isna() & (bom_df['ITEM_DESCRIPTION'].str.startswith('WASHER')),'System No.'] = 'MEPFSC'
    
    # Convert UOM
    bom_df['UOM'] = bom_df['UOM'].str.upper()
    bom_df.loc[bom_df['UOM'] == 'EACH','UOM'] = 'PCS'
    bom_df.loc[bom_df['UOM'] == 'MILLIMETER','UOM'] = 'MM'
    bom_df.loc[bom_df['UOM'] == 'FEET','QTY'] = (bom_df.loc[bom_df['UOM'] == 'FEET','QTY'] * 304.8).apply(lambda f: np.round(f,decimals=2))
    bom_df.loc[bom_df['UOM'] == 'FEET','UOM'] = 'MM'
    bom_df.loc[bom_df['UOM'] == 'INCHES','QTY'] = (bom_df.loc[bom_df['UOM'] == 'INCHES','QTY'] * 25.4).apply(lambda f: np.round(f,decimals=2))
    bom_df.loc[bom_df['UOM'] == 'INCHES','UOM'] = 'MM'
    
    # Rename column headers
    bom_df['Description 2\n(Description / Dwg Title)'] = bom_df['ITEM_DESCRIPTION']
    bom_df['Qty'] = bom_df['QTY']

    bom_df = bom_df[['Hierarchical No.','System No.','Description\n(Order Part No / Dwg No / REV No.)','Description 2\n(Description / Dwg Title)','Qty','UOM','Unit Cost [SGD]','Total Cost [SGD]','Manufacturer','Drawing Reference','WIP or Released','Obsolete','Parent']]
    return bom_df

def populate_hier_num(bom_df,i):
    cur_bom_df = bom_df.copy(deep=True)
    # print('Index %d' % i)
    if not pd.isna(cur_bom_df.at[i,'Hierarchical No.']): #do nothing if hier num already exists
        # print('skipped')
        return cur_bom_df
    parent_hier_num_list = cur_bom_df.loc[cur_bom_df['ITEM']==cur_bom_df.at[i,'MANUFACTURING_ITEM'],'Hierarchical No.']
    # print(parent_hier_num_list)
    if len(parent_hier_num_list) > 1:
        raise ValueError('Duplicate parent found')
    if len(parent_hier_num_list) < 1:
        raise ValueError('Parent not found.')
    parent_hier_num = parent_hier_num_list.iloc[0]
    if pd.isna(parent_hier_num): #recursively assign hierarchical number for the parent if it does not yet exist
        # print('recursion')
        cur_bom_df = populate_hier_num(cur_bom_df,bom_df.index(cur_bom_df['ITEM'] == cur_bom_df.iloc[i]['MANUFACTURING_ITEM']).iloc[0])
        parent_hier_num = cur_bom_df.loc[cur_bom_df['ITEM']==cur_bom_df.iloc[i]['MANUFACTURING_ITEM'],'Hierarchical No.'].iloc[0]
    siblings_hier_nums = cur_bom_df.loc[cur_bom_df['Hierarchical No.'].str.startswith(parent_hier_num+'.') &
                                        (~cur_bom_df['Hierarchical No.'].isna()) &
                                        (cur_bom_df['BOM_LEVEL']==cur_bom_df.at[i,'BOM_LEVEL']) &
                                        (~(cur_bom_df['Obsolete']=='Y'))
                                        ,'Hierarchical No.']
    siblings_hier_nums = siblings_hier_nums.apply(lambda s: int(s.split(parent_hier_num+'.')[1]))
    if len(siblings_hier_nums) == 0:
        current_item_hier_num = parent_hier_num + '.1'
        cur_bom_df.at[i,'Obsolete'] = 'N'
    else:
        duplicate_siblings_positions = cur_bom_df.loc[cur_bom_df['Hierarchical No.'].str.startswith(parent_hier_num+'.') &
                                            (~cur_bom_df['Hierarchical No.'].isna()) &
                                            (cur_bom_df['BOM_LEVEL']==cur_bom_df.at[i,'BOM_LEVEL']) &
                                            (~(cur_bom_df['Obsolete']=='Y')) &
                                            ((cur_bom_df['POS']==cur_bom_df.at[i,'POS']))
                                            ,'POS']
        # print(duplicate_siblings_positions)
        if len(duplicate_siblings_positions) > 0:
            cur_bom_df.at[i,'Obsolete'] = 'Y'
            current_item_hier_num = parent_hier_num + '.' + str(siblings_hier_nums.max())
        else:
            cur_bom_df.at[i,'Obsolete'] = 'N'
            current_item_hier_num = parent_hier_num + '.' + str(siblings_hier_nums.max()+1)
    
    # print('Parent Hier Num: %s' % parent_hier_num)
    # print('Siblings Hier Num: %s' % siblings_hier_nums)
    # print('Current Hier Num: %s' % current_item_hier_num)

    #TO-DO: Deal with alternate parts in Format D BOM

    
    cur_bom_df.at[i,'Hierarchical No.'] = current_item_hier_num
    return cur_bom_df

def parse_bom():
    if st.session_state.bom_file is None:
        st.session_state.upload_state = "Upload a file first!"
    else:
        st.session_state.bom_df = parse_oracle_bom(st.session_state.bom_file)
        st.session_state.upload_state = "BOM parsed successfully!"

def output_bom():
    if st.session_state.bom_df is None:
        st.session_state["upload_state"] = "Upload a file first!"
    else:
        with pd.ExcelWriter(st.session_state.output_bom_file,engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format(
            {
                "bold": True,
                "text_wrap": True,
                "valign": "top",
                "border": 1,
            }
        )
            st.session_state.bom_df.to_excel(writer, sheet_name='System BOM')
            (max_row, max_col) = st.session_state.bom_df.shape
            writer.sheets['System BOM'].autofilter(0, 0, max_row, max_col)

def getToken(tenant, client_id, client_secret):
    authority = "https://login.microsoftonline.com/" + tenant
    scope = ["https://api.businesscentral.dynamics.com/.default"]

    app = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential = client_secret)
    
    try:
      accessToken = app.acquire_token_for_client(scopes=scope)
      if accessToken['access_token']:
        print('New access token retreived....')
      else:
        print('Error aquiring authorization token.')
    except Exception as err:
      print(err)
    
    return accessToken

def retrieve_all_records(session, request_url, request_header):
    all_records = []
    url = request_url
    i = 1
    while True:
        if not url:
            break
        response = session.get(url, headers = request_header)
        if response.status_code == 200:
            json_data = json.loads(response.text)
            all_records = all_records + json_data['value']
            if '@odata.nextLink' in json_data.keys():
                url = json_data['@odata.nextLink']
            else:
                url = None
        else:
            raise ValueError('Status Code %s' % response.status_code)
        i += 1

    return all_records

@st.cache_data
def get_item_master(tenant_id,environment,client_id,client_secret):
    # define the retry strategy
    retry_strategy = Retry(
        total=4,  # maximum number of retries
        status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on
    )

    # create an HTTP adapter with the retry strategy and mount it to the session
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # create a new session object
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Fetch the token as json object
    reqToken = getToken(tenant_id, client_id, client_secret)

    # Build the request URL
    request_url = f"https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/{environment}/api/v2.0/items?company=Akribis%20Systems%20Pte%20Ltd&$select=number,displayName,baseUnitOfMeasureCode,unitCost"

    # Build the request Headers
    reqHeaders =  {"Accept-Language": "en-us", "Authorization": f"Bearer {reqToken['access_token']}", 'Prefer': 'odata.maxpagesize=10000'}
        
    # Fetch the data
    # response = requests.get(url_companies, headers=reqHeaders)

    response = retrieve_all_records(session, request_url, reqHeaders)

    item_master_df = pd.DataFrame(response)
    item_master_df = item_master_df.rename(columns={'number':'System No.',
                           'displayName':'Description',
                           'baseUnitOfMeasureCode':'UOM',
                           'unitCost':'Unit Cost'})
    item_master_df['UOM'] = item_master_df['UOM'].astype('category')
    item_master_df['Unit Cost'] = item_master_df['Unit Cost'].astype('float16')
    item_master_df['Description'] = item_master_df['Description'].str.strip()
    item_master_df['Description'] = item_master_df['Description'].str.upper()
    item_master_df = item_master_df[['System No.','Description','UOM','Unit Cost']]
    
    return item_master_df

# Streamlit session state declarations
if 'session_state' not in st.session_state:
    st.session_state.upload_state = 'Pending file upload'

if 'bom_df' not in st.session_state:
    st.session_state.bom_df = None

if 'output_bom_df' not in st.session_state:
    st.session_state.output_bom_df = None

if 'output_bom_file' not in st.session_state:
    st.session_state.output_bom_file = io.StringIO()

# GUI elements
st.title("KNS BOM Parser")
st.file_uploader('Upload source BOM:', key = 'bom_file',type='xlsx', accept_multiple_files=False, on_change = parse_bom,label_visibility="visible")

if type(st.session_state.bom_df) == pd.core.frame.DataFrame:
    st.dataframe(data=st.session_state.bom_df.style.highlight_null(color='pink',subset=['System No.','Manufacturer']))

st.markdown('''
## To-do: 
- Download as XLSX (instead of CSV)
- ~~Convert KNS UOM to AKB UOM~~
- ~~Search BC item master using description 1, and autofill sys num~~
- Match expendables items and assign service number
- Flag lines with inconsistent UOM for manual attention
- Highlight lines needing attention
    - ~~Unable to identify what item category~~
    - ~~Unable to identify correct manufacturer~~
- Function to extract sub-BOM for specific items
- Interface to manually enter list + price for KNS quoted AVL parts (e.g. Pecko, Hisaka), and update parsed BOM accordingly
- Interface to upload markup BOM (ECO pre-warning) and generate updated BOM
    - Feature to highlight line items that are affected by upcoming ECO
        - Reduction of volume
        - Addition of volume
        - Reworkability
- Feature to diff two versions of Format D BOM
            ''')