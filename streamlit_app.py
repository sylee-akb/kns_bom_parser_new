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
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential
from office365.runtime.auth.authentication_context import AuthenticationContext
import zipfile
import re

tenant_id = st.secrets['bc_tenant_id']
environment = 'Production'
client_id = st.secrets['bc_client_id']
client_secret = st.secrets['bc_client_secret']
ctb_sharepoint_site_url = st.secrets['ctb_sharepoint_site_url']
ctb_sharepoint_username = st.secrets['ctb_sharepoint_username']
ctb_sharepoint_password = st.secrets['ctb_sharepoint_password']
expendables_list_relative_path = 'Shared Documents/Stage Non-Inventorized Expendables List.xlsx'

def parse_oracle_bom(bom_file_obj):
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        bom_df = pd.read_excel(bom_file_obj,sheet_name=0,engine="openpyxl",skiprows=0,usecols=None ,converters={
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
    bom_df['REV'] = bom_df['REV'].apply(lambda s: ''.join(filter(str.isalpha,s)))

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
    bom_df.loc[bom_df['MANUFACTURER_NAME'] == 'OPTIONAL','MANUFACTURER_NAME'] = None
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'ITEM'] + 'REV' + bom_df.loc[bom_df['MANUFACTURER_NAME'].isna(),'REV']
    bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna() & ~bom_df['MANUFACTURER_PART_NUMBER'].isin(['N.A.','N.A','N/A','-']),'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna() & ~bom_df['MANUFACTURER_PART_NUMBER'].isin(['N.A.','N.A','N/A','-']),'MANUFACTURER_PART_NUMBER'].astype('str')
    bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna() & bom_df['MANUFACTURER_PART_NUMBER'].isin(['N.A.','N.A','N/A','-']),'Description\n(Order Part No / Dwg No / REV No.)'] = bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna() & bom_df['MANUFACTURER_PART_NUMBER'].isin(['N.A.','N.A','N/A','-']),'ITEM'] + 'REV' + bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna() & bom_df['MANUFACTURER_PART_NUMBER'].isin(['N.A.','N.A','N/A','-']),'REV']

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

    # Get Expendables List from Sharepoint
    expendables_df = get_expendables_list(ctb_sharepoint_site_url,expendables_list_relative_path,client_id,client_secret)

    # Assign best-guess manufacturer
    bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'Manufacturer'] = bom_df.loc[~bom_df['MANUFACTURER_NAME'].isna(),'MANUFACTURER_NAME']
    bom_df.loc[bom_df['BOM_LEVEL']=='TOP MODEL : ','Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('ASSY') ,'Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.contains('CABLE COMPLEMENT') ,'Manufacturer'] = 'AKRIBIS ASSY'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('CBL_') ,'Manufacturer'] = 'AKRIBIS CABLING'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['ITEM_DESCRIPTION'].str.startswith('TERM_') ,'Manufacturer'] = 'AKRIBIS CABLING'
    bom_df.loc[bom_df['MANUFACTURER_NAME'].isna() & bom_df['Description\n(Order Part No / Dwg No / REV No.)'].str.startswith('0') & bom_df['Hierarchical No.'].apply(lambda s: not (s in set(bom_df['Parent']))),'Manufacturer'] = 'AKRIBIS FAB'

    # Match system number
    bom_df['System No.'] = bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(match_sys_no_description)

    # Identify non-inventorized expendables
    bom_df.loc[bom_df['System No.'].isna() & bom_df['ITEM_DESCRIPTION'].isin(expendables_df['Description'].unique()),'System No.'] = 'SVCSEI0A0001'

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
    if len(parent_hier_num_list) < 1:
        raise ValueError('Parent not found: ' + cur_bom_df.at[i,'ITEM'])
    elif len(parent_hier_num_list) > 1:
        parent_hier_num = parent_hier_num_list.loc[parent_hier_num_list.index[parent_hier_num_list.index<i].max()]
        # raise ValueError('Duplicate parent found: '+ cur_bom_df.at[i,'ITEM'])
    else:
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

def parse_input_bom():
    if st.session_state.bom_file is None:
        st.session_state.upload_state = "Upload a file first!"
    else:
        st.session_state.bom_df = parse_oracle_bom(st.session_state.bom_file)
        st.session_state.upload_state = "BOM parsed successfully!"

def filename_to_partno(s):
    try:
        x = re.search(r"((?:\d|X|\.){5})(?:-|_)((?:\d|X|\.){4})(?:-|_)((?:\d|X|\.){3})(?:-|_|__|--|\s+)?((?:\d|X|\.){2})(?:-|_|__|--|\s+)?(?:REV)?(?:-|_|__|--|\s+)?([A-Z][A-Z]?)(?:-|_|__|--|\s+|\Z)?", s)
        if x == None:
            x = re.search(r"((?:\d|X|\.){5})(?:-|_)((?:\d|X|\.){4})(?:-|_)((?:\d|X|\.){3})(?:-|_|__|--|\s+)?(?:-|_|__|--|\s+)?(?:REV)?(?:-|_|__|--|\s+)?([A-Z][A-Z]?)(?:-|_|__|--|\s+|\Z)?", s)
            part_no = x.groups()[0].replace('X','.') + '-' + x.groups()[1].replace('X','.') +  '-' + x.groups()[2].replace('X','.') + '-' + '..' + 'REV' + x.groups()[3]
        else: # final 2-digit field matched
            part_no = x.groups()[0].replace('X','.') + '-' + x.groups()[1].replace('X','.') +  '-' + x.groups()[2].replace('X','.') + '-' + x.groups()[3].replace('X','.') + 'REV' + x.groups()[4]
        return part_no
    except Exception as err:
        return None

def parse_dwg_zip():
    if st.session_state.input_dwg_zip_file is None:
        print('Invalid dwg zip file')
    else:
        with zipfile.ZipFile(st.session_state.input_dwg_zip_file, 'r') as zf:
            file_namelist = zf.namelist()

        zip_df = pd.DataFrame(data  = file_namelist, columns =['File Name'])

        zip_df['Cleaned File Name'] = zip_df['File Name'].str.upper()
        zip_df['File Type'] = zip_df['Cleaned File Name'].apply(lambda s: s.split('.')[-1])
        zip_df['Cleaned File Name'] = zip_df['Cleaned File Name'].apply(lambda s: ''.join(s.split('.')[:-1]))
        zip_df['Drawing No.'] = zip_df['Cleaned File Name'].apply(filename_to_partno)
        # zip_df.loc[~zip_df['Drawing No.'].isna(),'Revision'] = zip_df.loc[~zip_df['Drawing No.'].isna(),'Drawing No.'].apply(lambda s: s.split('REV')[-1])
        # zip_df.loc[~zip_df['Drawing No.'].isna(),'Drawing No.'] = zip_df.loc[~zip_df['Drawing No.'].isna(),'Drawing No.'].apply(lambda s: s.split('REV')[0])

        zip_df.loc[zip_df['File Type']=='STP','File Type'] = 'STEP'

        st.session_state.zip_df = zip_df
        
        st.session_state.upload_state = "BOM parsed successfully!"
    return 0

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

def parse_markup_bom():
    '''
    Parses the markup BOM and returns a dataframe containing all linewise changes to be applied
    Changes can be: change, add, delete only
    Items to change are identified by a combination of part number + parent part number
    '''

def apply_markup_changes():
    '''
    Applies markout changes in markup_changes_df to bom_df, and returns a new dataframe with the changes
    '''

    return 0

def diff_input_boms():
    '''
    Compares input BOM 1 (with markup changes applied, if any) with input BOM 2
    and returns a dataframe showing the differences in a flattened way (material exposure only)
    '''

    return 0

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
    request_url = f"https://api.businesscentral.dynamics.com/v2.0/{tenant_id}/{environment}/api/v2.0/items?company=Akribis%20Systems%20Pte%20Ltd&$select=number,displayName,baseUnitOfMeasureCode,unitCost,type&$filter=type eq 'Inventory'"

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

@st.cache_data
def get_expendables_list(site_url,expendables_file_rel_path,client_id, client_secret):  
    ctx_auth = AuthenticationContext(site_url)
    if ctx_auth.acquire_token_for_user(ctb_sharepoint_username, ctb_sharepoint_password):
      context = ClientContext(site_url, ctx_auth)
    
    # context = ClientContext(site_url).with_credentials(
    #                    ClientCredential(client_id, client_secret)
    #                                )
    
    #download expendables_list to in-memory file
    input_file = io.BytesIO(
        context.web.get_file_by_server_relative_path(expendables_file_rel_path).get_content().execute_query().value
        )

    # filename = 'expendables_list.xlsx'
    # with open(filename, 'wb') as output_file:
    #     if verbose:
    #          print('Downloading Expendables List...')
    #     file = (
    #         ctx.web.get_file_by_server_relative_url(expendables_list_relative_path)
    #         .download(output_file)
    #         .execute_query()
    #     )
    #     if verbose:
    #         print('Expendables List downloaded.')
            
    #read expendables list into DataFrame
    expendables_df = pd.read_excel(input_file,sheet_name='Sheet1',engine="openpyxl",skiprows=0,usecols='A:D',converters={
        'Description':str,
        'Description 2':str,
        'Manufacturer':str,
        'Status':str}
                                          )
    expendables_df = expendables_df[expendables_df['Status'] == 'Released']
    expendables_df['Description'] = expendables_df['Description'].str.strip()
    expendables_df['Description'] = expendables_df['Description'].str.upper()
            
    return expendables_df 

def bom_file_check(part_number,file_type):
    '''
    Returns number of files matching file type and drawing no. for a particular part number.
    '''
    try:    
        return len(st.session_state.zip_df[(st.session_state.zip_df['Drawing No.'].apply(
            lambda r: False if type(r) != str else re.search(r'\A' + r,part_number) != None)) & (st.session_state.zip_df['File Type']==file_type)])
    except Exception as err:
        print('Error on: ' + str(part_number))
        return -1

def rename_file_to_new_zip(original_filename, zip_df, input_zip_file_obj, output_zip_file_obj):
    try:
        new_filename = zip_df.loc[zip_df['File Name'] == original_filename, 'Drawing No.'].iloc[0]
        new_extension = zip_df.loc[zip_df['File Name'] == original_filename, 'File Type'].iloc[0]
        new_filename = new_filename + '.' + new_extension
    except Exception as err:
        print('Problem finding original file: ' + str(original_filename))

    
    if not type(new_filename) == str:
        print(original_filename + ' not saved. Missing dwg number.')
        return 1
    
    file_contents = input_zip_file_obj.read(original_filename)
    output_zip_file_obj.writestr(zinfo_or_arcname = new_filename, data = file_contents)
    return 0

def update_zip_df():
    '''
    Updates zip_df to apply manual edits made by user through data_editor interface.
    If bom_df is a dataframe, also calls the idempotent bom_file_check to match drawings to BOM lines
    It also calls 
    '''
    if type(st.session_state.zip_df) == pd.core.frame.DataFrame:
        st.session_state.zip_df = modified_zip_df
        st.session_state.output_dwg_zip_file = io.BytesIO()
        with zipfile.ZipFile(st.session_state.output_dwg_zip_file, 'w') as out_zf:
            with zipfile.ZipFile(st.session_state.input_dwg_zip_file, 'r') as in_zf:
                st.session_state.zip_df['File Name'].apply(rename_file_to_new_zip,args=(st.session_state.zip_df,in_zf,out_zf))
        if type(st.session_state.bom_df) == pd.core.frame.DataFrame:
            st.session_state.bom_df['Drawing?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'PDF') > 0 else False)
            st.session_state.bom_df['STEP?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'STEP') > 0 else False)
            st.session_state.bom_df['Drawing Duplicated?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'PDF') > 1 else False)
            st.session_state.bom_df['STEP Duplicated?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'STEP') > 1 else False)

def update_bom_df():
    '''
    Updates bom_df to apply manual edits made by user through data_editor interface.
    If bom_df is a dataframe, also calls the idempotent bom_file_check to match drawings to BOM lines
    It also calls 
    '''
    if type(st.session_state.bom_df) == pd.core.frame.DataFrame:
        st.session_state.bom_df = modified_bom_df
 
        if type(st.session_state.zip_df) == pd.core.frame.DataFrame:
            st.session_state.bom_df['Drawing?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'PDF') > 0 else False)
            st.session_state.bom_df['STEP?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'STEP') > 0 else False)
            st.session_state.bom_df['Drawing Duplicated?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'PDF') > 1 else False)
            st.session_state.bom_df['STEP Duplicated?'] = st.session_state.bom_df['Description\n(Order Part No / Dwg No / REV No.)'].apply(lambda s: True if bom_file_check(s,'STEP') > 1 else False)           
            

# Streamlit session state declarations
if 'session_state' not in st.session_state:
    st.session_state.upload_state = 'Pending file upload'

if 'bom_df' not in st.session_state:
    st.session_state.bom_df = None

if 'output_bom_df' not in st.session_state:
    st.session_state.output_bom_df = None

if 'output_bom_file' not in st.session_state:
    st.session_state.output_bom_file = io.StringIO()

if 'zip_df' not in st.session_state:
    st.session_state.zip_df = None

if 'output_dwg_zip_file' not in st.session_state:
    st.session_state.output_dwg_zip_file = None

# GUI elements
st.title("Input BOM")
st.file_uploader('Upload K&S source BOM:', key = 'bom_file',type='xlsx', accept_multiple_files=False, on_change = parse_input_bom,label_visibility="visible")
st.file_uploader('Upload drawings zip file:', key = 'input_dwg_zip_file',type='zip', accept_multiple_files=False, on_change = parse_dwg_zip,label_visibility="visible")
st.file_uploader('Upload markup BOM:', key = 'markup_bom_file',type='xlsx', accept_multiple_files=False, on_change = apply_markup_changes,label_visibility="visible")

st.title("Output BOM")
if type(st.session_state.bom_df) == pd.core.frame.DataFrame:
    st.button('Update BOM', on_click = update_bom_df)
    modified_bom_df = st.data_editor(data=st.session_state.bom_df)

st.title('Output Drawings List')
if type(st.session_state.zip_df) == pd.core.frame.DataFrame:
    st.button('Update Drawing List', on_click = update_zip_df)
    if st.session_state.output_dwg_zip_file != None:
        st.download_button('Download Cleaned Drawings', st.session_state.output_dwg_zip_file, file_name='output_dwgs.zip')
    modified_zip_df = st.data_editor(data=st.session_state.zip_df)
    


st.markdown('''
## To-do: 
- Download as XLSX (instead of CSV)
- ~~Convert KNS UOM to AKB UOM~~
- ~~Search BC item master using description 1, and autofill sys num~~
- ~~Match expendables items and assign service number~~
    - Switch over to using client id and secret to authenticate sharepoint instead of username and password
- Flag lines with inconsistent UOM for manual attention
- Highlight lines needing attention
    - ~~Unable to identify what item category~~
    - ~~Unable to identify correct manufacturer~~
- Function to extract sub-BOM for specific items
- Interface to manually enter list + price for KNS quoted AVL parts (e.g. Pecko, Hisaka), and update parsed BOM accordingly
- Interface to upload markup BOM (ECO pre-warning) and generate updated BOM
    - Feature to highlight line items that are affected by upcoming ECO
        - Reduction of qty (not safe to buy)
        - Addition of qty (safe to buy according to old qty)
        - Not affected (safe to buy)
        - Reworkability (safe to buy, but more complicated)
- Interface to upload zip file containing all drawings
    - ~~Match BOM line items to indicate which items have drawings~~
    - ~~Feature to download zip file with drawings and step files renamed in standard dwg number format~~
    - Feature to directly send drawings to MFG docs repository
- Feature to diff two versions of Format D BOM
            ''')