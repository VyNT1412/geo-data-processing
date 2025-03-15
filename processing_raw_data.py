import pandas as pd
from langdetect import detect, DetectorFactory
from langdetect.lang_detect_exception import LangDetectException
import random
import re
DetectorFactory.seed = 0  

# load raw data
def load_excel(file_path):
    return pd.concat(pd.read_excel(file_path,sheet_name=None).values(),ignore_index=0)

# get columns are used 
def get_need_columns(df, *column_names):
    existing_columns = [col for col in column_names if col in df.columns]
    return df[existing_columns] if existing_columns else pd.DataFrame()

# merged files
def contact_df(*dfs):
    new_df = pd.DataFrame(columns=["address_id", "raw_address"])  # Tạo DataFrame rỗng
    processed_dfs = []
    for df in dfs:
        df = df.rename(columns={df.columns[0]: "address_id", df.columns[1]: "raw_address"})  
        processed_dfs.append(df)
    
    return pd.concat(processed_dfs, ignore_index=True)

#procsessing on column "raw_address"
def remove_null_values(df):
    return df.dropna(subset=["raw_address"])

def remove_duplicated_values(df):
    return df.drop_duplicates(subset=["raw_address"])

def remove_not_valid_address(df):
    # remove too short values
    df = df[df["raw_address"].str.len()>12]
    # remove values contain "?"
    df = df[~df["raw_address"].str.contains(r"\?", na=False)]
    # not in not in Vietnam
    def check_vietname_address(address):
        try:
            return detect(address) == "vi"
        except LangDetectException:
            return False
    df = df[df["raw_address"].apply(check_vietname_address)]
    return df
    
#processing on columns "address_id"
def is_valid_id(values_id):
    if isinstance(values_id,int):
        return True
    if isinstance(values_id, str) and values_id.isdigit():
        return True
    return False
    
def check_address_id(df):
    df = df.copy()
    invalid_rows = df[~df["address_id"].astype(str).apply(is_valid_id)]
    return invalid_rows   

def change_address_id(df):
    invalid_rows = check_address_id(df) # các hàng có address_id lỗi
    df = df.loc[~df.index.isin(invalid_rows)]
    def generate_unique_id(existing_ids):
        while True:
            new_id = str(random.randint(10**8, 10**9 - 1))  # Tạo số nguyên 10 chữ số
            if new_id not in existing_ids:
                return new_id
    def fix_id(id_value, existing_ids):
        if pd.isna(id_value):  # Nếu là NaN, tạo ID mới
            new_id = generate_unique_id(existing_ids)
            existing_ids.add(new_id)
            return new_id
        
        id_value = str(id_value)  # Chuyển thành chuỗi
        id_value = "".join(filter(str.isalnum, id_value)) 
        
        if id_value.replace(".", "", 1).isdigit():  
            id_value = str(int(float(id_value)))  
        
        if re.search(r"[A-Za-z]", id_value):  # Nếu có chữ, tạo ID mới
            id_value = generate_unique_id(existing_ids)
            existing_ids.add(id_value)
        
        return id_value

    existing_ids = set(df["address_id"].dropna().astype(str))  # Lấy danh sách ID hiện có
    invalid_rows["address_id"] = invalid_rows["address_id"].apply(lambda id_address: fix_id(id_address, existing_ids))

    df.update(invalid_rows)  # Cập nhật lại các hàng bị lỗi với ID mới
    return df

def run_pipeline():
    # load raw files
    customer_df = load_excel("./raw_data/Khach_hang_Doi_tac.xlsx")
    locations_df = load_excel("./raw_data/LocationId.xlsx") 
    supplier_df = load_excel("./raw_data/Nha_cung_cap.xlsx") 

    # get all columns need
    customer_df = get_need_columns(customer_df,"CUST_CODE","CUST_ADDR")
    locations_df = get_need_columns(locations_df,"LS_ACC_FLEX_01","LS_ACC_FLEX_01_DESC")
    supplier_df = get_need_columns(supplier_df,	"ADDR_CODE","ADDR_LINE_1")

    # merged data
    merged_df = contact_df(customer_df, locations_df, supplier_df)

    # processing data 
    processed_df = remove_null_values(merged_df)
    processed_df = remove_duplicated_values(processed_df)
    processed_df = remove_not_valid_address(processed_df)
    processed_df = change_address_id(processed_df)

    return processed_df

if __name__ == "__main__":
    final_df = run_pipeline()
    final_df.to_csv("processed_data.csv", index=False)
    print("Pipeline completed. Processed data saved to processed_data.csv.")







