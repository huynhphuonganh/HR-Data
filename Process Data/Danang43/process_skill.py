
import pandas as pd

def clean_skill_name(name: str) -> str:
    if pd.isna(name):
        return name  
    
    # Tách phần sau dấu ':'
    if ':' in name:
        name = name.split(':', 1)[1] 

    # Xử lý khoảng trắng thừa
    name = ' '.join(name.split())
    
    # Viết hoa chữ cái đầu tiên
    name = name.capitalize()
    
    return name

def process_skill(df: pd.DataFrame) -> pd.DataFrame:
    if 'name' in df.columns:
        df['name'] = df['name'].apply(clean_skill_name)
    return df


