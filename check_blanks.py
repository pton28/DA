import pandas as pd
std = pd.read_csv('d:/BK_Document/251/DoAn/New/DA/data/Golden/standardized/std_store_performance.csv')
print(f'Blank dates: {std["sale_date"].isna().sum()} out of {len(std)}')
