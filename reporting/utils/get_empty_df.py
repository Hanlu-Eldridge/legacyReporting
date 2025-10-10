import pandas as pd

def get_empty_df(df, placeholder=""):
    if df.empty:
        # Create placeholder row with all columns from df and values as `placeholder`
        df_filtered = pd.DataFrame([{col: placeholder for col in df.columns}])
    else:
        # Sort by "Security Description" alphabetically
        df_filtered = df.sort_values(by="Security Description", ascending=True)
    return df_filtered