import pandas as pd


def get_tranche_df(df, rating, placeholder=""):
    df_filtered = df[df['Tranche Rating'] == rating]
    if df_filtered.empty:
        # Create placeholder row with all columns from df and values as `placeholder`
        df_filtered = pd.DataFrame([{col: placeholder for col in df.columns}])
    else:
        # Sort by "Security Description" alphabetically
        df_filtered = df_filtered.sort_values(by="Security Description", ascending=True)
    return df_filtered