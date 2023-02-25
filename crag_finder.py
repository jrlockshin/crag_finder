import pandas as pd
import numpy as np

"""finds top crags by route downloads
from csvs exported by mountainproject.com.
"""

# dictionary of rating categories w/ranking
RATINGS = {
    "5.7-5.9": 3,
    "5.10a-d": 4,
    "5.11a-d": 2,
    "5.12a-d": 1,
    "<= 5.6": 0
}

# name of area
AREA = 'clear_creek'

# file path to csv downloaded from mountainproject
CSV = f'{AREA}/route-finder.csv'


def filter_routes(sport_only=True, no_deaths=True):
    """filters routes by type and assigns rating categories."""
    def categorize_ratings(row):
        l = ['5.7', '5.8', '5.9']
        if any(i in row['Rating'] for i in l):
            cat = '5.7-5.9'
        elif '5.10' in row['Rating']:
            cat = '5.10a-d'
        elif '5.11' in row['Rating']:
            cat = '5.11a-d'
        elif '5.12' in row['Rating']:
            cat = '5.12a-d'
        else:
            cat = None
            
        if no_deaths:
            if 'R' in row['Rating'] or 'X' in row['Rating']:
                cat = None
        
        row['rating_category'] = cat    
        
        return row
    
    
    df = pd.read_csv(CSV)
    
    if sport_only:
        r_types = ['Sport', 'Sport, TR']
        df = df[df["Route Type"].isin(r_types)]
    
    df = df.apply(categorize_ratings, axis=1)
    df = df[~df['rating_category'].isna()]
    
    return df


def groupby_crag(df):
    """groups all routes by crag."""
    crag_dict = {
        "Route": ["count"],
        "Avg Stars": ["mean"]
    }

    by_crag = df.groupby(["Location"]).agg(crag_dict)
    by_crag.columns = ['total_routes', 'avg_stars_crag']
    by_crag.reset_index(drop=False, inplace=True)
    by_crag.rename(columns={'Location': 'crag'}, inplace=True)
    
    by_crag['total_routes'] = by_crag['total_routes'].astype(int)
    by_crag['avg_stars_crag'] = by_crag['avg_stars_crag'].apply(
        lambda x: round(x, 2))
    
    return by_crag


def groupby_crag_rating(df):
    """groups all routes by crag and rating category."""
    grp_by = [
        "Location",
        "rating_category"
    ]

    agg_dict = {
        "Route": ["count"],
        "Avg Stars": ["mean"]
    }

    by_rating = df.groupby(grp_by).agg(agg_dict)
    by_rating.columns = ['num_routes', 'avg_stars']
    by_rating.reset_index(drop=False, inplace=True)
    by_rating.rename(columns={'Location': 'crag'}, inplace=True)
    
    by_rating['num_routes'] = by_rating['num_routes'].astype(int)
    by_rating['avg_stars'] = by_rating['avg_stars'].apply(
        lambda x: round(x, 2))
    
    return by_rating


def routes_by_crag(df):
    """aggregates all routes by crag and rating category."""
    by_crag = groupby_crag(df)
    by_crag_rating = groupby_crag_rating(df)
    all_df = by_crag.merge(by_crag_rating, on='crag', how='left')

    all_df['pct_of_routes'] = all_df['num_routes'] / all_df['total_routes']
    all_df['pct_of_routes'] = all_df['pct_of_routes'] * 100
    all_df['pct_of_routes'] = all_df['pct_of_routes'].apply(
        lambda x: round(x, 2))

    all_df = all_df.sort_values([
        'crag',
        'num_routes',
        'pct_of_routes',
        'avg_stars'],
        ascending=False
    )
    
    return all_df


def main():
    """finds top crags by rating category, avg. stars, and rating diversity."""
    def top_crags_by_rating(rating):
        df = all_df[all_df["rating_category"] == rating]
        sbst_df = df[['crag', 'num_routes', 'avg_stars']]

        crag_sbst = by_crag[['crag']]
        crag_df = crag_sbst.merge(sbst_df, on='crag', how='left')

        n_col = f'num_routes_{rating}'
        s_col = f'avg_stars_{rating}'

        crag_df.rename(
            columns={'num_routes': n_col, 'avg_stars': s_col}, 
            inplace=True
        )
        crag_df.set_index('crag', inplace=True)

        return crag_df
    
    
    df = filter_routes()
    all_df = routes_by_crag(df)
    by_crag = groupby_crag(df)

    by_crag_copy = by_crag.set_index('crag')
    dfs = [by_crag_copy]
    
    # find top crags by rating from rating dict
    sort_order = []
    
    n = max([i for i in RATINGS.values()])
    for j in range(n, 0, -1):
        rating = [i for i in RATINGS if RATINGS[i] == j][0]
        df = top_crags_by_rating(rating)
        dfs.append(df)
        
        sort_order.append(f'num_routes_{rating}')
        sort_order.append(f'avg_stars_{rating}')
    
    top_df = pd.concat(dfs, axis=1)
    top_df = top_df.sort_values(sort_order, ascending=False)
    
    # find crags with most rating diversity
    var_df = top_df
    for rating in RATINGS:
        if RATINGS[rating] != 0:
            var_df = var_df[~var_df[f'num_routes_{rating}'].isna()]

    # export
    top_df.to_csv(f'{AREA}/{AREA}_top_crags_all.csv', index=True)
    var_df.to_csv(f'{AREA}/{AREA}_top_crags_diverse.csv', index=True)


if __name__ == "__main__":
    main()
