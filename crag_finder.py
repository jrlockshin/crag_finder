import pandas as pd
import numpy as np

"""finds top crags by route downloads
from csvs exported by mountainproject.com.
"""

# dictionary of rating categories w/ranking
RATINGS = {
    "5.7-5.9": 3,
    "5.10a-d": 2,
    "5.11a-d": 1,
    "5.12a-d": 0,
    "<= 5.6": 0
}

# name of area
AREA = 'red_rocks'

# file path to csv downloaded from mountainproject
CSV = 'route-finder.csv'


def filter_routes(sport_only=True, no_deaths=True):
    """filters routes by type and assigns rating categories."""
    
    df = pd.read_csv(CSV)
    
    rating_map = {
        "5.10": "5.10a-d",
        "5.10+": "5.10a-d",
        "5.10+ PG13": "5.10a-d",
        "5.10-": "5.10a-d",
        "5.10a": "5.10a-d",
        "5.10a PG13": "5.10a-d",
        "5.10a R": "5.10a-d",
        "5.10a/b": "5.10a-d",
        "5.10b": "5.10a-d",
        "5.10b PG13": "5.10a-d",
        "5.10b/c": "5.10a-d",
        "5.10c": "5.10a-d",
        "5.10c PG13": "5.10a-d",
        "5.10c R": "5.10a-d",
        "5.10c/d": "5.10a-d",
        "5.10d": "5.10a-d",
        "5.11": "5.11a-d",
        "5.11+": "5.11a-d",
        "5.11-": "5.11a-d",
        "5.11a": "5.11a-d",
        "5.11a PG13": "5.11a-d",
        "5.11a/b": "5.11a-d",
        "5.11a/b PG13": "5.11a-d",
        "5.11b": "5.11a-d",
        "5.11b PG13": "5.11a-d",
        "5.11b R": "5.11a-d",
        "5.11b/c": "5.11a-d",
        "5.11c": "5.11a-d",
        "5.11c PG13": "5.11a-d",
        "5.11c R": "5.11a-d",
        "5.11c/d": "5.11a-d",
        "5.11d": "5.11a-d",
        "5.12-": "5.12a-b",
        "5.12a": "5.12a-b",
        "5.12a PG13": "5.12a-b",
        "5.12a/b": "5.12a-b",
        "5.4": "<= 5.6",
        "5.5": "<= 5.6",
        "5.5 PG13": "<= 5.6",
        "5.5 R": "<= 5.6",
        "5.6": "<= 5.6",
        "5.6 R": "<= 5.6",
        "5.7": "5.7-5.9",
        "5.7 PG13": "5.7-5.9",
        "5.7+": "5.7-5.9",
        "5.7+ PG13": "5.7-5.9",
        "5.8": "5.7-5.9",
        "5.8 PG13": "5.7-5.9",
        "5.8 R": "5.7-5.9",
        "5.8+": "5.7-5.9",
        "5.8+ R": "5.7-5.9",
        "5.8-": "5.7-5.9",
        "5.9": "5.7-5.9",
        "5.9 PG13": "5.7-5.9",
        "5.9+": "5.7-5.9",
        "5.9+ PG13": "5.7-5.9",
        "5.9-": "5.7-5.9"
    }
    
    if sport_only:
        r_types = ['Sport', 'Sport, TR']
        df = df[df["Route Type"].isin(r_types)]
        
    if no_deaths:
        for rating in rating_map:
            if 'R' in rating or 'X' in rating:
                rating_map[rating] = None

    df['rating_category'] = df['Rating'].map(rating_map)
    df = df[~df['rating_category'].isna()]

    df['rating_int'] = df['rating_category'].map(RATINGS)
    df = df[~df['rating_int'].isna()]
    
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
        "rating_category",
        "rating_int"
    ]

    agg_dict = {
        "Route": ["count"],
        "Avg Stars": ["mean"]
    }

    by_crag_rating = df.groupby(grp_by).agg(agg_dict)
    by_crag_rating.columns = ['num_routes', 'avg_stars']
    by_crag_rating.reset_index(drop=False, inplace=True)
    by_crag_rating.rename(columns={'Location': 'crag'}, inplace=True)
    
    by_crag_rating['num_routes'] = by_crag_rating['num_routes'].astype(int)
    by_crag_rating['avg_stars'] = by_crag_rating['avg_stars'].apply(
        lambda x: round(x, 2))
    
    return by_crag_rating


def routes_by_crag(df):
    """aggregates all routes by crag and rating category."""
    
    by_crag = groupby_crag(df)
    by_crag_rating = groupby_crag_rating(df)

    all_df = by_crag.merge(by_crag_rating, on='crag', how='left')

    all_df['pct_of_routes'] = all_df['num_routes'] / all_df['total_routes']
    all_df['pct_of_routes'] = all_df['pct_of_routes'] * 100
    all_df['pct_of_routes'] = all_df['pct_of_routes'].apply(
        lambda x: round(x, 2))
    
    all_df['rating_int'] = all_df['rating_int'].astype(int)

    all_df = all_df.sort_values([
        'crag',
        'rating_int',
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
    for rating in RATINGS:
        if RATINGS[rating] != 0:
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
    top_df.to_csv(f'{AREA}_top_crags_all.csv', index=True)
    var_df.to_csv(f'{AREA}_top_crags_diverse.csv', index=True)


if __name__ == "__main__":
    main()
