# ranking_engine.py
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Union

# --- THAM SỐ CẤU HÌNH ---
SCORE_WEIGHTS = {'Aspect_Score': 0.7, 'Rating_Score': 0.3}
MIN_REVIEW_THRESHOLD = 50 

# HÀM 1: TÍNH TOÁN (Lấy từ code của bạn)
def calculate_all_scores(group_df: pd.DataFrame, min_threshold: int) -> pd.Series:
    results = {}
    ASPECTS = ['Food', 'Place', 'Price']
    
    for aspect in ASPECTS:
        segments = group_df[aspect].astype(str).str.split(' | ')
        all_opinions = [item.strip() for sublist in segments.dropna() for item in sublist if item.strip()]
        counts = {'P': 0, 'N': 0, 'NEU': 0}
        for op in all_opinions:
            if op.endswith('[P]'): counts['P'] += 1
            elif op.endswith('[N]'): counts['N'] += 1
            elif op.endswith('[NEU]'): counts['NEU'] += 1
        
        total_opinions = counts['P'] + counts['N'] + counts['NEU']
        
        if total_opinions > 0:
            net_sentiment = (counts['P'] - counts['N']) / total_opinions
            confidence_weight = min(1.0, np.log(1 + total_opinions) / np.log(1 + min_threshold))
            results[f'Score_{aspect}'] = round(net_sentiment * confidence_weight, 4)
            results[f'Count_{aspect}'] = total_opinions
        else:
            results[f'Score_{aspect}'] = np.nan
            results[f'Count_{aspect}'] = 0

    # Tính toán Rating Score
    rating_column = group_df['rating']
    extracted_rating = rating_column.astype(str).str.extract(r'(\d+(\.\d+)?)')[0].dropna().astype(float)
    num_reviews_raw = len(extracted_rating)
    mean_rating = extracted_rating.mean() if num_reviews_raw > 0 else np.nan

    if num_reviews_raw == 0 or pd.isna(mean_rating):
        results['Score_Rating'] = np.nan
        results['Review_Count_Raw'] = 0
    else:
        normalized_score = (mean_rating - 3) / 2
        confidence_rating = min(1.0, np.log(1 + num_reviews_raw) / np.log(1 + min_threshold))
        results['Score_Rating'] = round(normalized_score * confidence_rating, 4)
        results['Review_Count_Raw'] = num_reviews_raw
        
    return pd.Series(results)

# HÀM 2: CHẠY RANKING ENGINE
def run_ranking_engine(
    df_restaurants: pd.DataFrame, 
    df_reviews: pd.DataFrame, 
    place_ids_to_rank: List[str]
) -> pd.DataFrame:
    
    # 1. Lọc reviews
    df_reviews_sample = df_reviews[df_reviews['place_id'].isin(place_ids_to_rank)].copy()
    if df_reviews_sample.empty:
        print("Không có review nào trong DB cho các place_id này.")
        return pd.DataFrame()

    # 2. TÍNH TOÁN
    print("Bắt đầu tính toán tất cả điểm số (Aspects và Rating)...")
    df_results = (
        df_reviews_sample
        .groupby('place_id', group_keys=False)
        .apply(lambda g: calculate_all_scores(g, MIN_REVIEW_THRESHOLD))
        .reset_index()
    )
    print("Tính toán hoàn tất.")

    # 3. Tính điểm tổng hợp
    df_results['Score_Aspect_Avg_Weighted'] = df_results[['Score_Food', 'Score_Place', 'Score_Price']].mean(axis=1)
    
    # Xử lý NaN (Rất quan trọng)
    df_results['Score_Rating'] = df_results['Score_Rating'].fillna(0)
    df_results['Score_Aspect_Avg_Weighted'] = df_results['Score_Aspect_Avg_Weighted'].fillna(0)

    df_results['Overall_Recommendation_Score'] = (
        df_results['Score_Aspect_Avg_Weighted'] * SCORE_WEIGHTS['Aspect_Score']
        + df_results['Score_Rating'] * SCORE_WEIGHTS['Rating_Score']
    )

    # 4. Sắp xếp
    df_recommendations = df_results.sort_values(
        by='Overall_Recommendation_Score', ascending=False
    ).reset_index(drop=True)

    # 5. Lấy tên nhà hàng (Sử dụng 'place_name' từ code của bạn)
    df_final = pd.merge(
        df_restaurants[['place_id', 'place_name']], 
        df_recommendations,
        on='place_id',
        how='right'
    ).drop_duplicates(subset=['place_id']).reset_index(drop=True)
    
    return df_final