# main.py
import pandas as pd
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os
import traceback 

try:
    import ranking_engine
except ImportError:
    print("LỖI: Không tìm thấy file 'ranking_engine.py' cùng thư mục.")
    exit()


app = FastAPI(
    title="FoodiePro API",
    description="API cho hệ thống đề xuất nhà hàng FoodiePro",
    version="1.0.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép tất cả
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"], 
)

# --- TẢI DỮ LIỆU KHI SERVER KHỞI ĐỘNG ---
DF_RESTAURANTS = pd.DataFrame()
DF_REVIEWS = pd.DataFrame()

# == ĐƯỜNG DẪN DỮ LIỆU CUỐI CÙNG (TỪ PIPELINE) ==
RESTAURANT_DATA_PATH = "data/restaurant_list_textsearch.csv"
REVIEW_DATA_PATH = "data/reviews_with_aspects.csv"

@app.on_event("startup")
def load_data():
    global DF_RESTAURANTS, DF_REVIEWS
    
    # Tạo thư mục 'data' nếu chưa có (phòng trường hợp)
    os.makedirs("data", exist_ok=True) 
    
    try:
        print("Đang tải dữ liệu cuối cùng từ pipeline...")
        
        # Đọc file nhà hàng
        print(f"Đang đọc: {RESTAURANT_DATA_PATH}")
        DF_RESTAURANTS = pd.read_csv(RESTAURANT_DATA_PATH)
        
        # Đọc file review đã xử lý
        print(f"Đang đọc: {REVIEW_DATA_PATH}")
        DF_REVIEWS = pd.read_csv(REVIEW_DATA_PATH)
        
        print("Data loaded. Cleaning duplicates (as a safety check)...")
        
        # Chốt an toàn: Luôn xóa trùng lặp khi tải
        DF_RESTAURANTS.drop_duplicates(subset=['place_id'], keep='first', inplace=True)
        
        print("Tải dữ liệu thành công.")
        print(f"Total unique restaurants: {len(DF_RESTAURANTS)}")
        print(f"Total reviews (with aspects): {len(DF_REVIEWS)}")
        
        # Kiểm tra các cột bắt buộc
        required_resto_cols = ['place_id', 'restaurant_name', 'street', 'ward', 'district1', 'district2', 'photo_url', 'website', 'place_name']
        for col in required_resto_cols:
            if col not in DF_RESTAURANTS.columns:
                print(f"!!! CẢNH BÁO: Thiếu cột '{col}' trong file nhà hàng. API có thể lỗi.")
                
        required_review_cols = ['place_id', 'Food', 'Place', 'Price', 'rating']
        for col in required_review_cols:
             if col not in DF_REVIEWS.columns:
                print(f"!!! CẢNH BÁO: Thiếu cột '{col}' trong file review. Ranking có thể lỗi.")

    except FileNotFoundError as e:
        print("="*50)
        print(f"LỖI NGHIÊM TRỌNG: KHÔNG TÌM THẤY TỆP DỮ LIỆU: {e.filename}")
        print("Bạn đã sao chép 2 tệp sau vào thư mục 'data/' của backend chưa?")
        print(f"  1. {RESTAURANT_DATA_PATH}")
        print(f"  2. {REVIEW_DATA_PATH}")
        print("="*50)
    except Exception as e:
        print(f"Lỗi không xác định khi tải dữ liệu: {e}")
        traceback.print_exc()

# --- CÁC HÀM HỖ TRỢ (SEARCH) ---
def find_matching_restaurants(df_all_restaurants: pd.DataFrame, query: str, location: str) -> List[str]:
    """
    Lọc nhà hàng dựa trên query và location.
    """
    if df_all_restaurants.empty:
        return []
    final_mask = pd.Series(True, index=df_all_restaurants.index)
    
    # Lọc location
    if location:
        location_lower = location.lower()
        mask_street = df_all_restaurants['street'].str.lower().str.contains(location_lower, na=False)
        mask_ward = df_all_restaurants['ward'].str.lower().str.contains(location_lower, na=False)
        mask_district1 = df_all_restaurants['district1'].str.lower().str.contains(location_lower, na=False)
        mask_district2 = df_all_restaurants['district2'].str.lower().str.contains(location_lower, na=False)
        location_mask = mask_street | mask_ward | mask_district1 | mask_district2
        final_mask = final_mask & location_mask
        
    # Lọc query
    if query:
        query_lower = query.lower()
        query_mask = df_all_restaurants['restaurant_name'].str.lower().str.contains(query_lower, na=False)
        final_mask = final_mask & query_mask
        
    df_filtered = df_all_restaurants[final_mask]
    return df_filtered['place_id'].unique().tolist()

# --- ĐỊNH NGHĨA REQUEST BODY ---
class SearchRequest(BaseModel):
    query: Optional[str] = None
    location: Optional[str] = None

# --- ENDPOINT CHÍNH ---

@app.get("/")
def read_root():
    return {"message": "Welcome to FoodiePro API!"}

@app.post("/recommend")
def get_recommendations(request: SearchRequest):
    """
    Endpoint chính mà frontend sẽ gọi.
    """
    print(f"Received request: query='{request.query}', location='{request.location}'")
    
    if DF_RESTAURANTS.empty or DF_REVIEWS.empty:
        print("Server data not loaded. Returning error.")
        return {"error": "Dữ liệu server chưa sẵn sàng. Vui lòng kiểm tra logs."}

    # --- BƯỚC A: SEARCH (TÌM KIẾM) ---
    place_ids_to_rank = find_matching_restaurants(DF_RESTAURANTS, request.query, request.location)
    
    if not place_ids_to_rank:
        print("No matching restaurants found from search.")
        return [] # Trả về mảng rỗng (frontend sẽ xử lý)
    
    print(f"Found {len(place_ids_to_rank)} matching restaurants. Starting ranking...")

    # --- BƯỚC B: RANK (XẾP HẠNG) ---
    try:
        df_ranked = ranking_engine.run_ranking_engine(
            df_restaurants=DF_RESTAURANTS,
            df_reviews=DF_REVIEWS,
            place_ids_to_rank=place_ids_to_rank
        )
        
        if df_ranked.empty:
            print("Ranking engine returned empty DataFrame.")
            return []

        # --- BƯỚC C: FORMAT & RETURN (TRẢ VỀ) ---
        # Gộp thêm photo_url và website từ data chính
        df_final_results = pd.merge(
            df_ranked,
            DF_RESTAURANTS[['place_id', 'photo_url', 'website']],
            on='place_id',
            how='left'
        )
        
        # Đổi tên cột cho frontend
        if 'place_name' in df_final_results.columns:
             df_final_results = df_final_results.rename(columns={'place_name': 'name'})
        else:
             print("!!! CẢNH BÁO: Thiếu cột 'place_name' sau khi ranking.")
             df_final_results['name'] = 'N/A' 
        
        # Chọn các cột cuối cùng để gửi đi
        columns_to_return = ['place_id', 'name', 'Overall_Recommendation_Score', 'photo_url', 'website']
        final_columns = [col for col in columns_to_return if col in df_final_results.columns]
        
        # Xử lý giá trị rỗng (NaN) thành 'N/A'
        df_final_json = df_final_results[final_columns].fillna('N/A')
        
        results_json = df_final_json.to_dict('records')
        
        print(f"Returning {len(results_json)} ranked results.")
        return results_json

    except Exception as e:
        print(f"!!! LỖI NGHIÊM TRỌNG TRONG KHI RANKING: {e}")
        traceback.print_exc()
        return {"error": f"An internal error occurred: {str(e)}"}

# --- CHẠY SERVER (CHO RENDER) ---
if __name__ == "__main__":
    print("Starting FoodiePro API server...")
    # Lấy cổng (PORT) từ biến môi trường của Render
    port = int(os.environ.get("PORT", 8000))
    # Chạy trên 0.0.0.0 để chấp nhận kết nối từ bên ngoài
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)