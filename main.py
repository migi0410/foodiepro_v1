# main.py
import pandas as pd
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Optional
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import os

# Import engine ranking của bạn
# Đảm bảo tệp ranking_engine.py nằm cùng thư mục với main.py
import ranking_engine

# --- KHỞI TẠO ỨNG DỤNG ---
app = FastAPI(
    title="FoodiePro API",
    description="API cho hệ thống đề xuất nhà hàng FoodiePro",
    version="1.0.0"
)

# --- THÊM CORS MIDDLEWARE ---
# Cho phép JavaScript (từ index.html) gọi được API này
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép tất cả
    allow_credentials=True,
    allow_methods=["*"], # Cho phép tất cả các method (POST, GET,...)
    allow_headers=["*"], # Cho phép tất cả các header
)

# --- TẢI DỮ LIỆU KHI SERVER KHỞI ĐỘNG ---
DF_RESTAURANTS = pd.DataFrame()
DF_REVIEWS = pd.DataFrame()

@app.on_event("startup")
def load_data():
    global DF_RESTAURANTS, DF_REVIEWS
    try:
        print("Loading data...")
        
        # Sửa đổi 1: Tải danh sách nhà hàng
        # *** THAY ĐỔI ĐỊNH DẠNG NẾU CẦN (ví dụ: pd.read_csv) ***
        DF_RESTAURANTS = pd.read_csv("restaurant_list_textsearch.csv")
        
        # Sửa đổi 2: Tải dữ liệu review
        # *** THAY ĐỔI ĐỊNH DẠNG NẾU CẦN (ví dụ: pd.read_csv) ***
        DF_REVIEWS = pd.read_csv("final_aspects_extracted.csv")
        
        print("Data loaded successfully.")
        print(f"Total restaurants: {len(DF_RESTAURANTS)}, Total reviews: {len(DF_REVIEWS)}")
        
        # Kiểm tra các cột bắt buộc
        required_resto_cols = ['place_id', 'restaurant_name', 'place_name', 'street', 'ward', 'district1', 'district2', 'photo_url', 'website']
        for col in required_resto_cols:
            if col not in DF_RESTAURANTS.columns:
                print(f"!!! CẢNH BÁO: Thiếu cột '{col}' trong tệp nhà hàng. Code có thể lỗi.")
                if col == 'photo_url':
                     DF_RESTAURANTS['photo_url'] = None # Thêm cột giả để tránh lỗi
                
        required_review_cols = ['place_id', 'Food', 'Place', 'Price', 'rating']
        for col in required_review_cols:
             if col not in DF_REVIEWS.columns:
                print(f"!!! CẢNH BÁO: Thiếu cột '{col}' trong tệp review. Code có thể lỗi.")

    except FileNotFoundError:
        print("="*50)
        print("LỖI NGHIÊM TRỌNG: KHÔNG TÌM THẤY TỆP DỮ LIỆU")
        print("Hãy đảm bảo các tệp sau nằm cùng thư mục với main.py:")
        print("1. restaurant_list_textsearch.pkl")
        print("2. final_aspects_extracted.pkl")
        print("="*50)
    except Exception as e:
        print("="*50)
        print(f"LỖI KHI TẢI DỮ LIỆU: {e}")
        print("Bạn có chắc chắn tệp của mình là định dạng .pkl không?")
        print("Nếu tệp của bạn là .csv, hãy sửa code thành pd.read_csv('ten_file.csv')")
        print("="*50)

# --- CÁC HÀM HỖ TRỢ (SEARCH) ---
def find_matching_restaurants(df_all_restaurants: pd.DataFrame, query: str, location: str) -> List[str]:
    """
    Hàm này thực hiện BƯỚC A (Search) - ĐÃ CẬP NHẬT
    Lọc ra các nhà hàng khớp với tiêu chí tìm kiếm.
    """
    if df_all_restaurants.empty:
        return []

    # Khởi tạo mask (bộ lọc) ban đầu là True (lấy tất cả)
    final_mask = pd.Series(True, index=df_all_restaurants.index)

    # 1. Lọc theo location (Ưu tiên street > ward > district1 > district2)
    if location:
        # Chuyển đổi location input sang chữ thường để tìm kiếm không phân biệt hoa/thường
        location_lower = location.lower()
        
        # Tạo mask cho từng cột location, đảm bảo xử lý
        # giá trị NaN (na=False) để tránh lỗi
        mask_street = df_all_restaurants['street'].str.lower().str.contains(location_lower, na=False)
        mask_ward = df_all_restaurants['ward'].str.lower().str.contains(location_lower, na=False)
        mask_district1 = df_all_restaurants['district1'].str.lower().str.contains(location_lower, na=False)
        mask_district2 = df_all_restaurants['district2'].str.lower().str.contains(location_lower, na=False)
        
        # Kết hợp 4 cột: Chỉ cần khớp 1 trong 4 cột là được (toán tử | là OR)
        location_mask = mask_street | mask_ward | mask_district1 | mask_district2
        
        # Áp dụng bộ lọc location vào mask tổng
        final_mask = final_mask & location_mask

    # 2. Lọc theo query (Chỉ tìm trong cột 'restaurant_name')
    if query:
        # Chuyển đổi query input sang chữ thường
        query_lower = query.lower()
        
        # Tạo mask cho tên nhà hàng
        query_mask = df_all_restaurants['restaurant_name'].str.lower().str.contains(query_lower, na=False)
        
        # Áp dụng bộ lọc query vào mask tổng
        final_mask = final_mask & query_mask

    # Lấy ra các nhà hàng thỏa mãn tất cả điều kiện
    df_filtered = df_all_restaurants[final_mask]

    # Trả về danh sách các place_id đã lọc
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
    Đây là endpoint chính mà frontend gọi đến.
    """
    print(f"Received request: query='{request.query}', location='{request.location}'")
    
    if DF_RESTAURANTS.empty or DF_REVIEWS.empty:
        print("Server data not loaded. Returning error.")
        return {"error": "Server data not loaded. Check server logs."}

    # --- BƯỚC A: SEARCH (TÌM KIẾM) ---
    # Lấy ra danh sách các nhà hàng khớp với "Phở" và "Quận 1"
    place_ids_to_rank = find_matching_restaurants(DF_RESTAURANTS, request.query, request.location)
    
    if not place_ids_to_rank:
        print("No matching restaurants found from search.")
        return [] # Trả về mảng rỗng, frontend sẽ xử lý
    
    print(f"Found {len(place_ids_to_rank)} matching restaurants. Starting ranking...")

    # --- BƯỚC B: RANK (XẾP HẠNG) ---
    # Đưa danh sách ID này vào ranking engine của bạn
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
        
        # 1. Lấy thêm photo_url từ file nhà hàng
        # (Frontend cần cột 'photo_url')
        df_final_results = pd.merge(
            df_ranked,
            DF_RESTAURANTS[['place_id', 'photo_url', 'website']],
            on='place_id',
            how='left'
        )
        
        # 2. Đổi tên cột 'place_name' (từ ranker) thành 'name' (cho frontend)
        if 'place_name' in df_final_results.columns:
             df_final_results = df_final_results.rename(columns={'place_name': 'name'})
        else:
             print("!!! CẢNH BÁO: Thiếu cột 'place_name' sau khi ranking.")
             df_final_results['name'] = 'N/A' # Thêm cột giả
        
        # 3. Chọn các cột mà frontend (script.js) của bạn đang chờ
        columns_to_return = ['place_id', 'name', 'Overall_Recommendation_Score', 'photo_url','website']
        
        # Đảm bảo tất cả các cột đều tồn tại
        final_columns = [col for col in columns_to_return if col in df_final_results.columns]
        
        # Xử lý NaN (giá trị rỗng) trước khi gửi JSON
        df_final_json = df_final_results[final_columns].fillna('N/A')
        
        results_json = df_final_json.to_dict('records')
        
        print(f"Returning {len(results_json)} ranked results.")
        return results_json

    except Exception as e:
        print(f"!!! LỖI NGHIÊM TRỌNG TRONG KHI RANKING: {e}")
        import traceback
        traceback.print_exc()
        return {"error": f"An internal error occurred: {str(e)}"}

# --- CÁCH CHẠY SERVER ---
if __name__ == "__main__":
    print("Starting FoodiePro API server...")
    # Lấy cổng (PORT) từ biến môi trường của Render
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)