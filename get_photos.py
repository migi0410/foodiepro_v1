import pandas as pd
import numpy as np
import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- CẤU HÌNH TỆP ---
# (Hãy đổi tên tệp và định dạng nếu cần)
FILE_NAME = "restaurant_list_textsearch.csv"
# FILE_NAME = "restaurant_list_textsearch.csv"

# Cột ID nhà hàng (PHẢI CÓ)
PLACE_ID_COLUMN = "place_id" 
# Cột để điền ảnh
PHOTO_URL_COLUMN = "photo_url"

def setup_driver() -> webdriver.Chrome:
    """Khởi tạo và cài đặt Chrome Driver tự động."""
    print("Setting up Chrome driver...")
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    
    # --- LỜI KHUYÊN ---
    # Hãy chạy ở chế độ BÌNH THƯỜNG (không headless) trước.
    # Google Maps rất có thể sẽ yêu cầu bạn đăng nhập hoặc xác nhận CAPTCHA.
    # Bạn hãy làm thủ công, sau đó script sẽ tiếp tục.
    #
    # options.add_argument("--headless") # CHỈ DÙNG KHI MỌI THỨ ĐÃ CHẠY ỔN
    
    # Ngăn thông báo "Chrome is being controlled by automated software"
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    print("Driver setup complete.")
    return driver

def get_gmaps_image_url(driver: webdriver.Chrome, place_id: str) -> str | None:
    """
    Sử dụng Selenium để truy cập Google Maps bằng Place ID
    và lấy URL của ảnh đại diện (ảnh hero).
    (ĐÃ CẬP NHẬT: Chấp nhận mọi loại link http/https)
    """
    if not place_id or pd.isna(place_id):
        print("  > Lỗi: place_id bị trống.")
        return None

    try:
        # 1. Xây dựng URL Google Maps
        map_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
        driver.get(map_url)
        
        # 2. Chờ trang tải
        wait = WebDriverWait(driver, 10) 

        # 3. Lấy ảnh đại diện
        # (Selector dựa trên F12 của bạn: tìm <img> bên trong <div class="RZ66Rb">)
        image_selector = "div.RZ66Rb img" 
        
        # Chờ cho đến khi phần tử ảnh xuất hiện
        image_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, image_selector))
        )
        
        # Chờ cho đến khi 'src' được tải (không phải link rỗng và bắt đầu bằng http)
        wait.until(
            lambda d: image_element.get_attribute("src") and image_element.get_attribute("src").startswith("http")
        )

        image_url = image_element.get_attribute("src")
        
        # === SỬA ĐỔI QUAN TRỌNG ===
        # Chỉ cần kiểm tra xem nó có phải là một link http hay không
        if image_url and image_url.startswith("http"):
            return image_url
        else:
            # Điều này gần như không thể xảy ra vì wait.until ở trên đã kiểm tra
            print(f"  > Đã tìm thấy link nhưng không hợp lệ (không phải http): {image_url}")
            return None

    except TimeoutException:
        print(f"  > Lỗi Timeout khi tìm Place ID: {place_id}")
        print("  > Nguyên nhân: Trang tải quá chậm, hoặc bị kẹt ở CAPTCHA.")
        return "TIMEOUT_OR_CAPTCHA" # Đánh dấu để bạn kiểm tra lại
    except NoSuchElementException:
        print(f"  > Lỗi NoSuchElement: Google đã thay đổi HTML (Selector hỏng).")
        return "SELECTOR_BROKEN" # Đánh dấu lỗi
    except Exception as e:
        print(f"  > Lỗi không xác định: {e}")
        return None

def main():
    """Hàm chính để đọc file, tìm ảnh và lưu lại."""
    
    # 1. Đọc tệp
    print(f"Đang đọc tệp: {FILE_NAME}")
    if FILE_NAME.endswith(".pkl"):
        df = pd.read_pickle(FILE_NAME)
    elif FILE_NAME.endswith(".csv"):
        # SỬA ĐỔI: Thử đọc với UTF-8 trước, nếu lỗi thì thử latin1
        try:
            df = pd.read_csv(FILE_NAME)
        except UnicodeDecodeError:
            print("  > Đọc UTF-8 lỗi, thử đọc bằng 'latin1'...")
            df = pd.read_csv(FILE_NAME, encoding='latin1')
    else:
        print("Định dạng tệp không được hỗ trợ (chỉ .pkl hoặc .csv)")
        return

    # 2. Tạo cột photo_url nếu chưa có
    if PHOTO_URL_COLUMN not in df.columns:
        df[PHOTO_URL_COLUMN] = np.nan

    # 3. Tìm các dòng cần điền (còn trống hoặc là 'N/A')
    mask_to_fill = (df[PHOTO_URL_COLUMN].isnull()) | \
                   (df[PHOTO_URL_COLUMN] == 'N/A') | \
                   (df[PHOTO_URL_COLUMN] == '')
    
    df_to_fill = df[mask_to_fill]

    if df_to_fill.empty:
        print("Tất cả nhà hàng đã có photo_url. Không có gì để làm.")
        return

    print(f"Tìm thấy {len(df_to_fill)} nhà hàng cần lấy ảnh.")
    
    # Tạo file backup (luôn dùng pickle để an toàn, không lo encoding)
    backup_file = FILE_NAME.replace(".csv", ".pkl").replace(".pkl", "_backup.pkl")
    df.to_pickle(backup_file) 
    print(f"Đã tạo file backup (pickle) tại: {backup_file}")

    # 4. Khởi tạo Selenium
    driver = setup_driver()
    count = 0
    total = len(df_to_fill)

    # Lần chạy đầu tiên, Google có thể hỏi Cookie
    try:
        driver.get("https://www.google.com/maps")
        print("\nĐÃ MỞ GOOGLE MAPS. HÃY KIỂM TRA TRÌNH DUYỆT!")
        print("Nếu thấy yêu cầu 'Accept Cookies' (Chấp nhận Cookie), hãy bấm vào đó.")
        print("Script sẽ đợi bạn 15 giây để thao tác...")
        time.sleep(15)
        print("Bắt đầu... (Nếu bị lỗi, hãy tăng thời gian chờ này lên)")
    except Exception:
        pass


    try:
        # 5. Lặp qua các nhà hàng cần điền
        for index, row in df_to_fill.iterrows():
            count += 1
            place_id = row[PLACE_ID_COLUMN]
            print(f"\nĐang xử lý {count}/{total}: Place ID {place_id}")
            
            # Lấy URL ảnh
            image_url = get_gmaps_image_url(driver, place_id)
            
            if image_url:
                print(f"  > Tìm thấy URL: {image_url[:60]}...") 
                df.at[index, PHOTO_URL_COLUMN] = image_url
            else:
                print("  > Không tìm thấy ảnh.")
                if image_url not in ["TIMEOUT_OR_CAPTCHA", "SELECTOR_BROKEN"]:
                    df.at[index, PHOTO_URL_COLUMN] = "NOT_FOUND" 

            # 6. Lưu tiến trình mỗi 5 nhà hàng
            if count % 5 == 0:
                print(f"\n--- Đã xử lý {count} nhà hàng, đang lưu tiến trình... ---")
                if FILE_NAME.endswith(".pkl"):
                    df.to_pickle(FILE_NAME)
                else:
                    # SỬA ĐỔI: Thêm encoding='utf-8-sig'
                    df.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
                print("--- Lưu thành công. ---")

            # 7. Chờ 2-4 giây
            time.sleep(np.random.uniform(2, 4)) 

    except Exception as e:
        print(f"\n!!! LỖI NGHIÊM TRỌNG TRONG VÒNG LẶP: {e} !!!")
    finally:
        # 8. Dọn dẹp và lưu lần cuối
        print("\nHoàn tất xử lý. Đang đóng driver và lưu tệp lần cuối...")
        driver.quit()
        
        if FILE_NAME.endswith(".pkl"):
            df.to_pickle(FILE_NAME)
        else:
            # SỬA ĐỔI: Thêm encoding='utf-8-sig'
            df.to_csv(FILE_NAME, index=False, encoding='utf-8-sig')
            
        print(f"Đã lưu tệp {FILE_NAME} thành công.")
        print("Chương trình kết thúc.")

if __name__ == "__main__":
    main()