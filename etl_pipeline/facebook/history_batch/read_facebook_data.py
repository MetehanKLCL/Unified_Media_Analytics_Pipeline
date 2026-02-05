import json
import os
import pandas as pd

# --- AYARLAR ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw", "facebook")

def read_data_with_links():
    # En son inen 'posts' dosyasını bul
    files = [f for f in os.listdir(RAW_DIR) if f.startswith("facebook_posts")]
    if not files:
        print("Dosya bulunamadı!")
        return
        
    latest_file = max([os.path.join(RAW_DIR, f) for f in files], key=os.path.getctime)
    print(f"📂 Okunan Dosya: {os.path.basename(latest_file)}\n")

    with open(latest_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    clean_data = []
    for post in data:
        clean_data.append({
            "1. Tarih": post.get("time")[:10],
            "2. Metin": post.get("text", "")[:30] + "...", # Yer kaplamasın diye kısalttım
            "3. Beğeni": post.get("likes"),
            "4. Yorum": post.get("comments"),
            "5. Paylaşım": post.get("shares"),
            "6. Link": post.get("url")  # <-- İŞTE İSTEDİĞİN LİNK BURADA
        })

    # Tabloyu ayarla
    df = pd.DataFrame(clean_data)
    
    # Pandas'a linkleri kesmemesini söyleyelim
    pd.set_option('display.max_colwidth', None) 
    
    print(df.to_string(index=False))

if __name__ == "__main__":
    read_data_with_links()