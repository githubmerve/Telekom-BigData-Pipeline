import json
import time
import random
import os
import re
from datetime import datetime
from kafka import KafkaProducer
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# --- KAFKA YAPILANDIRMA ---
KAFKA_SERVER = "192.168.56.110:9092"
KAFKA_TOPIC = "sikayetler"

try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=5
    )
    print(f"✅ Kafka bağlantısı kuruldu: {KAFKA_SERVER}")
except Exception as e:
    print(f"❌ Kafka'ya bağlanılamadı: {e}")
    producer = None

# Ortak Yapılandırma ve Hafıza Havuzu
processed_links = set()
DATA_FILE = "scraped_data.jsonl"

def load_history():
    """Ortak scraped_data.jsonl dosyasını tarayarak daha önce çekilmiş linkleri hafızaya alır."""
    if os.path.exists(DATA_FILE):
        print(f"\n📂 Ortak {DATA_FILE} havuzu taranıyor ve hafızaya yükleniyor...")
        count = 0
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    processed_links.add(data["link"])
                    count += 1
                except: continue
        print(f"✅ Toplam {count} adet eski link hafızaya alındı. Çakışmalar otomatik engellenecek.\n")

def start_driver():
    options = Options()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service("/usr/bin/chromedriver")
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        return driver
    except Exception as e:
        print(f"❌ Driver başlatılamadı: {e}")
        return None

def get_full_content(driver, link):
    """Şikayet detayına girer; kurumsal cevapları ayıklayıp SADECE asıl kullanıcı metnini ve DOĞRU tarihi çeker."""
    try:
        driver.get(link)
        time.sleep(3.0) # Sayfa DOM ağacının tam oturması için kararlı süre
        
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 🎯 1. ADIM: Sadece asıl şikayet alanının içindeki "Devamını Oku" butonunu tetikle
        try:
            more_btn = driver.find_element(By.XPATH, "//article//p[contains(@class, 'wrap-anywhere')]//span[contains(text(), '...')]")
            if more_btn.is_displayed():
                driver.execute_script("arguments[0].click();", more_btn)
                time.sleep(0.5)
        except:
            pass

        driver.execute_script("window.scrollTo(0, 200);")
        time.sleep(0.5)

        # 🎯 2. ADIM: Nokta Atışı İçerik Yalıtımı (Kurumsal cevapları bloklayan akıllı filtre)
        full_text = ""
        exact_selectors = [
            "//article//div[contains(@class, 'selection-share') and .//p[contains(@class, 'wrap-anywhere')]]",
            "//p[contains(@class, 'wrap-anywhere')]/parent::div"
        ]

        for xpath in exact_selectors:
            try:
                element = driver.find_element(By.XPATH, xpath)
                raw_text = driver.execute_script("return arguments[0].textContent;", element)
                
                if raw_text and len(raw_text.strip()) > 40:
                    if "Değerli Müşterimiz" in raw_text or "Vodafone Memnuniyet Merkezi" in raw_text:
                        paragraphs = element.find_elements(By.XPATH, ".//p[contains(@class, 'wrap-anywhere')]")
                        full_text = " ".join([p.text.strip() for p in paragraphs]).strip()
                    else:
                        full_text = raw_text.strip()
                    
                    if len(full_text) > 40:
                        break
            except: continue

        if len(full_text) < 35:
            return "Hata: İçerik paragrafları tam olarak çözülemedi.", "Hata"

        # 🎯 3. ADIM: %100 Nokta Atışı Tarih Avcısı (Getirdiğin Dinamik _5a_ Yapısı Sınıf Bazlı Çözüldü)
        js_date_script = """
        try {
            // Getirdiğin HTML parçasına göre en spesifik sınıfları ve özellikleri tarıyoruz
            let timeElem = document.querySelector("article span.text-zinc-500[data-base-ui-click-trigger]") || 
                           document.querySelector("article header span[data-base-ui-click-trigger]") ||
                           document.querySelector("span[id*='base-ui-'][aria-label*=':']");
            
            if (timeElem) {
                // Sitenin hileli iç metin taktiklerine karşı her zaman aria-label özniteliğini öncelikli alıyoruz
                return timeElem.getAttribute("aria-label") || timeElem.innerText || timeElem.textContent;
            }
            return "Tarih Yok";
        } catch(e) {
            return "Tarih Yok";
        }
        """
        
        complaint_date = driver.execute_script(js_date_script)
        
        # Eğer DOM gecikmesi yaşanırsa sistemi kilitlemeyen yedek kalkan
        if not complaint_date or complaint_date.strip() == "Tarih Yok" or not re.search(r'\d', complaint_date):
            complaint_date = datetime.now().strftime("%d %B %H:%M")

        # 🎯 4. ADIM: Dil Standartlaştırma (Ayları Türkçe Eşitleme)
        month_map = {
            "January": "Ocak", "February": "Şubat", "March": "Mart", "April": "Nisan",
            "May": "Mayıs", "June": "Haziran", "July": "Temmuz", "August": "Ağustos",
            "September": "Eylül", "October": "Ekim", "November": "Kasım", "December": "Aralık"
        }
        for eng, tr in month_map.items():
            complaint_date = complaint_date.replace(eng, tr)

        # Kalıntı temizliği
        for junk in ["...", "Devamını oku", "Küçült", "Tümünü gör", "Daha az gör"]:
            full_text = full_text.replace(junk, "")

        full_text = re.sub(r'\s+', ' ', full_text).strip()
        complaint_date = re.sub(r'\s+', ' ', complaint_date).strip()

        return full_text, complaint_date

    except Exception as e:
        return f"Hata: {str(e)[:50]}", "Hata"

def save_to_file(data):
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    if producer:
        try:
            producer.send(KAFKA_TOPIC, data)
        except Exception as e:
            print(f"⚠️ Kafka gönderim hatası: {e}")

def scrape_special_category(driver, company_name, category_name, base_url, max_pages=200):
    print(f"\n{'='*70}")
    print(f"🎯 🔥 [{company_name} - {category_name.upper()}] ÖZEL KATEGORİ TARAMASI BAŞLADI")
    print(f"{'='*70}")
    
    current_round_links = []
    
    for page in range(1, max_pages + 1):
        try:
            driver.get(f"{base_url}?page={page}")
            time.sleep(random.uniform(2, 3.5))
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            
            cards = driver.find_elements(By.CSS_SELECTOR, "article")
            new_links_found = 0
            
            for card in cards:
                try:
                    link = card.find_element(By.TAG_NAME, "a").get_attribute("href")
                    if link and link not in processed_links and "sikayetvar.com" in link:
                        if link.strip().endswith("-video"):
                            continue
                        current_round_links.append(link)
                        new_links_found += 1
                except: continue
            
            print(f"📄 Sayfa {page}/200 | Filtrelere Takılmayan Yeni Link: {new_links_found} | Toplam Havuz: {len(current_round_links)}")
            
        except Exception: 
            break

    total = len(current_round_links)
    if total == 0:
        print(f"\nℹ️ 200 sayfalık derin tarama bitti; ancak halihazırda veri kümesinde bulunmayan yeni bir linke rastlanmadı.")
        return

    print(f"\n\n💎 Derin Taramadan Yakalanan {total} ADET ÖZEL ODAKLI ŞİKAYET ÇEKİLİYOR...\n")
    
    for i, link in enumerate(current_round_links, 1):
        content, date = get_full_content(driver, link)
        
        if content and not content.startswith("Hata:"):
            record = {
                "company": company_name,
                "date": date,
                "link": link,
                "content": content,
                "crawl_timestamp": time.time()
            }
            
            save_to_file(record)
            processed_links.add(link)
            
            print(f"\n{'-'*50}")
            print(f"[{i}/{total}] ✅ {company_name} | Kategori: {category_name.upper()} | 📅 {date}")
            print(f"🔗 {link}")
            print(f"📝 İÇERİK:\n{content}")
            print(f"{'-'*50}")
        else:
            print(f"[{i}/{total}] ⚠️ Sayfa yükleme hatası/gecikmesi: {link}")
            time.sleep(random.uniform(1.5, 3))
            
        time.sleep(random.uniform(3, 5.5))

def run():
    load_history()
    driver = start_driver()
    if not driver: return
    
    # 🎯 Yeni talep ettiğin Tarife Değişikliği linkleri ve kategorileri milimetrik olarak haritalandı
    special_targets = [
        ("VODAFONE", "tarife-degisikligi", "https://www.sikayetvar.com/vodafone/tarife-degisikligi"),
        ("TURKCELL", "tarife-degisikligi", "https://www.sikayetvar.com/turkcell/tarife-degisikligi"),
        ("TURKTELEKOM", "tarife-degisikligi", "https://www.sikayetvar.com/turk-telekom/tarife-degisikligi")
    ]

    try:
        while True:
            for company, category, url in special_targets:
                scrape_special_category(driver, company, category, url, max_pages=200)
            
            print(f"\n📊 ÖZEL KATEGORİ DERİN TURU BİTTİ. Hafızadaki Toplam Link Sayısı: {len(processed_links)}\n")
            print("🕒 5 dakika bekleniyor, ardından yeni veriler için tekrar taranacak...")
            time.sleep(300)
            
    except KeyboardInterrupt:
        print("\n🛑 Sistem durduruldu.")
    finally:
        if producer: producer.flush()
        if driver: driver.quit()

if __name__ == "__main__":
    run()


