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

# Yapılandırma
processed_links = set()
DATA_FILE = "scraped_data.jsonl"

def load_history():
    """Mevcut verileri hafızaya alır ve Kafka'ya fırlatır."""
    if os.path.exists(DATA_FILE):
        print(f"\n📂 {DATA_FILE} taranıyor ve Kafka'ya aktarılıyor...")
        count = 0
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    processed_links.add(data["link"])
                    if producer:
                        producer.send(KAFKA_TOPIC, data)
                    count += 1
                except: continue
        if producer:
            producer.flush()
        print(f"✅ {count} adet eski veri hatırlandı ve Kafka'ya basıldı.\n")

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
    """Şikayet detayına girer; kurumsal cevapları ayıklayarak SADECE asıl kullanıcı metnini çeker."""
    try:
        driver.get(link)
        time.sleep(2.5) # Sayfa DOM ağacının tarayıcıya tam oturması için bekleme süresi
        
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

        # 🎯 1. ADIM: Sadece asıl şikayet alanının içindeki "Devamını Oku" butonunu tetikle
        try:
            # article içindeki ilk wrap-anywhere barındıran buton alanını hedefliyoruz
            more_btn = driver.find_element(By.XPATH, "//article//p[contains(@class, 'wrap-anywhere')]//span[contains(text(), '...')]")
            if more_btn.is_displayed():
                driver.execute_script("arguments[0].click();", more_btn)
                time.sleep(0.5)
        except:
            pass

        driver.execute_script("window.scrollTo(0, 200);")
        time.sleep(0.5)

        # 🎯 2. ADIM: Nokta Atışı İçerik Yalıtımı (Kurumsal cevapları ve başlıkları bloklayan akıllı filtre)
        full_text = ""
        
        # Söktüğün HTML haritasına göre geliştirilen nokta atışı seçici dizisi
        exact_selectors = [
            "//article//div[contains(@class, 'selection-share') and .//p[contains(@class, 'wrap-anywhere')]]",
            "/html/body/main/div[5]/div[2]/div/div/article/div[3]", # Söküp getirdiğin o harika mutlak yol
            "//p[contains(@class, 'wrap-anywhere')]/parent::div"
        ]

        for xpath in exact_selectors:
            try:
                element = driver.find_element(By.XPATH, xpath)
                raw_text = driver.execute_script("return arguments[0].textContent;", element)
                
                if raw_text and len(raw_text.strip()) > 40:
                    # Markanın otomatik kurumsal cevap metinlerini koruma amaçlı yalıtıyoruz
                    if "Değerli Müşterimiz" in raw_text or "Vodafone Memnuniyet Merkezi" in raw_text:
                        # Eğer kurumsal cevap karıştıysa sadece şikayet paragraf alt bileşenlerini cımbızla çek
                        paragraphs = element.find_elements(By.XPATH, ".//p[contains(@class, 'wrap-anywhere')]")
                        full_text = " ".join([p.text.strip() for p in paragraphs]).strip()
                    else:
                        full_text = raw_text.strip()
                    
                    if len(full_text) > 40:
                        break
            except: continue

        # Eğer koruma bariyerine takılırsa sistemi tıkama, jenerik hata döndür
        if len(full_text) < 35:
            return "Hata: İçerik paragrafları tam olarak çözülemedi.", "Hata"

        # 🎯 3. ADIM: Tarih Avcısı (Sadece şikayetin kendi ana tarihini alır)
        complaint_date = "Tarih Yok"
        try:
            date_elem = driver.find_element(By.XPATH, "//article//header//span[@data-base-ui-click-trigger]")
            complaint_date = date_elem.get_attribute("aria-label") or date_elem.text.strip()
        except:
            try:
                date_elem = driver.find_element(By.CSS_SELECTOR, "span[data-base-ui-click-trigger]")
                complaint_date = date_elem.get_attribute("aria-label") or date_elem.text.strip()
            except: pass

        if complaint_date == "Tarih Yok" or not re.search(r'\d', complaint_date):
            complaint_date = datetime.now().strftime("%d %B %H:%M")

        # Metin içi üç nokta veya temizlik kalıntıları eleniyor
        for junk in ["...", "Devamını oku", "Küçült", "Tümünü gör", "Daha az gör"]:
            full_text = full_text.replace(junk, "")

        # Doğrusal kararlı NLP formatı
        full_text = re.sub(r'\s+', ' ', full_text).strip()

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

def scrape_big_data(driver, company_name, base_url, max_pages=200):
    print(f"\n{'='*60}")
    print(f"🚀 [{company_name}] TARAMA BAŞLADI (Sınır: {max_pages} Sayfa)")
    print(f"{'='*60}")
    
    current_round_links = []
    consecutive_empty_pages = 0
    
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
            
            print(f"📄 Sayfa {page} | Yeni Link: {new_links_found} | Havuz: {len(current_round_links)}", end="\r")
            
            if new_links_found == 0:
                consecutive_empty_pages += 1
            else:
                consecutive_empty_pages = 0
                
            if page > 10 and consecutive_empty_pages >= 10:
                print(f"\n✨ {company_name} için 10 sayfa boyunca yeni veriye rastlanmadı. Güvenli durma tetiklendi.")
                break
        except Exception: break

    total = len(current_round_links)
    if total == 0:
        return

    print(f"\n\n💎 {total} YENİ METİN TABANLI ŞİKAYET ÇEKİLİYOR...\n")
    
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
            print(f"[{i}/{total}] ✅ {company_name} | 📅 {date}")
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
    
    targets = [
        ("VODAFONE", "https://www.sikayetvar.com/vodafone"),
        ("TURKTELEKOM", "https://www.sikayetvar.com/turk-telekom"),
        ("TURKCELL", "https://www.sikayetvar.com/turkcell")
    ]

    try:
        while True:
            for name, url in targets:
                scrape_big_data(driver, name, url, max_pages=200)
            
            print(f"\n📊 TUR BİTTİ. Toplam Veri: {len(processed_links)}\n")
            time.sleep(300)
            
    except KeyboardInterrupt:
        print("\n🛑 Sistem durduruldu.")
    finally:
        if producer: producer.flush()
        if driver: driver.quit()

if __name__ == "__main__":
    run()

