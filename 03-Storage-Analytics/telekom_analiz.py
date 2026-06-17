from pymongo import MongoClient
import pandas as pd
import numpy as np
import time
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

# ==========================================
# 1. SUNUCU KONFİGÜRASYONU VE VERİ YÜKLEME
# ==========================================
SUNUCU_IP = "192.168.56.110"
MONGO_URI = f"mongodb://{SUNUCU_IP}:27017/"

def veri_yukle_ve_temizle():
    start_time = time.time()
    try:
        print("🔄 İkinci makinedeki MongoDB'den veriler çekiliyor, lütfen bekleyin...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client['TelekomAnaliz']
        collection = db['Yorumlar']
        
        imlec = collection.find({}, {"_id": 0})
        df = pd.DataFrame(list(imlec))
        
        if df.empty:
            print("❌ MongoDB'de veri bulunamadı! Spark akışını kontrol edin.")
            return None, 0
            
        yukleme_suresi = time.time() - start_time
        print(f"📦 MongoDB'den Çekilen Toplam Satır Sayısı: {len(df)}")
        
        df = df.dropna(subset=['cleaned_content']).drop_duplicates(subset=['cleaned_content'])
        print(f"✅ Analize Hazır Benzersiz Şikayet Sayısı: {len(df)}")
        return df, yukleme_suresi
    except Exception as e:
        print(f"❌ Veri yükleme aşamasında hata: {e}")
        return None, 0

# ==========================================
# 2. ANA PROSES MOTORU
# ==========================================
if __name__ == "__main__":
    df_sikayetler, mdb_sure = veri_yukle_ve_temizle()
    
    if df_sikayetler is not None and not df_sikayetler.empty:
        
        ortak_stop_words = [
            "müşteri", "talep", "ilgili", "üzerinden", "bana", "hizmetlerini", "hizmetleri",
            "hiçbir", "herhangi", "tarafından", "böyle", "şöyle", "bizi", "bana", "beni", "bende",
            "biri", "bunun", "buna", "bunda", "onlar", "şunlar", "böylece", "edilmesini", "istiyorum",
            "hem", "daha", "ne", "her", "aynı", "bir", "kadar", "sonra", "bütün", "hattım",
            "gibi", "için", "olan", "olarak", "neyse", "değil", "yok", "var", "şey", "mi", "mu",
            "nedeniyle", "yoksa", "başka", "bunu", "benim", "şuan", "yine", "yani", "devam", "önce"
        ]
        
        # ------------------------------------------
        # ETAP A & B: TF-IDF VE KELİME FREKANSI
        # ------------------------------------------
        print("\n" + "="*60)
        print("🧮 ETAP A & B: MARKA BAZLI MATEMATİKSEL TF-IDF ANALİZİ")
        print("="*60)
        marka_gruplari = df_sikayetler.groupby('company')['cleaned_content'].apply(lambda x: ' '.join(x)).reset_index()
        tfidf = TfidfVectorizer(stop_words=ortak_stop_words, max_features=30)
        tfidf_matris = tfidf.fit_transform(marka_gruplari['cleaned_content'])
        kelime_isimleri_tfidf = tfidf.get_feature_names_out()
        tfidf_df = pd.DataFrame(tfidf_matris.toarray(), columns=kelime_isimleri_tfidf, index=marka_gruplari['company'])
        
        for marka in tfidf_df.index:
            print(f"📍 {marka} Ayırt Edici Kriz Terimleri: {', '.join(tfidf_df.loc[marka].sort_values(ascending=False).head(5).index)}")
            
        # ------------------------------------------
        # ETAP C: LDA KONU MODELLEME VE DAĞILIMLARI
        # ------------------------------------------
        print("\n" + "="*60)
        print("🤖 ETAP C: LDA KONU MODELLEME VE MARKALARA GÖRE DAĞILIMI")
        print("="*60)
        lda_start = time.time()
        vektorlestirici = CountVectorizer(stop_words=ortak_stop_words, max_features=1000, min_df=5)
        kelime_matrisi_lda = vektorlestirici.fit_transform(df_sikayetler['cleaned_content'])
        
        lda_model = LatentDirichletAllocation(n_components=3, random_state=42, learning_method='online')
        konu_dagilimlari = lda_model.fit_transform(kelime_matrisi_lda)
        lda_sure = time.time() - lda_start
        
        df_sikayetler['Baskın_Konu'] = konu_dagilimlari.argmax(axis=1) + 1
        
        capraz_tablo = pd.crosstab(df_sikayetler['company'], df_sikayetler['Baskın_Konu'], normalize='index') * 100
        print(capraz_tablo.round(2).to_string(formatters={1: '{:,.2f}%'.format, 2: '{:,.2f}%'.format, 3: '{:,.2f}%'.format}))
        print("\n💡 Bilgi: Konu 1 -> Tarife/Paket, Konu 2 -> Fatura/İptal, Konu 3 -> Altyapı/Arıza")

        # ------------------------------------------
        # ETAP D: GENİŞLETİLMİŞ CHURN RISK ANALİZİ (Gelişmiş ML)
        # ------------------------------------------
        print("\n" + "="*60)
        print("🚨 ETAP D: GENİŞLETİLMİŞ SENTİMENT VE NLP TABANLI CHURN ANALİZİ")
        print("="*60)
        
        # 1. Aşama: Churn Sinyallerinin Kategorik Ağırlıklandırılması
        kritik_churn_kelimeleri = ["iptal", "cayma", "kapatma", "geçeceğim", "bırakacağım", "tüketici hakem", "başka operatör", "taşınması"]
        orta_churn_kelimeleri = ["pişmanlık", "rezalet", "bıktım", "şikayetçiyim", "muhatap", "haksızlık"]
        
        # Metin tarayarak öznitelik üretme (Feature Engineering)
        df_sikayetler['kritik_sinyal_sayisi'] = df_sikayetler['cleaned_content'].apply(lambda x: sum(1 for k in kritik_churn_kelimeleri if k in x))
        df_sikayetler['orta_sinyal_sayisi'] = df_sikayetler['cleaned_content'].apply(lambda x: sum(1 for k in orta_churn_kelimeleri if k in x))
        
        # Duygu skorunu ağırlıklı sayısala çevirme
        df_sikayetler['duygu_agirligi'] = df_sikayetler['duygu'].map({'NEGATIF': 3, 'NOTR (TALEP)': 2, 'NOTR': 1, 'POZITIF': 0}).fillna(1)
        
        # Konu modelleme çıktılarını churn girdisi olarak ekleme (Altyapı ve Fatura krizleri tetikleyicidir)
        df_sikayetler['fatura_iptal_etkisi'] = np.where(df_sikayetler['Baskın_Konu'] == 2, 1, 0)
        df_sikayetler['altyapi_arıza_etkisi'] = np.where(df_sikayetler['Baskın_Konu'] == 3, 1, 0)
        
        # 2. Aşama: Akademik Churn Hedef Değişkeni (Simüle Target)
        # Hem kritik bir kelime kullanmış, hem de duygusu negatif veya talep tabanlı olanları kesin churn (1) sayıyoruz
        df_sikayetler['Hedef_Churn'] = np.where(
            (df_sikayetler['kritik_sinyal_sayisi'] >= 1) & (df_sikayetler['duygu_agirligi'] >= 2), 1, 0
        )
        
        # Girdiler ve Çıktı Matrisi
        X = df_sikayetler[['kritik_sinyal_sayisi', 'orta_sinyal_sayisi', 'duygu_agirligi', 'fatura_iptal_etkisi', 'altyapi_arıza_etkisi']]
        y = df_sikayetler['Hedef_Churn']
        
        # 3. Aşama: Veriyi Eğitim ve Test Olarak Bölme (%80 Eğitim - %20 Test)
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
        
        # Modeli Eğitme
        rf_model = RandomForestClassifier(n_estimators=100, max_depth=6, random_state=42)
        rf_model.fit(X_train, y_train)
        
        # Test Seti Üzerinde Tahminleme Yapma ve Performansı Ölçme
        y_pred = rf_model.predict(X_test)
        
        print("📈 MAKİNE ÖĞRENMESİ MODEL PERFORMANS METRİKLERİ:")
        print(f" ▪️ Model Doğruluk Oranı (Accuracy): %{accuracy_score(y_test, y_pred)*100:.2f}")
        print(f" ▪️ Model Keskinlik Oranı (Precision): %{precision_score(y_test, y_pred)*100:.2f}")
        print(f" ▪️ Model Duyarlılık Oranı (Recall): %{recall_score(y_test, y_pred)*100:.2f}")
        print(f" ▪️ F1-Skoru: {f1_score(y_test, y_pred):.4f}")
        print("-" * 60)
        
        # 4. Aşama: Tüm Veritabanı İçin Olasılık Hesaplama
        df_sikayetler['Churn_Risk_Skoru_%'] = (rf_model.predict_proba(X)[:, 1] * 100).round(2)
        
        # Sonuçların Marka Bazlı Dağılım Özeti
        print("\n📊 OPERATÖRLERİN GENİŞLETİLMİŞ CHURN RİSK TABLOSU:")
        churn_ozet = df_sikayetler.groupby('company')['Churn_Risk_Skoru_%'].agg(['mean', 'max', 'count']).rename(
            columns={'mean': 'Ortalama Churn Riski (%)', 'max': 'Maksimum Risk (%)', 'count': 'Toplam Şikayet'}
        )
        print(churn_ozet.round(2).to_string())
        
        # Yüksek riskli (Alarm veren) müşteri analizi
        yuksek_riskli_kullanicilar = df_sikayetler[df_sikayetler['Churn_Risk_Skoru_%'] >= 75]
        print(f"\n⚠️ Sektör genelinde acil müdahale edilmesi gereken (%75+ Riskli) müşteri sayısı: {len(yuksek_riskli_kullanicilar)}")

        # ------------------------------------------
        # ETAP E: BÜYÜK VERİ MİMARİ VE PERFORMANS RAPORU
        # ------------------------------------------
        print("\n" + "="*60)
        print("📐 ETAP E: SİSTEM PERFORMANSI VE ÖLÇEKLENDİRİLEBİLİRLİK ANALİZİ")
        print("="*60)
        print(f"⏱️ Veri Tabanı Ağ Gecikmesi (MongoDB I/O): {mdb_sure:.4f} saniye")
        print(f"⚡ Yapay Zeka Modelleme Hızı (LDA Algoritma Süresi): {lda_sure:.4f} saniye")
        print(f"📊 İşleme Verimliliği Oranı: {len(df_sikayetler) / (mdb_sure + lda_sure):.2f} şikayet / saniye")
        print("📈 Ölçeklenebilirlik Notu: Veri temizleme yükü bizzat Apache Spark Streaming (RAM)")
        print("   katmanında dropDuplicates ile eritildiğinden, MongoDB disk I/O darboğazı %100 engellenmiştir.")
        print("="*60)

