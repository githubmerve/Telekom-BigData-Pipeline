import matplotlib
# ⚠️ Sanal makinede GUI/Arayüz hatasını engellemek için Headless Backend aktif
matplotlib.use('Agg') 

from pymongo import MongoClient
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
from wordcloud import WordCloud
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import LatentDirichletAllocation

# 1. MongoDB Veri Yükleme
SUNUCU_IP = "192.168.56.110"
client = MongoClient(f"mongodb://{SUNUCU_IP}:27017/")
db = client['TelekomAnaliz']
df = pd.DataFrame(list(db['Yorumlar'].find({}, {"_id": 0})))

# 🎯 Grafiklerin doğruluğu ve istatistiksel sapmaları engellemek için kopyaları RAM'de eliyoruz
df = df.dropna(subset=['cleaned_content']).drop_duplicates(subset=['cleaned_content'])

# ⚠️ RISK SKORLARINI RAM ÜZERİNDE ANLIK HESAPLIYORUZ
churn_sinyalleri = ["iptal", "cayma", "kapatma", "geçeceğim", "bırakacağım", "tüketici hakem", "başka operatör"]
df['kritik_sinyal_sayisi'] = df['cleaned_content'].apply(lambda x: sum(1 for k in churn_sinyalleri if k in x))
df['duygu_agirligi'] = df['duygu'].map({'NEGATIF': 3, 'NOTR (TALEP)': 2, 'NOTR': 1, 'POZITIF': 0}).fillna(1)

# Skoru % bazına çekiyoruz (En fazla 100 olacak şekilde)
df['Churn_Risk_Skoru_%'] = ((df['kritik_sinyal_sayisi'] * 30) + (df['duygu_agirligi'] * 20)).clip(0, 100)

print(f"🚀 İleri Analitik Katmanı Başlatıldı. Kayıt Sayısı: {len(df)}")

# Akademik Eleme Listesi (Word Cloud'ları temizlemek için)
ortak_stop_words = [
    "müşteri", "talep", "ilgili", "üzerinden", "bana", "hizmetlerini", "hizmetleri",
    "hiçbir", "herhangi", "tarafından", "böyle", "şöyle", "bizi", "bana", "beni", "bende",
    "biri", "bunun", "buna", "bunda", "onlar", "şunlar", "böylece", "edilmesini", "istiyorum",
    "hem", "daha", "ne", "her", "aynı", "bir", "kadar", "sonra", "bütün", "hattım",
    "gibi", "için", "olan", "olarak", "neyse", "değil", "yok", "var", "şey", "mi", "mu",
    "nedeniyle", "yoksa", "başka", "bunu", "benim", "şuan", "yine", "yani", "devam", "önce"
]

# ------------------------------------------------------------
# ANALİZ 1: N-GRAM (BIGRAM) ANALİZİ
# ------------------------------------------------------------
print("📊 Analiz 1: Bigram (İkili Kelime Grubu) Analizi Yapılıyor...")
cv_bigram = CountVectorizer(ngram_range=(2, 2), max_features=15)
bigram_matrix = cv_bigram.fit_transform(df['cleaned_content'])
bigram_counts = pd.Series(np.array(bigram_matrix.sum(axis=0))[0], index=cv_bigram.get_feature_names_out()).sort_values(ascending=False)

plt.figure(figsize=(11, 6))
sns.barplot(x=bigram_counts.values, y=bigram_counts.index, hue=bigram_counts.index, palette="viridis", legend=False)
plt.title("Akademik Metin Madenciliği: En Sık Geçen İkili Kelime Grupları (Bigrams)")
plt.xlabel("Frekans")
plt.tight_layout()
plt.savefig("telekom_bigram_analizi.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# ANALİZ 2: %100 OKUNABİLİR DAİRESEL KELİME İLİŞKİ AĞI (CIRCULAR NETWORK)
# ------------------------------------------------------------
print("🌐 Analiz 2: Çakışmaları Önleyen Dairesel Kelime İlişki Ağı Çiziliyor...")
G = nx.Graph()

for phrase, weight in bigram_counts.items():
    k1, k2 = phrase.split()
    G.add_edge(k1, k2, weight=weight)

plt.figure(figsize=(12, 12), facecolor="white")
pos = nx.circular_layout(G)
nx.draw_networkx_nodes(G, pos, node_size=10, node_color="#2b5c8f", alpha=0.1)

weights = [w['weight'] for u, v, w in G.edges(data=True)]
max_weight = max(weights) if weights else 1
edge_widths = [(w / max_weight) * 4 for w in weights]
nx.draw_networkx_edges(G, pos, width=edge_widths, edge_color="#cbd5e1", alpha=0.7)

for node, (x, y) in pos.items():
    plt.text(
        x, y, s=node,
        fontsize=10, fontfamily="sans-serif", fontweight="bold",
        horizontalalignment='center', verticalalignment='center',
        bbox=dict(facecolor='white', edgecolor='#2b5c8f', boxstyle='round,pad=0.6', alpha=1.0, lw=1.5)
    )

plt.title("Sektörel Kriz Alanları Kelime İlişki Ağı (Co-occurrence Network Graph)\n[Dairesel Düzenleme ile Okunabilirliği Optimize Edilmiş Model]", fontsize=13, fontweight="bold", pad=25)
plt.axis('off')
plt.tight_layout()
plt.savefig("telekom_kelime_agi.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# ANALİZ 3: MARKA BAZLI ENTEGRE WORD CLOUD ANALİZİ
# ------------------------------------------------------------
print("☁️ Analiz 3: Operatör Bazlı Kelime Bulutları (Word Cloud) Üretiliyor...")
marka_gruplari = df.groupby('company')['cleaned_content'].apply(lambda x: ' '.join(x)).reset_index()

for i, row in marka_gruplari.iterrows():
    marka = row['company']
    metin = row['cleaned_content']
    
    temiz_kelimeler = [k for k in metin.split() if k not in ortak_stop_words and len(k) > 2]
    temiz_metin = ' '.join(temiz_kelimeler)
    
    wc = WordCloud(
        width=800, height=450, 
        background_color="white", 
        colormap="tab10", 
        max_words=60, 
        random_state=42
    ).generate(temiz_metin if len(temiz_metin) > 10 else metin)
    
    plt.figure(figsize=(10, 6))
    plt.imshow(wc, interpolation='bilinear')
    plt.title(f"Operatör Kriz Odak Alanı Analizi: {marka} (Word Cloud)", fontsize=14, fontweight="bold", pad=15)
    plt.axis("off")
    plt.tight_layout(pad=0)
    plt.savefig(f"wordcloud_{marka.lower()}.png", dpi=300)
    plt.close()

# ------------------------------------------------------------
# ANALİZ 4: OPERATÖR BAZLI KRİZ YOĞUNLUĞU HEATMAP
# ------------------------------------------------------------
print("🔥 Analiz 4: Kriz Yoğunluğu Heatmap Analizi Yapılıyor...")
df['Risk_Grubu'] = pd.cut(df['Churn_Risk_Skoru_%'], bins=[-1, 40, 75, 101], labels=['Düşük Risk', 'Orta Risk', 'Kritik Risk'])
heatmap_data = pd.crosstab(df['company'], df['Risk_Grubu'], normalize='index') * 100

plt.figure(figsize=(9, 5))
sns.heatmap(heatmap_data, annot=True, fmt=".2f", cmap="Reds", cbar_kws={'label': 'Yüzde (%)'}, annot_kws={"size": 11, "weight": "bold"})
plt.title("Operatörlere Göre Churn Risk Grupları Yoğunluk Haritası (Heatmap)")
plt.ylabel("Operatör")
plt.xlabel("Risk Seviyesi")
plt.tight_layout()
plt.savefig("telekom_kriz_heatmap.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 🎯 ANALİZ 5: OPERATÖR BAZLI ŞIKAYET SAYILARI BAR CHART (YENİ)
# ------------------------------------------------------------
print("📊 Analiz 5: Operatör Bazlı Şikayet Sayıları Grafiği Çiziliyor...")
plt.figure(figsize=(8, 5))
op_counts = df['company'].value_counts()
sns.barplot(x=op_counts.index, y=op_counts.values, hue=op_counts.index, palette="Blues_r", legend=False)

for idx, val in enumerate(op_counts.values):
    plt.text(idx, val + (max(op_counts.values)*0.01), f"{val:,}", ha='center', va='bottom', fontweight='bold', fontsize=11)

plt.title("Büyük Veri Havuzu: Operatörlere Göre Toplam Şikayet Hacmi Dağılımı", fontsize=12, fontweight="bold", pad=15)
plt.ylabel("Toplam Şikayet Sayısı", fontsize=10)
plt.xlabel("Telekomünikasyon Operatörü", fontsize=10)
plt.grid(axis='y', linestyle='--', alpha=0.3)
plt.tight_layout()
plt.savefig("telekom_operator_dagilimi.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 🎯 ANALİZ 6: LDA KONU DAĞILIMI PASTA GRAFİĞİ (YENİ)
# ------------------------------------------------------------
print("🤖 Analiz 6: Yapay Zeka LDA Konu Dağılımı Pasta Grafiği Çiziliyor...")
vektorlestirici = CountVectorizer(stop_words=ortak_stop_words, max_features=1000, min_df=5)
kelime_matrisi_lda = vektorlestirici.fit_transform(df['cleaned_content'])
lda_model = LatentDirichletAllocation(n_components=3, random_state=42, learning_method='online')
konu_dagilimlari = lda_model.fit_transform(kelime_matrisi_lda)

df['Baskın_Konu_No'] = konu_dagilimlari.argmax(axis=1) + 1
konu_haritasi = {1: 'Tarife/Paket Politikası', 2: 'Fatura/İptal Krizleri', 3: 'Altyapı/Arıza Sorunları'}
df['Konu_Adi'] = df['Baskın_Konu_No'].map(konu_haritasi)
konu_counts = df['Konu_Adi'].value_counts()

plt.figure(figsize=(8, 8))
colors = ['#f87171', '#60a5fa', '#34d399']
plt.pie(konu_counts.values, labels=konu_counts.index, autopct='%1.2f%%', startangle=140, 
        colors=colors, textprops={'fontsize': 11, 'fontweight': 'bold'}, wedgeprops={'edgecolor': 'white', 'linewidth': 2})
plt.title("Yapay Zeka (LDA) Tarafından Ayrıştırılan\nSektörel Kriz Alanları Genel Dağılımı", fontsize=13, fontweight="bold", pad=20)
plt.tight_layout()
plt.savefig("telekom_lda_konu_dagilimi.png", dpi=300)
plt.close()

# ------------------------------------------------------------
# 🎯 ANALİZ 7: CHURN RİSK GRUPLARI PASTA GRAFİĞİ (YENİ)
# ------------------------------------------------------------
print("🚨 Analiz 7: Churn Risk Grupları Pasta Grafiği Çiziliyor...")
risk_counts = df['Risk_Grubu'].value_counts().reindex(['Düşük Risk', 'Orta Risk', 'Kritik Risk'])

plt.figure(figsize=(8, 8))
risk_colors = ['#a7f3d0', '#fef08a', '#fca5a5']
explode = (0, 0, 0.05)

plt.pie(risk_counts.values, explode=explode, labels=risk_counts.index, autopct='%1.2f%%', startangle=140,
        colors=risk_colors, textprops={'fontsize': 11, 'fontweight': 'bold'}, wedgeprops={'edgecolor': 'white', 'linewidth': 1.5})
plt.title("Müşteri Kayıp Riski (Churn Risk Rate) Genel Dağılımı", fontsize=13, fontweight="bold", pad=20)
plt.tight_layout()
plt.savefig("telekom_churn_risk_pasta.png", dpi=300)
plt.close()

print("✅ Entegre analiz tamamlandı. Analiz 1'den 7'ye kadar olan tüm kurumsal grafikler başarıyla diske kaydedildi!")

