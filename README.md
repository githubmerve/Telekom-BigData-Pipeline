# TELEKOMÜNİKASYON SEKTÖRÜNDEKİ MÜŞTERİ ŞİKAYETLERİNİN BÜYÜK VERİ VE WEB MADENCİLİĞİ YÖNTEMLERİYLE ANALİZİ


Bu proje, telekomünikasyon sektöründeki (Turkcell, Vodafone, Türk Telekom) müşteri krizlerini ve memnuniyetsizliklerini uçtan uca analiz etmek amacıyla geliştirilmiş, 3 düğümlü (multi-node) dağıtık bir büyük veri mimarisidir.

Sistem; web kazıma (Web Scraping), mesaj kuyruklama (Message Queuing), gerçek zamanlı akış işleme (Stream Processing), NoSQL veri depolama ve ileri analitik/makine öğrenmesi katmanlarını tek bir mimaride birleştirmektedir.

---

# 1. Sistem Mimarisi ve Teknoloji Yığını

Proje, birbirleriyle ağ üzerinden (Host-Only Network) haberleşen üç farklı Ubuntu sanal makinesi üzerinde dağıtık olarak çalışmaktadır.

## Proje Yapısı

```text
Telekom-BigData-Pipeline
│
├── 01-Data-Crawler
│   ├── nstream_crawler.py
│   └── nspecial_stream.py
│
├── 02-Kafka-Consumer-Producer
│   └── spark_cleaner.py
│
├── 03-Storage-Analytics
│   ├── telekom_analiz.py
│   ├── telekom_gorsel.py
│   └── run_analysis.sh
│
└── data
    └── Telekom_Sikayet_Veriseti.csv
```

## Kullanılan Teknolojiler

| Katman                 | Teknoloji                        |
| ---------------------- | -------------------------------- |
| Veri Kazıma            | Python, Selenium WebDriver       |
| Mesaj Kuyruğu          | Apache Kafka, Zookeeper          |
| Büyük Veri İşleme      | Apache Spark Streaming (PySpark) |
| Veri Depolama          | MongoDB NoSQL Database           |
| Yapay Zeka ve Analitik | Scikit-Learn, LDA, NetworkX      |
| Görselleştirme         | Matplotlib, Seaborn              |
| Otomasyon              | Linux Cron Jobs, Bash Scripting  |

---

# 2. Veri Akış Hattı (Data Pipeline)

## Etap 1: Veri Toplama ve Kuyruklama (Node 1)

`nstream_crawler.py` ve `nspecial_stream.py` botları, Şikayetvar platformundaki operatör sayfalarını geriye dönük ve canlı olarak eş zamanlı tarar.

Toplanan veriler JSON formatına dönüştürülerek Apache Kafka üzerindeki `sikayetler` topic'ine gerçek zamanlı olarak gönderilir.

### İş Akışı

1. Şikayetvar sayfalarının taranması
2. Ham verilerin çıkarılması
3. JSON formatına dönüştürülmesi
4. Kafka Topic'e aktarılması

---

## Etap 2: Gerçek Zamanlı Büyük Veri Ön İşleme ve NLP (Node 2)

Apache Spark Streaming katmanı Kafka kuyruğunu sürekli dinleyerek gelen verileri işler.

### Gerçekleştirilen İşlemler

* Küçük harfe dönüştürme
* Regex tabanlı noktalama temizleme
* Türkçe stop-word temizliği
* Veri standardizasyonu
* Gerçek zamanlı duygu analizi

### Duygu Analizi Çıktıları

* NEGATIF
* POZITIF
* NOTR
* NOTR (TALEP)

### Veri Depolama

Temizlenmiş ve zenginleştirilmiş kayıtlar MongoDB üzerinde depolanmaktadır.

---

## Etap 3: Periyodik Gelişmiş Modelleme ve Otomasyon (Node 3)

Linux Cron Job sistemi her iki saatte bir çalışarak analiz sürecini otomatik olarak başlatmaktadır.

Çalıştırılan bileşenler:

```bash
run_analysis.sh
├── telekom_analiz.py
└── telekom_gorsel.py
```

Bu aşamada MongoDB'den güncel veriler çekilir ve gelişmiş analizler gerçekleştirilir.

---

# 3. Gelişmiş Analitik ve Yapay Zeka Modelleri

## A. TF-IDF Analizi

TF-IDF yöntemi kullanılarak her operatöre özgü ayırt edici kriz terimleri belirlenmektedir.

## B. LDA (Latent Dirichlet Allocation) Konu Modellemesi

46 binden fazla benzersiz müşteri şikayeti, denetimsiz öğrenme yöntemi ile üç temel kriz başlığı altında gruplanmaktadır:

* Tarife ve Paket Problemleri
* Fatura ve İptal Süreçleri
* Altyapı ve Arıza Problemleri

## C. Random Forest ile Churn Risk Tahminleme

Makine öğrenmesi modeli müşteri kaybı riski taşıyan kullanıcıları belirlemek amacıyla geliştirilmiştir.

Süreç aşağıdaki adımlardan oluşmaktadır:

1. Özellik çıkarımı (Feature Engineering)
2. Hedef değişken oluşturulması
3. Model eğitimi
4. Risk skorlarının hesaplanması

---

# 4. Metin Madenciliği ve Görselleştirmeler

`telekom_gorsel.py` modülü tarafından aşağıdaki grafikler otomatik olarak üretilmektedir:

* Kelime birliktelik ağı (Co-occurrence Network Graph)
* Risk yoğunluk haritası (Heatmap)
* Operatör bazlı kelime bulutları (Word Clouds)
* Konu ve risk dağılım pasta grafikleri

---

# 5. Sistem Performansı ve Ölçeklenebilirlik

## MongoDB Disk I/O Optimizasyonu

Veri temizleme ve kopya kayıtların kaldırılması işlemleri Spark katmanında gerçekleştirilmektedir. Böylece veritabanı üzerindeki yük azaltılmaktadır.

## İşleme Performansı

| Metrik            | Değer                    |
| ----------------- | ------------------------ |
| Toplam Kayıt      | 48.895+                  |
| Benzersiz Şikayet | 46.715+                  |
| İşleme Hızı       | 350–400 Şikayet/Saniye   |
| Mimari            | 3 Düğümlü Dağıtık Sistem |

---

# Proje Kapsamı

Bu proje aşağıdaki alanları tek bir sistemde birleştirmektedir:

* Büyük Veri Sistemleri
* Apache Kafka
* Apache Spark Streaming
* MongoDB
* Doğal Dil İşleme (NLP)
* Metin Madenciliği
* Konu Modellemesi (LDA)
* Makine Öğrenmesi
* Churn Analizi
* Dağıtık Sistemler
* Linux Otomasyonu
* Veri Görselleştirme

Bu yönüyle proje, gerçek zamanlı müşteri geri bildirimlerinin analiz edilmesi ve müşteri kaybı risklerinin belirlenmesi amacıyla geliştirilmiş uçtan uca bir büyük veri analitik platformudur.
