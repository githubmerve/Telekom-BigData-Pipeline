#!/bin/bash
# 1. Kendi sanal ortamının bulunduğu dizine git ve onu aktif et
source /home/vboxuser/Desktop/analiz_venv/bin/activate

# 2. Kodunun olduğu masaüstü dizinine geç
cd /home/vboxuser/Desktop

# 3. Senin orijinal analiz kodunu çalıştır
python3 telekom_analiz.py

