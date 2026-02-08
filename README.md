Project Big Data ini adalah sistem monitoring dan analisis traffic Jakarta secara real-time yang mengintegrasikan:

Data traffic simulasi berdasarkan pattern jam puncak Jakarta Data cuaca real-time dari Open-Meteo API Dashboard interaktif menggunakan Streamlit Analisis pattern traffic per jam, lokasi, dan kondisi cuaca

Project ini mendemonstrasikan konsep 5V Big Data:

Volume: 40.000+ records data historis 

Velocity: Real-time monitoring & update 

Variety: Traffic data, weather data, analytics 

Veracity: Data dari API eksternal (Open-Meteo) 

Value: Insight untuk prediksi kemacetan Jakarta

🎯 Fitur Utama

📊 Dashboard Utama

Status traffic real-time untuk 5 wilayah Jakarta Visualisasi pattern kendaraan per jam (24 jam) Top 10 kemacetan terbesar Statistik agregat (avg kendaraan, kecepatan, dll)

🌤️ Monitoring Cuaca Real-Time

Integrasi dengan Open-Meteo API Data cuaca 5 wilayah Jakarta (Pusat, Utara, Selatan, Timur, Barat) Kategori hujan: None, Light, Moderate, Heavy, Extreme Pengaruh cuaca terhadap traffic density

📋 Data Raw & Export

View data traffic & weather Filter berdasarkan lokasi Download CSV untuk analisis lanjutan

🔄 Simulasi Real-Time

Auto-generate traffic data berdasarkan: Pattern jam puncak (06:00-09:00, 16:00-19:00) Kondisi cuaca (hujan = +30% - +100% traffic) Variasi per lokasi

🛠️ Teknologi yang Digunakan

Teknologi Fungsi Python 3.8+ Backend & logic Streamlit Web dashboard framework SQLite Database (built-in, tanpa server) Pandas Data processing & analytics NumPy Numerical operations Matplotlib Data visualization Requests API calls (Open-Meteo) Scikit-learn Machine learning analytics

🚀 Cara Install & Menjalankan

1️⃣ Clone Repository bashgit clone https://github.com/4andreas/-Bigdata-Traffic-Jakarta.git cd -Bigdata-Traffic-Jakarta

2️⃣ Install Dependencies Pastikan Python 3.8+ sudah terinstall, lalu: bashpip install -r requirements.txt Atau install manual: bashpip install pandas numpy requests streamlit matplotlib scikit-learn

3️⃣ Jalankan Aplikasi bashstreamlit run app.py Dashboard akan terbuka otomatis di browser pada: http://localhost:8501

💡 Cara Menggunakan

Pertama Kali Dijalankan

Aplikasi akan auto-generate data historis (30 hari terakhir)

Proses ini memakan waktu 30-60 detik Menghasilkan 500,000+ data points Hanya dijalankan sekali (saat database kosong)

Dashboard siap digunakan!

Fitur-Fitur Dashboard

🔄 Refresh Simulasi

Klik tombol "Refresh Simulasi" di sidebar Generate traffic data baru berdasarkan waktu & cuaca saat ini

🌤️ Refresh Cuaca

Klik tombol "Refresh Cuaca" di sidebar Fetch data cuaca real-time dari Open-Meteo API

📍 Filter Lokasi

Pilih lokasi spesifik di dropdown Lihat data untuk 1 wilayah saja

📥 Download Data

Buka tab "Data Raw" Klik "Download CSV" untuk export data

Pattern Traffic Jam Puncak: Pagi: 06:00 - 09:00 (commute ke kantor) Sore: 16:00 - 19:00 (pulang kantor)

Kategori Kondisi Traffic:

KondisiRange Kendaraan

🟢 Lancar 0 - 100

🟡 Sedang 100 - 200

🟠 Padat 200 - 350

🔴 Sangat Padat 350 - 500

⚫ Macet 500+

Pengaruh Hujan

Kategori Impact Factor Keterangan None 1.0x Tidak ada pengaruh Light 1.3x +30% traffic Moderate 1.6x +60% traffic Heavy 1.8x +80% traffic Extreme 2.0x +100% traffic

🌐 API yang Digunakan

Open-Meteo Weather API URL: https://api.open-meteo.com/v1/forecast

Gratis: Tidak perlu API key Data: Temperature, precipitation, windspeed, weather code

Timezone: Asia/Jakarta
