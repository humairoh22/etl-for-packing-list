# Overview
Projek simple ETL untuk menyediakan data surat jalan yang akan dibuatkan packing list di web delivery monitoring.

# Problem
Operasional Supply Chain Manager request untuk dibuatkan suatu program yang dapat meningkatkan efisiensi pekerjaan admin transport dalam hal menyiapkan data surat jalan yang akan dibuatkan packing list. Selama ini admin transport
melakukan input data secara manual dari sistem Accurate ke web delivery monitoring yang di mana membutuhkan waktu +/- 1 jam untuk proses penginputan data.

# Solution
- Membuat script python untuk mengekstrak, transform, dan load data dari hasil ekspor data di Accurate berbentuk file Excel ke dalam database.
- ETL script nantinya akan otomatis running setiap file Excel diupdate atau dioverwrite.
