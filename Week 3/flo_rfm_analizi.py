
###############################################################
# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)
###############################################################

###############################################################
# İş Problemi (Business Problem)
###############################################################
# FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama stratejileri belirlemek istiyor.
# Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu davranış öbeklenmelerine göre gruplar oluşturulacak..

###############################################################
# Veri Seti Hikayesi
###############################################################

# Veri seti son alışverişlerini 2020 - 2021 yıllarında OmniChannel(hem online hem offline alışveriş yapan) olarak yapan müşterilerin geçmiş alışveriş davranışlarından
# elde edilen bilgilerden oluşmaktadır.

# master_id: Eşsiz müşteri numarası
# order_channel : Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile, Offline)
# last_order_channel : En son alışverişin yapıldığı kanal
# first_order_date : Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date : Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online : Muşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline : Muşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online : Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline : Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline : Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online : Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12 : Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

###############################################################
# GÖREVLER
###############################################################

# GÖREV 1: Veriyi Anlama (Data Understanding) ve Hazırlama
           # 1. flo_data_20K.csv verisini okuyunuz. ve kopyasını çıkar

import datetime as dt
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option("display.float_format", lambda x:'%.3f' % x)
df_= pd.read_csv("datasets/datasets/flo_data_20k.csv", )
df = df_.copy()
df.head()

# 2. Veri setinde

        # a. İlk 10 gözlem,
df.head(10)
        # b. Değişken isimleri,
df.columns
        # c. Boyut,
df.shape
        # d. Betimsel istatistik,
df.describe().T
        # e. Boş değer,
df.isnull().sum()
        # f. Değişken tipleri, incelemesi yapınız.
df.info()
#ya da
df.dtypes


# 3. Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir.
# Herbir müşterinin toplam alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

### total_order_num_omnichannel= order_num_total_ever_online + order_num_total_ever_offline
### total_customer_value_omnichannel = customer_value_total_ever_online + customer_value_total_ever_offline

df.head()

df["total_order_num_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["total_order_num_omnichannel"] = df["total_order_num_omnichannel"].astype(int) # burada integera cevirdim adet olduğu için sıfırlardan kurtulmak istedim
df["total_customer_value_omnichannel"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

df.head()

# 4. Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

df.dtypes

import datetime as dt
# date_columns = ["last_order_date", "first_order_date ", "last_order_date_online", "last_order_date_offline"]
# df[date_columns] = df[date_columns].apply(pd.to_datetime) burada listeye atıp toplu bir şekilde datetime
                                                            #çevirmeye çalıştım ama hata aldım

df["last_order_date"] = df["last_order_date"].apply(pd.to_datetime)
df["first_order_date"] = df["first_order_date"].apply(pd.to_datetime)
df["last_order_date_online"] = df["last_order_date_online"].apply(pd.to_datetime)
df["last_order_date_offline"] = df["last_order_date_offline"].apply(pd.to_datetime)

df.dtypes

# 5. Alışveriş kanallarındaki müşteri sayısının, toplam alınan ürün sayısı ve toplam harcamaların dağılımına bakınız.


df.groupby('order_channel').agg({"master_id": "count","total_order_num_omnichannel":"sum", "total_customer_value_omnichannel":"sum"})

# 6. En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

df.groupby("master_id").agg({"total_customer_value_omnichannel": "sum"}).sort_values("total_customer_value_omnichannel", ascending=False).head()


# 7. En fazla siparişi veren ilk 10 müşteriyi sıralayınız.

df.groupby("master_id").agg({"total_order_num_omnichannel": "sum"}).sort_values("total_order_num_omnichannel", ascending=False).head()

# 8. Veri ön hazırlık sürecini fonksiyonlaştırınız.

def create_rfm(dataframe, csv=False):

    # VERIYI HAZIRLAMA
    df["total_order_num_omnichannel"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["total_order_num_omnichannel"] = df["total_order_num_omnichannel"].astype(int)
    df["total_customer_value_omnichannel"] = df["customer_value_total_ever_online"] + df["customer_value_total_ever_offline"]

    # RFM METRIKLERININ HESAPLANMASI
    df["last_order_date"] = df["last_order_date"].apply(pd.to_datetime)
    df["first_order_date"] = df["first_order_date"].apply(pd.to_datetime)
    df["last_order_date_online"] = df["last_order_date_online"].apply(pd.to_datetime)
    df["last_order_date_offline"] = df["last_order_date_offline"].apply(pd.to_datetime)

    df.groupby('order_channel').agg({
        "master_id": "count",
        "total_order_num_omnichannel": "sum",
        "total_customer_value_omnichannel": "sum"
    })

     df.groupby("master_id").agg({
        "total_customer_value_omnichannel": "sum"
    }).sort_values("total_customer_value_omnichannel", ascending=False).head()


    # En fazla siparişi veren ilk n müşteriyi
    df.groupby("master_id").agg({
        "total_order_num_omnichannel": "sum"
    }).sort_values("total_order_num_omnichannel", ascending=False).head()

return df


###############################################################
# GÖREV 2: RFM Metriklerinin Hesaplanması
###############################################################
# Recency, Frequency, Monetary (RFM metrikleridir)

# Veri setindeki en son alışverişin yapıldığı tarihten 2 gün sonrasını analiz tarihi
df.head()
df["last_order_date"].max() #müşterinin alışveriş yaptığı son tarih
today_date = dt.datetime(2021, 6, 1)
type(today_date)

# customer_id, recency, frequnecy ve monetary değerlerinin yer aldığı yeni bir rfm dataframe

rfm = df.groupby("master_id").agg({"last_order_date": lambda last_order_date: (today_date - last_order_date.max()).days,
                                     'total_order_num_omnichannel': lambda total_order_num_omnichannel: total_order_num_omnichannel.sum(),
                                     'total_customer_value_omnichannel': lambda total_customer_value_omnichannel: total_customer_value_omnichannel.sum()})
rfm.head()
rfm.columns = ['recency', 'frequency', 'monetary']

rfm.head()

rfm.describe().T

###############################################################
# GÖREV 3: RF ve RFM Skorlarının Hesaplanması (Calculating RF and RFM Scores)
###############################################################

#  Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çevrilmesi ve
# Bu skorları recency_score, frequency_score ve monetary_score olarak kaydedilmesi

rfm["recency_score"] = pd.qcut(rfm['recency'], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm['frequency'].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm['monetary'], 5, labels=[1, 2, 3, 4, 5])



# recency_score ve frequency_score’u tek bir değişken olarak ifade edilmesi ve RF_SCORE olarak kaydedilmesi

# r ve f değerlerini bir araya getirmek için,
rfm["RF_SCORE"] = (rfm['recency_score'].astype(str) +
                    rfm['frequency_score'].astype(str))

rfm[rfm["RF_SCORE"] == "55"]
rfm[rfm["RF_SCORE"] == "11"]

###############################################################
# GÖREV 4: RF Skorlarının Segment Olarak Tanımlanması
###############################################################

# Oluşturulan RFM skorların daha açıklanabilir olması için segment tanımlama ve  tanımlanan seg_map yardımı ile RF_SCORE'u segmentlere çevirme

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_Risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm['segment'] = rfm['RF_SCORE'].replace(seg_map, regex=True)
rfm.head()

###############################################################
# GÖREV 5: Aksiyon zamanı!
###############################################################

# 1. Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm[["segment", "recency", "frequency", "monetary"]].groupby("segment").agg("mean")

#(at_risk, cant_loose önemli
#loyal_customers'lar da benim için önemli, onlara ne yapıyorsam at risk ve cant loose
#takilere de aynısını yapmalıyım diye düşünebilirim. )

# 2. RFM analizi yardımı ile 2 case için ilgili profildeki müşterileri bulunuz ve müşteri id'lerini csv ye kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri tercihlerinin üstünde. Bu nedenle markanın
# tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak iletişime geçeilmek isteniliyor. Bu müşterilerin sadık  ve
# kadın kategorisinden alışveriş yapan kişiler olması planlandı. Müşterilerin id numaralarını csv dosyasına yeni_marka_hedef_müşteri_id.cvs
# olarak kaydediniz.

rfm.head()
df.head()

df_new = pd.merge(rfm, df, on="master_id")
df_new.head()


# Belirtilen koşullara göre filtreleme yapma
hedef_musteri_df = df_new[(df_new["segment"] == "loyal_customers") & (df_new["interested_in_categories_12"].str.contains("KADIN"))]

hedef_musteri_df.head(10)
# Hedef müşteri DataFrame'i üzerinde gerekli sütunları seçme
hedef_musteri_id_df = hedef_musteri_df[["master_id"]]

# Yeni DataFrame'i CSV dosyasına kaydetme
hedef_musteri_id_df.to_csv("yeni_marka_hedef_musteri_id.csv")

# b. Erkek ve Çoçuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte iyi müşterilerden olan ama uzun süredir
# alışveriş yapmayan ve yeni gelen müşteriler özel olarak hedef alınmak isteniliyor. Uygun profildeki müşterilerin id'lerini csv dosyasına indirim_hedef_müşteri_ids.csv
# olarak kaydediniz.

hedef2 = ((df_new["interested_in_categories_12"].str.contains("ERKEK") | df_new["interested_in_categories_12"].str.contains("COCUK")) &
         ((df_new["segment"] == "new_customers") | (df_new["segment"] == "cant_loose")))

hedefindirim =  df_new[hedef2]
hedefindirim.sample(10)

hedefindirim_id = hedefindirim[["master_id"]]
hedefindirim_id.to_csv("indirim_hedef_musteri_ids.csv")

#örnek müşteri :
df_new[df_new["master_id"] == "007cdfe4-1f54-11ea-87bf-000d3a38a36f"]

#end#