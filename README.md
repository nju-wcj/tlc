# TLC分析

## 数据整理

根据开源https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page地址，通过html原文本文件获取URL地址

下载所有表头

并对所有标签进行分析，综合分析得

1， 部分年份特征标签较少

2， 地址分为坐标和区域编号2中类型，经过评估选取区域编号作为特征

以后分析所得2017-2020年数据符合分析需求

选用2017-2020年数据，并分别对fhv，green，yellow标签进行分析：

Fhv标签

```
43         DOLocationID
49         DOlocationID
45     Dispatching_base_num
2192  Dispatching_base_number
47       DropOff_datetime
42         PULocationID
48         PUlocationID
46       Pickup_DateTime
568           SR_Flag
39     dispatching_base_num
41       dropoff_datetime
38      hvfhs_license_num
40       pickup_datetime
```

 Green标签

```
68       DOLocationID
67       PULocationID
66        RatecodeID
62         VendorID
818   congestion_surcharge
76         ehail_fee
72           extra
71        fare_amount
77   improvement_surcharge
64   lpep_dropoff_datetime
63   lpep_pickup_datetime
73          mta_tax
69      passenger_count
79       payment_type
65    store_and_fwd_flag
74        tip_amount
75       tolls_amount
78       total_amount
70       trip_distance
80         trip_type
```

Yellow标签

```
203       DOLocationID
202       PULocationID
200        RatecodeID
195         VendorID
122   congestion_surcharge
206          extra
205       fare_amount
210  improvement_surcharge
207         mta_tax
198     passenger_count
204       payment_type
201    store_and_fwd_flag
208        tip_amount
209       tolls_amount
211       total_amount
197  tpep_dropoff_datetime
196   tpep_pickup_datetime
199      trip_distance
```

 

根据标签选取所需要的特征值：

Fhv车由于有大小写区分，分别取特征值

```
Dispatching_base_num：派遣编号
pickup_datetime：上车时间
dropoff_datetime：下车时间
PULocationID：上车地点编号
DOLocationID：下车地点编号
```

 Green车，分别取特征值

```
VendorID：订单编号
lpep_pickup_datetime：上车时间
lpep_pickup_datetime：下车时间
PULocationID：上车地点编号
DOLocationID：下车地点编号
trip_distance：旅行距离
passenger_count：乘客人数
total_amount：总费用
```

Yellow车，分别取特征值

```
VendorID：订单编号
tpep_pickup_datetime：上车时间
tpep_pickup_datetime：下车时间
PULocationID：上车地点编号
DOLocationID：下车地点编号
trip_distance：旅行距离
passenger_count：乘客人数
total_amount：总费用
```

 

 