CREATE TABLE access_logs (
    timestamp    DateTime64(3),
    event_type   LowCardinality(String),
    user_id      UInt64,
    ip_address   IPv4,
    url          String,
    duration_ms  UInt32
)
ENGINE = MergeTree()
ORDER BY (timestamp, user_id);

CREATE TABLE IF NOT EXISTS sales_var001 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       LowCardinality(String),
    customer_id    UInt64,
    region         LowCardinality(String),
    quantity       UInt16,
    unit_price     Decimal64(2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     IPv4
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);


INSERT INTO db_1.sales_var001 (sale_id, sale_timestamp, product_id, category, customer_id, region, quantity, unit_price, discount_pct, is_online, ip_address) VALUES
(1001, '2024-05-01 10:00:00.000', 10, 'electronics', 100, 'Moscow', 3, 50.00, 0.00, 1, '192.168.1.1'),
(1002, '2024-05-05 11:30:00.000', 11, 'clothing', 101, 'SPB', 2, 30.00, 0.10, 1, '192.168.1.2'),
(1003, '2024-05-10 14:20:00.000', 12, 'books', 102, 'Moscow', 1, 15.00, 0.05, 0, '192.168.1.3'),
(1004, '2024-05-15 09:45:00.000', 13, 'electronics', 103, 'Kazan', 4, 25.00, 0.00, 1, '192.168.1.4'),
(1005, '2024-05-20 16:30:00.000', 14, 'clothing', 104, 'SPB', 2, 45.00, 0.15, 1, '192.168.1.5'),
(1006, '2024-05-25 12:00:00.000', 15, 'books', 105, 'Moscow', 3, 12.00, 0.00, 0, '192.168.1.6'),
(1007, '2024-05-28 18:15:00.000', 16, 'electronics', 106, 'Kazan', 5, 80.00, 0.20, 1, '192.168.1.7'),
(1008, '2024-05-30 13:40:00.000', 17, 'clothing', 107, 'SPB', 2, 35.00, 0.05, 1, '192.168.1.8'),
(1009, '2024-05-31 08:20:00.000', 18, 'books', 108, 'Moscow', 1, 20.00, 0.00, 0, '192.168.1.9'),
(1010, '2024-05-31 19:00:00.000', 19, 'electronics', 109, 'Kazan', 3, 60.00, 0.10, 1, '192.168.1.10'),
(1011, '2024-06-02 10:00:00.000', 10, 'electronics', 110, 'Moscow', 2, 50.00, 0.00, 1, '10.0.0.1'),
(1012, '2024-06-05 11:30:00.000', 11, 'clothing', 111, 'SPB', 3, 30.00, 0.10, 1, '10.0.0.2'),
(1013, '2024-06-08 14:20:00.000', 12, 'books', 112, 'Moscow', 1, 15.00, 0.05, 0, '10.0.0.3'),
(1014, '2024-06-12 09:45:00.000', 13, 'electronics', 113, 'Kazan', 4, 25.00, 0.00, 1, '10.0.0.4'),
(1015, '2024-06-15 16:30:00.000', 14, 'clothing', 114, 'SPB', 2, 45.00, 0.15, 1, '10.0.0.5'),
(1016, '2024-06-18 12:00:00.000', 15, 'books', 115, 'Moscow', 3, 12.00, 0.00, 0, '10.0.0.6'),
(1017, '2024-06-20 18:15:00.000', 16, 'electronics', 116, 'Kazan', 5, 80.00, 0.20, 1, '10.0.0.7'),
(1018, '2024-06-22 13:40:00.000', 17, 'clothing', 117, 'SPB', 2, 35.00, 0.05, 1, '10.0.0.8'),
(1019, '2024-06-25 08:20:00.000', 18, 'books', 118, 'Moscow', 1, 20.00, 0.00, 0, '10.0.0.9'),
(1020, '2024-06-28 19:00:00.000', 19, 'electronics', 119, 'Kazan', 3, 60.00, 0.10, 1, '10.0.0.10'),
(1021, '2024-07-01 10:00:00.000', 20, 'electronics', 120, 'Moscow', 4, 50.00, 0.00, 1, '172.16.0.1'),
(1022, '2024-07-03 11:30:00.000', 21, 'clothing', 121, 'SPB', 2, 30.00, 0.10, 1, '172.16.0.2'),
(1023, '2024-07-06 14:20:00.000', 22, 'books', 122, 'Moscow', 3, 15.00, 0.05, 0, '172.16.0.3'),
(1024, '2024-07-09 09:45:00.000', 23, 'electronics', 123, 'Kazan', 1, 25.00, 0.00, 1, '172.16.0.4'),
(1025, '2024-07-12 16:30:00.000', 24, 'clothing', 124, 'SPB', 5, 45.00, 0.15, 1, '172.16.0.5'),
(1026, '2024-07-15 12:00:00.000', 25, 'books', 125, 'Moscow', 2, 12.00, 0.00, 0, '172.16.0.6'),
(1027, '2024-07-18 18:15:00.000', 26, 'electronics', 126, 'Kazan', 3, 80.00, 0.20, 1, '172.16.0.7'),
(1028, '2024-07-20 13:40:00.000', 27, 'clothing', 127, 'SPB', 4, 35.00, 0.05, 1, '172.16.0.8'),
(1029, '2024-07-22 08:20:00.000', 28, 'books', 128, 'Moscow', 1, 20.00, 0.00, 0, '172.16.0.9'),
(1030, '2024-07-25 19:00:00.000', 29, 'electronics', 129, 'Kazan', 2, 60.00, 0.10, 1, '172.16.0.10'),
(1031, '2024-07-28 10:00:00.000', 10, 'electronics', 130, 'Moscow', 3, 50.00, 0.00, 1, '172.16.0.11'),
(1032, '2024-07-30 11:30:00.000', 11, 'clothing', 131, 'SPB', 2, 30.00, 0.10, 1, '172.16.0.12');
(1035, '2024-05-01 12:15:00.000', 12, 'books', 102, 'Moscow', 1, 15.00, 0.05, 0, '192.168.1.3'),
(1036, '2024-05-02 09:00:00.000', 13, 'electronics', 103, 'Kazan', 4, 25.00, 0.00, 1, '192.168.1.4'),
(1037, '2024-05-02 11:00:00.000', 14, 'clothing', 104, 'SPB', 2, 45.00, 0.15, 1, '192.168.1.5'),
(1038, '2024-05-02 14:30:00.000', 15, 'books', 105, 'Moscow', 3, 12.00, 0.00, 0, '192.168.1.6'),
(1039, '2024-05-03 08:00:00.000', 16, 'electronics', 106, 'Kazan', 5, 80.00, 0.20, 1, '192.168.1.7'),
(1040, '2024-05-03 10:00:00.000', 17, 'clothing', 107, 'SPB', 2, 35.00, 0.05, 1, '192.168.1.8'),
(1041, '2024-05-03 13:00:00.000', 18, 'books', 108, 'Moscow', 1, 20.00, 0.00, 0, '192.168.1.9'),
(1042, '2024-05-04 09:30:00.000', 19, 'electronics', 109, 'Kazan', 3, 60.00, 0.10, 1, '192.168.1.10'),
(1043, '2024-05-04 12:00:00.000', 10, 'electronics', 110, 'Moscow', 2, 50.00, 0.00, 1, '10.0.0.1'),
(1044, '2024-05-04 15:30:00.000', 11, 'clothing', 111, 'SPB', 3, 30.00, 0.10, 1, '10.0.0.2'),
(1045, '2024-05-05 08:00:00.000', 12, 'books', 112, 'Moscow', 1, 15.00, 0.05, 0, '10.0.0.3'),
(1046, '2024-05-05 10:30:00.000', 13, 'electronics', 113, 'Kazan', 4, 25.00, 0.00, 1, '10.0.0.4'),
(1047, '2024-05-05 13:00:00.000', 14, 'clothing', 114, 'SPB', 2, 45.00, 0.15, 1, '10.0.0.5'),
(1048, '2024-05-06 09:00:00.000', 15, 'books', 115, 'Moscow', 3, 12.00, 0.00, 0, '10.0.0.6'),
(1049, '2024-05-06 11:30:00.000', 16, 'electronics', 116, 'Kazan', 5, 80.00, 0.20, 1, '10.0.0.7'),
(1050, '2024-05-06 14:00:00.000', 17, 'clothing', 117, 'SPB', 2, 35.00, 0.05, 1, '10.0.0.8'),
(1051, '2024-05-07 08:00:00.000', 18, 'books', 118, 'Moscow', 1, 20.00, 0.00, 0, '10.0.0.9'),
(1052, '2024-05-07 10:30:00.000', 19, 'electronics', 119, 'Kazan', 3, 60.00, 0.10, 1, '10.0.0.10'),
(1053, '2024-05-07 13:00:00.000', 10, 'electronics', 120, 'Moscow', 4, 50.00, 0.00, 1, '172.16.0.1'),
(1054, '2024-05-08 09:00:00.000', 11, 'clothing', 121, 'SPB', 2, 30.00, 0.10, 1, '172.16.0.2'),
(1055, '2024-05-08 11:30:00.000', 12, 'books', 122, 'Moscow', 3, 15.00, 0.05, 0, '172.16.0.3'),
(1056, '2024-05-08 14:00:00.000', 13, 'electronics', 123, 'Kazan', 1, 25.00, 0.00, 1, '172.16.0.4'),
(1057, '2024-05-09 08:30:00.000', 14, 'clothing', 124, 'SPB', 5, 45.00, 0.15, 1, '172.16.0.5'),
(1058, '2024-05-09 11:00:00.000', 15, 'books', 125, 'Moscow', 2, 12.00, 0.00, 0, '172.16.0.6'),
(1059, '2024-05-09 13:30:00.000', 16, 'electronics', 126, 'Kazan', 3, 80.00, 0.20, 1, '172.16.0.7'),
(1060, '2024-05-10 09:00:00.000', 17, 'clothing', 127, 'SPB', 4, 35.00, 0.05, 1, '172.16.0.8'),
(1061, '2024-05-10 11:30:00.000', 18, 'books', 128, 'Moscow', 1, 20.00, 0.00, 0, '172.16.0.9'),
(1062, '2024-05-10 14:00:00.000', 19, 'electronics', 129, 'Kazan', 2, 60.00, 0.10, 1, '172.16.0.10'),
(1063, '2024-06-01 09:00:00.000', 10, 'electronics', 130, 'Moscow', 3, 50.00, 0.00, 1, '192.168.2.1'),
(1064, '2024-06-01 10:30:00.000', 11, 'clothing', 131, 'SPB', 2, 30.00, 0.10, 1, '192.168.2.2'),
(1065, '2024-06-01 12:15:00.000', 12, 'books', 132, 'Moscow', 1, 15.00, 0.05, 0, '192.168.2.3'),
(1066, '2024-06-02 09:00:00.000', 13, 'electronics', 133, 'Kazan', 4, 25.00, 0.00, 1, '192.168.2.4'),
(1067, '2024-06-02 11:00:00.000', 14, 'clothing', 134, 'SPB', 2, 45.00, 0.15, 1, '192.168.2.5'),
(1068, '2024-06-02 14:30:00.000', 15, 'books', 135, 'Moscow', 3, 12.00, 0.00, 0, '192.168.2.6'),
(1069, '2024-06-03 08:00:00.000', 16, 'electronics', 136, 'Kazan', 5, 80.00, 0.20, 1, '192.168.2.7'),
(1070, '2024-06-03 10:00:00.000', 17, 'clothing', 137, 'SPB', 2, 35.00, 0.05, 1, '192.168.2.8'),
(1071, '2024-06-03 13:00:00.000', 18, 'books', 138, 'Moscow', 1, 20.00, 0.00, 0, '192.168.2.9'),
(1072, '2024-06-04 09:30:00.000', 19, 'electronics', 139, 'Kazan', 3, 60.00, 0.10, 1, '192.168.2.10'),
(1073, '2024-06-04 12:00:00.000', 10, 'electronics', 140, 'Moscow', 2, 50.00, 0.00, 1, '10.0.1.1'),
(1074, '2024-06-04 15:30:00.000', 11, 'clothing', 141, 'SPB', 3, 30.00, 0.10, 1, '10.0.1.2'),
(1075, '2024-06-05 08:00:00.000', 12, 'books', 142, 'Moscow', 1, 15.00, 0.05, 0, '10.0.1.3'),
(1076, '2024-06-05 10:30:00.000', 13, 'electronics', 143, 'Kazan', 4, 25.00, 0.00, 1, '10.0.1.4'),
(1077, '2024-06-05 13:00:00.000', 14, 'clothing', 144, 'SPB', 2, 45.00, 0.15, 1, '10.0.1.5'),
(1078, '2024-06-06 09:00:00.000', 15, 'books', 145, 'Moscow', 3, 12.00, 0.00, 0, '10.0.1.6'),
(1079, '2024-06-06 11:30:00.000', 16, 'electronics', 146, 'Kazan', 5, 80.00, 0.20, 1, '10.0.1.7'),
(1080, '2024-06-06 14:00:00.000', 17, 'clothing', 147, 'SPB', 2, 35.00, 0.05, 1, '10.0.1.8'),
(1081, '2024-06-07 08:00:00.000', 18, 'books', 148, 'Moscow', 1, 20.00, 0.00, 0, '10.0.1.9'),
(1082, '2024-06-07 10:30:00.000', 19, 'electronics', 149, 'Kazan', 3, 60.00, 0.10, 1, '10.0.1.10'),
(1083, '2024-07-01 09:00:00.000', 10, 'electronics', 150, 'Moscow', 4, 50.00, 0.00, 1, '172.16.1.1'),
(1084, '2024-07-01 10:30:00.000', 11, 'clothing', 151, 'SPB', 2, 30.00, 0.10, 1, '172.16.1.2'),
(1085, '2024-07-01 12:15:00.000', 12, 'books', 152, 'Moscow', 3, 15.00, 0.05, 0, '172.16.1.3'),
(1086, '2024-07-02 09:00:00.000', 13, 'electronics', 153, 'Kazan', 1, 25.00, 0.00, 1, '172.16.1.4'),
(1087, '2024-07-02 11:00:00.000', 14, 'clothing', 154, 'SPB', 5, 45.00, 0.15, 1, '172.16.1.5'),
(1088, '2024-07-02 14:30:00.000', 15, 'books', 155, 'Moscow', 2, 12.00, 0.00, 0, '172.16.1.6'),
(1089, '2024-07-03 08:00:00.000', 16, 'electronics', 156, 'Kazan', 3, 80.00, 0.20, 1, '172.16.1.7'),
(1090, '2024-07-03 10:00:00.000', 17, 'clothing', 157, 'SPB', 4, 35.00, 0.05, 1, '172.16.1.8'),
(1091, '2024-07-03 13:00:00.000', 18, 'books', 158, 'Moscow', 1, 20.00, 0.00, 0, '172.16.1.9'),
(1092, '2024-07-04 09:30:00.000', 19, 'electronics', 159, 'Kazan', 2, 60.00, 0.10, 1, '172.16.1.10'),
(1093, '2024-07-04 12:00:00.000', 10, 'electronics', 160, 'Moscow', 3, 50.00, 0.00, 1, '192.168.3.1'),
(1094, '2024-07-04 15:30:00.000', 11, 'clothing', 161, 'SPB', 2, 30.00, 0.10, 1, '192.168.3.2'),
(1095, '2024-07-05 08:00:00.000', 12, 'books', 162, 'Moscow', 1, 15.00, 0.05, 0, '192.168.3.3'),
(1096, '2024-07-05 10:30:00.000', 13, 'electronics', 163, 'Kazan', 4, 25.00, 0.00, 1, '192.168.3.4'),
(1097, '2024-07-05 13:00:00.000', 14, 'clothing', 164, 'SPB', 2, 45.00, 0.15, 1, '192.168.3.5'),
(1098, '2024-07-06 09:00:00.000', 15, 'books', 165, 'Moscow', 3, 12.00, 0.00, 0, '192.168.3.6'),
(1099, '2024-07-06 11:30:00.000', 16, 'electronics', 166, 'Kazan', 5, 80.00, 0.20, 1, '192.168.3.7'),
(1100, '2024-07-06 14:00:00.000', 17, 'clothing', 167, 'SPB', 2, 35.00, 0.05, 1, '192.168.3.8'),
(1101, '2024-07-07 08:00:00.000', 18, 'books', 168, 'Moscow', 1, 20.00, 0.00, 0, '192.168.3.9'),
(1102, '2024-07-07 10:30:00.000', 19, 'electronics', 169, 'Kazan', 3, 60.00, 0.10, 1, '192.168.3.10'),
(1103, '2024-08-01 09:00:00.000', 10, 'electronics', 170, 'Moscow', 2, 50.00, 0.00, 1, '10.0.2.1'),
(1104, '2024-08-01 10:30:00.000', 11, 'clothing', 171, 'SPB', 3, 30.00, 0.10, 1, '10.0.2.2'),
(1105, '2024-08-01 12:15:00.000', 12, 'books', 172, 'Moscow', 1, 15.00, 0.05, 0, '10.0.2.3'),
(1106, '2024-08-02 09:00:00.000', 13, 'electronics', 173, 'Kazan', 4, 25.00, 0.00, 1, '10.0.2.4'),
(1107, '2024-08-02 11:00:00.000', 14, 'clothing', 174, 'SPB', 2, 45.00, 0.15, 1, '10.0.2.5'),
(1108, '2024-08-02 14:30:00.000', 15, 'books', 175, 'Moscow', 3, 12.00, 0.00, 0, '10.0.2.6'),
(1109, '2024-08-03 08:00:00.000', 16, 'electronics', 176, 'Kazan', 5, 80.00, 0.20, 1, '10.0.2.7'),
(1110, '2024-08-03 10:00:00.000', 17, 'clothing', 177, 'SPB', 2, 35.00, 0.05, 1, '10.0.2.8'),
(1111, '2024-08-03 13:00:00.000', 18, 'books', 178, 'Moscow', 1, 20.00, 0.00, 0, '10.0.2.9'),
(1112, '2024-08-04 09:30:00.000', 19, 'electronics', 179, 'Kazan', 3, 60.00, 0.10, 1, '10.0.2.10'),
(1113, '2024-08-04 12:00:00.000', 10, 'electronics', 180, 'Moscow', 4, 50.00, 0.00, 1, '172.16.2.1'),
(1114, '2024-08-04 15:30:00.000', 11, 'clothing', 181, 'SPB', 2, 30.00, 0.10, 1, '172.16.2.2'),
(1115, '2024-08-05 08:00:00.000', 12, 'books', 182, 'Moscow', 3, 15.00, 0.05, 0, '172.16.2.3'),
(1116, '2024-08-05 10:30:00.000', 13, 'electronics', 183, 'Kazan', 1, 25.00, 0.00, 1, '172.16.2.4'),
(1117, '2024-08-05 13:00:00.000', 14, 'clothing', 184, 'SPB', 5, 45.00, 0.15, 1, '172.16.2.5'),
(1118, '2024-08-06 09:00:00.000', 15, 'books', 185, 'Moscow', 2, 12.00, 0.00, 0, '172.16.2.6'),
(1119, '2024-08-06 11:30:00.000', 16, 'electronics', 186, 'Kazan', 3, 80.00, 0.20, 1, '172.16.2.7'),
(1120, '2024-08-06 14:00:00.000', 17, 'clothing', 187, 'SPB', 4, 35.00, 0.05, 1, '172.16.2.8'),
(1121, '2024-08-07 08:00:00.000', 18, 'books', 188, 'Moscow', 1, 20.00, 0.00, 0, '172.16.2.9'),
(1122, '2024-08-07 10:30:00.000', 19, 'electronics', 189, 'Kazan', 2, 60.00, 0.10, 1, '172.16.2.10'),
(1123, '2024-08-07 13:00:00.000', 10, 'electronics', 190, 'Moscow', 3, 50.00, 0.00, 1, '192.168.4.1'),
(1124, '2024-08-08 09:00:00.000', 11, 'clothing', 191, 'SPB', 2, 30.00, 0.10, 1, '192.168.4.2'),
(1125, '2024-08-08 11:30:00.000', 12, 'books', 192, 'Moscow', 1, 15.00, 0.05, 0, '192.168.4.3'),
(1126, '2024-08-08 14:00:00.000', 13, 'electronics', 193, 'Kazan', 4, 25.00, 0.00, 1, '192.168.4.4'),
(1127, '2024-08-09 08:30:00.000', 14, 'clothing', 194, 'SPB', 2, 45.00, 0.15, 1, '192.168.4.5'),
(1128, '2024-08-09 11:00:00.000', 15, 'books', 195, 'Moscow', 3, 12.00, 0.00, 0, '192.168.4.6'),
(1129, '2024-08-09 13:30:00.000', 16, 'electronics', 196, 'Kazan', 5, 80.00, 0.20, 1, '192.168.4.7'),
(1130, '2024-08-10 09:00:00.000', 17, 'clothing', 197, 'SPB', 2, 35.00, 0.05, 1, '192.168.4.8'),
(1131, '2024-08-10 11:30:00.000', 18, 'books', 198, 'Moscow', 1, 20.00, 0.00, 0, '192.168.4.9'),
(1132, '2024-08-10 14:00:00.000', 19, 'electronics', 199, 'Kazan', 3, 60.00, 0.10, 1, '192.168.4.10');

SELECT count() FROM db_1.sales_var001;



-- 1 выручка по категориям 
SELECT 
    category,
    SUM(quantity * unit_price * (1 - discount_pct)) AS revenue
FROM db_1.sales_var001
GROUP BY category
ORDER BY revenue DESC;

--2. Топ-3 клиента по количеству покупок
SELECT 
    customer_id,
    count() AS purchase_count,
    SUM(quantity) AS total_quantity
FROM db_1.sales_var001
GROUP BY customer_id
ORDER BY purchase_count DESC
LIMIT 3;

-- 3. Средний чек по месяцам
SELECT 
    toYYYYMM(sale_timestamp) AS month,
    AVG(quantity * unit_price) AS avg_bill
FROM db_1.sales_var001
GROUP BY month
ORDER BY month;

--4. Фильтрация по партиции (только июнь 2024)
SELECT *
FROM db_1.sales_var001
WHERE sale_timestamp >= '2024-06-01' AND sale_timestamp < '2024-07-01';

-- Задание 3: ReplacingMergeTree — справочник товаров
-- содание таблицы продуктов 
CREATE TABLE IF NOT EXISTS db_1.products_var001 (
    product_id    UInt32,
    product_name  String,
    category      LowCardinality(String),
    supplier      String,
    base_price    Decimal64(2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (product_id);


--2. Вставьте 6 товаров (version = 1)
INSERT INTO products_var001 VALUES
(10, 'Ноутбук', 'electronics', 'TechCorp', 50000.00, 2.5, 1, now(), 1),
(11, 'Футболка', 'clothing', 'FashionInc', 1500.00, 0.2, 1, now(), 1),
(12, 'Книга', 'books', 'BookPub', 800.00, 0.5, 1, now(), 1),
(13, 'Смартфон', 'electronics', 'TechCorp', 30000.00, 0.3, 1, now(), 1),
(14, 'Джинсы', 'clothing', 'FashionInc', 3500.00, 0.8, 1, now(), 1),
(15, 'Наушники', 'electronics', 'AudioLab', 5000.00, 0.2, 1, now(), 1);

-- 3. Обновите 3 товара (version = 2)
INSERT INTO db_1.products_var001 VALUES
(10, 'Ноутбук Pro', 'electronics', 'TechCorp', 55000.00, 2.5, 1, now(), 2),
(11, 'Футболка Premium', 'clothing', 'FashionInc', 2500.00, 0.2, 1, now(), 2),
(13, 'Смартфон Pro', 'electronics', 'TechCorp', 35000.00, 0.3, 0, now(), 2);

--4. Проверьте — видны обе версии
SELECT * FROM db_1.products_var001;

-- 5. Выполните слияние и проверьте
SELECT * FROM db_1.products_var001 FINAL;


-- 1. Создайте таблицу
DROP TABLE IF EXISTS db_1.sales_var001;

CREATE TABLE db_1.sales_var001 (
    sale_id        UInt64,
    sale_timestamp DateTime64(3),
    product_id     UInt32,
    category       String,
    customer_id    UInt64,
    region         String,
    quantity       UInt16,
    unit_price     Decimal(10,2),
    discount_pct   Float32,
    is_online      UInt8,
    ip_address     String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_timestamp)
ORDER BY (sale_timestamp, customer_id, product_id);

--Задание 3: Таблица продуктов (ReplacingMergeTree)

CREATE TABLE IF NOT EXISTS db_1.products_var001 (
    product_id    UInt32,
    product_name  String,
    category      String,
    supplier      String,
    base_price    Decimal(10,2),
    weight_kg     Float32,
    is_available  UInt8,
    updated_at    DateTime,
    version       UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY product_id;

--Задание 4: Таблица метрик (SummingMergeTree)

CREATE TABLE IF NOT EXISTS db_1.daily_metrics_var001 (
    metric_date    Date,
    campaign_id    UInt32,
    channel        String,
    impressions    UInt64,
    clicks         UInt64,
    conversions    UInt32,
    spend_cents    UInt64
)
ENGINE = SummingMergeTree()
ORDER BY (metric_date, campaign_id, channel);

--Задание 5: Таблица тегов (Array)

CREATE TABLE IF NOT EXISTS db_1.tags_var001 (
    item_id  UInt32,
    item_name String,
    tags     Array(String)
)
ENGINE = MergeTree()
ORDER BY item_id;



-- Данные для продуктов (Задание 3)

INSERT INTO db_1.products_var001 VALUES
(10, 'Ноутбук', 'electronics', 'TechCorp', 50000.00, 2.5, 1, now(), 1),
(11, 'Футболка', 'clothing', 'FashionInc', 1500.00, 0.2, 1, now(), 1),
(12, 'Книга', 'books', 'BookPub', 800.00, 0.5, 1, now(), 1),
(13, 'Смартфон', 'electronics', 'TechCorp', 30000.00, 0.3, 1, now(), 1),
(14, 'Джинсы', 'clothing', 'FashionInc', 3500.00, 0.8, 1, now(), 1),
(15, 'Наушники', 'electronics', 'AudioLab', 5000.00, 0.2, 1, now(), 1);

-- Обновления
INSERT INTO db_1.products_var001 VALUES
(10, 'Ноутбук Pro', 'electronics', 'TechCorp', 55000.00, 2.5, 1, now(), 2),
(11, 'Футболка Premium', 'clothing', 'FashionInc', 2500.00, 0.2, 1, now(), 2),
(13, 'Смартфон Pro', 'electronics', 'TechCorp', 35000.00, 0.3, 0, now(), 2);
--Данные для метрик (Задание 4)

INSERT INTO db_1.daily_metrics_var001 VALUES
('2024-06-01', 11, 'google', 1000, 100, 10, 5000),
('2024-06-01', 11, 'yandex', 800, 80, 8, 4000),
('2024-06-02', 11, 'google', 1100, 110, 11, 5500),
('2024-06-02', 11, 'yandex', 850, 85, 9, 4250),
('2024-06-03', 11, 'google', 1200, 120, 12, 6000),
('2024-06-03', 11, 'yandex', 900, 90, 9, 4500),
('2024-06-04', 11, 'google', 1300, 130, 13, 6500),
('2024-06-04', 11, 'yandex', 950, 95, 10, 4750);

--Повторные строки для демонстрации суммирования

INSERT INTO db_1.daily_metrics_var001 VALUES
('2024-06-01', 11, 'google', 100, 5, 1, 500),
('2024-06-02', 11, 'yandex', 50, 3, 0, 250);


--Задание 3: Проверка ReplacingMergeTree

-- Видны обе версии
SELECT * FROM db_1.products_var001;

-- Только последние версии
SELECT * FROM db_1.products_var001 FINAL;


--Задание 4: Проверка SummingMergeTree

-- Исходные данные (с дубликатами)
SELECT * FROM db_1.daily_metrics_var001;

-- Просуммированные данные
SELECT * FROM db_1.daily_metrics_var001 FINAL;

-- CTR по каналам
SELECT 
    channel,
    SUM(clicks) AS total_clicks,
    SUM(impressions) AS total_impressions,
    SUM(clicks) / SUM(impressions) AS CTR
FROM db_1.daily_metrics_var001 FINAL
GROUP BY channel;


 --Задание 5: Комплексный анализ
-- 1. Проверка партиций таблицы sales_var001

SELECT 
    toYYYYMM(sale_timestamp) AS month,
    count() AS rows,
    sum(quantity) AS total_quantity
FROM db_1.sales_var001
GROUP BY month
ORDER BY month;

--2
SELECT
    p.product_name,
    p.category,
    sum(s.quantity * s.unit_price * (1 - s.discount_pct)) AS revenue
FROM db_1.sales_var001 AS s
INNER JOIN db_1.products_var001 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;

--3 

SELECT name, type
FROM system.columns
WHERE database = 'db_1' AND table = 'sales_var001';

SELECT name, type
FROM system.columns
WHERE database = 'db_1' AND table = 'products_var001';

SELECT name, type
FROM system.columns
WHERE database = 'db_1' AND table = 'daily_metrics_var001';


--2 запрос 
SELECT
    p.product_name,
    p.category,
    sum(s.quantity * s.unit_price * (1 - s.discount_pct)) AS revenue
FROM db_1.sales_var001 AS s
INNER JOIN db_1.products_var001 AS p
    ON s.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC
LIMIT 5;

--4. Запрос с массивом

SELECT
    arrayJoin(tags) AS tag,
    count() AS items_count
FROM db_1.tags_var001
GROUP BY tag
ORDER BY items_count DESC;

--5. Контрольная сумма

SELECT 'sales' AS tbl, count() AS rows, sum(quantity) AS check_sum FROM db_1.sales_var001
UNION ALL
SELECT 'products', count(), sum(toUInt64(product_id)) FROM db_1.products_var001 FINAL
UNION ALL
SELECT 'metrics', count(), sum(clicks) FROM db_1.daily_metrics_var001 FINAL;