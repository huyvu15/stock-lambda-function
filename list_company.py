# import mysql.connector
# from vnstock import listing_companies

# # Lấy danh sách các công ty niêm yết và chỉ lấy 13 cột đầu tiên
# companies = listing_companies()
# filtered_companies = companies.iloc[:, :13]
# print(filtered_companies)
# # Thiết lập kết nối với MySQL
# connection = mysql.connector.connect(
#     host='127.0.0.1',
#     database='da2',
#     user='root',
#     password='Ngocmai12062k3@'
# )

# cursor = connection.cursor()
# table = 'company'
# for i, row in filtered_companies.iterrows():
#     sql = (
#         "INSERT INTO " + 'companies' + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
#     )
#     cursor.execute(sql, tuple(row))

# # Xác nhận thay đổi
# connection.commit()

# # Đóng kết nối
# cursor.close()
# connection.close()

# print("Dữ liệu đã được đẩy lên MySQL thành công.")
