import os

# Đọc cấu hình từ biến môi trường của Docker Compose
SQLALCHEMY_DATABASE_URI = os.getenv('SQLALCHEMY_DATABASE_URI', 'sqlite:////app/superset_home/superset.db')
SECRET_KEY = os.getenv('SUPERSET_SECRET_KEY', 'CHANGE_ME_TO_A_COMPLEX_RANDOM_SECRET')

# Vô hiệu hóa tính năng cảnh báo của Superset
SUPERSET_WEBSERVER_TIMEOUT = 120
