import pandas as pd
from datetime import datetime
from cli_info import OracleClient
import calendar
import json

cl = OracleClient()
excel_file = 'Infinite.xlsx'
dtype_spec = {
    'CLIENT_B': str
}
df = pd.read_excel(excel_file, dtype=dtype_spec)

data_dict = df.to_dict(orient='records')

print(data_dict)

new_data = []
for item in data_dict:
    date_birth_in = datetime.strptime(item['BIRTHDAY'], "%d.%m.%Y")
    date_birth = date_birth_in.strftime("%Y-%m-%d")
    
    expiry_date_str = str(item['EX'])
    expiry_date_obj = datetime.strptime(expiry_date_str, "%m.%y")
    last_day_of_month = calendar.monthrange(expiry_date_obj.year, expiry_date_obj.month)[1]
    date_expiry_in = expiry_date_obj.replace(day=last_day_of_month)
    date_expiry = date_expiry_in.strftime("%Y-%m-%d")

    cl = OracleClient()
    res_cli = cl.client_by_client_code(item['CLIENT_B'])

    temp = {
        "additional_phone": None,
        "bank_manager_fio": None,
        "bank_manager_phone": None,
        "bank_product": None,
        "bin": 419525,
        "card_type_id": 4,
        "clid": item['CLIENT_B'],
        "date_birth": date_birth,
        "date_expiry": date_expiry,
        "email": item['R_E_MAILS'],
        "firstname": item['F_NAMES'],
        "inn": None,
        "language": "ru",
        "lastname": item['SURNAME'],
        "manager": False,
        "manualSubscribe": False,
        "messageId": None,
        "middlename": res_cli['middle_name'],
        "pan": str(item['PAN']),
        "phone": str(item['R_MOB_PHONE']),
        "project_additional_data": [],
        "service_level": "BASIC",
        "welcome": "1"
    }
    new_data.append(temp)

with open('new_data.json', 'w', encoding='utf-8') as json_file:
    json.dump(new_data, json_file, ensure_ascii=False, indent=4)
