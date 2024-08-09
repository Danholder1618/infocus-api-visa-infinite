from database.mysql import database
from models.api_models import Customer
from typing import List
import json

async def save_token_to_db(token):
    query = """
    INSERT INTO tokens (login, password, access_token, refresh_token, token_type, expires_in, refresh_expires_in)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        token.login, token.password, token.access_token, token.refresh_token, 
        token.token_type, token.expires_in, token.refresh_expires_in
    )
    await database.execute_query(query, params)

async def update_token_in_db(token):
    query = """
    UPDATE tokens SET access_token = %s, refresh_token = %s, token_type = %s, expires_in = %s, refresh_expires_in = %s
    WHERE login = %s
    """
    params = (
        token.access_token, token.refresh_token, token.token_type, 
        token.expires_in, token.refresh_expires_in, token.login
    )
    await database.execute_query(query, params)

async def get_token_from_db():
    query = "SELECT * FROM tokens WHERE id = %s"
    result = await database.fetch_one(query, params=(1,), as_dict=True)
    return result

async def get_authorization_header():
    query = "SELECT access_token FROM tokens"
    result = await database.fetch_one(query, as_dict=True)
    access_token = result['access_token'] if result else None
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    } if access_token else {}

async def create_table_if_not_exists():
    query = """
    CREATE TABLE IF NOT EXISTS tokens (
        id INT AUTO_INCREMENT PRIMARY KEY,
        login VARCHAR(255) NOT NULL,
        password VARCHAR(255) NOT NULL,
        access_token TEXT,
        refresh_token TEXT,
        token_type VARCHAR(50),
        expires_in INT,
        refresh_expires_in INT
    );

    CREATE TABLE IF NOT EXISTS customers (
        id INT AUTO_INCREMENT PRIMARY KEY,
        additional_phone VARCHAR(255) UNIQUE,
        bank_manager_fio VARCHAR(255),
        bank_manager_phone VARCHAR(255) UNIQUE,
        bank_product VARCHAR(255),
        bin VARCHAR(255),
        card_type_id INT,
        clid VARCHAR(255),
        date_birth DATETIME,
        date_expiry DATETIME,
        email VARCHAR(255) UNIQUE,
        firstname VARCHAR(255),
        inn VARCHAR(255),
        language VARCHAR(255),
        lastname VARCHAR(255),
        manager BOOLEAN,
        manual_subscribe BOOLEAN,
        message_id VARCHAR(255),
        middlename VARCHAR(255),
        pan VARCHAR(255),
        phone VARCHAR(255) UNIQUE,
        project_additional_data JSON,
        service_level VARCHAR(255),
        welcome VARCHAR(255),
        status_flag VARCHAR(50) NOT NULL
    );
    """
    await database.execute_query(query)

async def load_customers_from_file(file_path: str) -> List[Customer]:
    with open(file_path, 'new_data') as file:
        data = json.load(file)
    return [Customer(**item) for item in data]

async def get_customer_from_db(phone: str):
    query = "SELECT * FROM customers WHERE phone = %s"
    return await database.fetch_one(query, params=(phone,), as_dict=True)

async def process_customers(customers: List[Customer]):
    for customer in customers:
        existing_customer = await get_customer_from_db(customer.phone)
        if existing_customer:
            if customer != existing_customer:
                customer.status_flag = 'updated'
                await update_customer_in_db(customer)
            else:
                continue
        else:
            customer.status_flag = 'new'
            await save_customer_to_db(customer)
    
    await mark_customers_as_sent()

async def save_customer_to_db(customer):
    query = """
    INSERT INTO customers (additional_phone, bank_manager_fio, bank_manager_phone, bank_product, bin, card_type_id,
                           clid, date_birth, date_expiry, email, firstname, inn, language, lastname, manager,
                           manual_subscribe, message_id, middlename, pan, phone, project_additional_data, service_level,
                           welcome, status_flag)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (customer.additional_phone, customer.bank_manager_fio, customer.bank_manager_phone, customer.bank_product,
              customer.bin, customer.card_type_id, customer.clid, customer.date_birth, customer.date_expiry, customer.email,
              customer.firstname, customer.inn, customer.language, customer.lastname, customer.manager, customer.manualSubscribe,
              customer.massageId, customer.middlename, customer.pan, customer.phone, json.dumps(customer.project_additional_data),
              customer.service_level, customer.welcome, customer.status_flag)
    await database.execute_query(query, params)

async def update_customer_in_db(customer):
    query = """
    UPDATE customers SET additional_phone = %s, bank_manager_fio = %s, bank_manager_phone = %s, bank_product = %s,
                         bin = %s, card_type_id = %s, clid = %s, date_birth = %s, date_expiry = %s, email = %s,
                         firstname = %s, inn = %s, language = %s, lastname = %s, manager = %s, manual_subscribe = %s,
                         message_id = %s, middlename = %s, pan = %s, phone = %s, project_additional_data = %s,
                         service_level = %s, welcome = %s, status_flag = %s
    WHERE phone = %s
    """
    params = (customer.additional_phone, customer.bank_manager_fio, customer.bank_manager_phone, customer.bank_product,
              customer.bin, customer.card_type_id, customer.clid, customer.date_birth, customer.date_expiry, customer.email,
              customer.firstname, customer.inn, customer.language, customer.lastname, customer.manager, customer.manualSubscribe,
              customer.massageId, customer.middlename, customer.pan, customer.phone, json.dumps(customer.project_additional_data),
              customer.service_level, customer.welcome, customer.status_flag, customer.phone)
    await database.execute_query(query, params)

async def mark_customers_as_sent():
    query = "UPDATE customers SET status_flag = 'sent' WHERE status_flag IN ('new', 'updated')"
    await database.execute_query(query)
