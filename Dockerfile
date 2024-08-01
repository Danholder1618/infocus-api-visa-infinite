FROM python:3.11

# Создание директорий
RUN mkdir /app && chmod 777 /app
RUN mkdir -p /app/logs
WORKDIR /app

# Установка зависимостей
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /app/requirements.txt

# Копирование кода
COPY . /app

# Установка прав
RUN chmod -R 777 /app

# Экспонирование порта
EXPOSE 6969

# Команда запуска
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "6969"]