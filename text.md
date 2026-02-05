# Практическое задание: Развертывание Apache Spark и разработка ETL‑пайплайна

## 0. Кратко: что нужно сделать
- Развернуть локальный Spark‑кластер (Master + Worker).
- Сгенерировать тестовые данные и построить ETL‑пайплайн на PySpark.
- Сохранить результаты, проверить их и оформить отчет.

Формат сдачи: GitHub‑репозиторий с кодом, отчетом и скриншотами.  
Срок: 2 недели с момента выдачи.  
Допустимая помощь: Можно использовать нейросети для поиска решений, но код и отчет пишете сами.  
Критерии оценки: Полнота выполнения, понимание, качество кода, аккуратность отчета.

---

## 1. Цели обучения
После выполнения вы сможете:
1. Объяснить базовую архитектуру Spark и роль Driver, Master, Worker, Executor.
2. Развернуть standalone‑кластер Spark на Windows.
3. Построить ETL‑процесс (Extract → Transform → Load) в PySpark.
4. Проверить корректность результатов и оформить отчет.

---

## 2. Требования и окружение
- ОС: Windows 10/11.
- Python: 3.9–3.11.
- JDK: 11 (LTS).
- Apache Spark: 3.5.1 (Hadoop 3).
- Инструменты: PowerShell, VS Code, Git.

---

## 3. Теория в 5 абзацах (минимум, который надо знать)
Apache Spark — фреймворк для распределенной обработки больших данных, ориентированный на in‑memory обработку (быстрее классического MapReduce).

Ключевые компоненты:
- Driver Program — ваша программа, которая формирует задания.
- Cluster Manager — распределяет ресурсы (в задании — standalone).
- Master — координатор кластера.
- Worker — рабочий узел, выполняет задачи.
- Executor — процесс на Worker, выполняющий вычисления.

---

## 4. Подготовка среды разработки

### 4.1 Установка JDK 11
Spark работает на JVM, поэтому Java обязательна.

Шаги:
1. Скачайте JDK 11: https://adoptium.net/temurin/releases/
2. Установите (по умолчанию путь вида: C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x).
3. Проверка в PowerShell:
        java -version
    
    Ожидаемый результат: openjdk version "11.0.xx".

Типовые ошибки:

| Ошибка | Причина | Решение |
|--------|---------|---------|
| 'java' не является внутренней или внешней командой | PATH не настроен | Добавить %JAVA_HOME%\bin в PATH |
| Ошибка jre1.8.x в пути | Конфликт старых Java | Удалить старые версии Java |
| The system cannot find the path specified | Неправильный путь | Переустановить и проверить путь |

### 4.2 Установка Apache Spark
Шаги:
1. Скачайте Spark 3.5.1 (Pre‑built for Hadoop 3): https://spark.apache.org/downloads.html
2. Распакуйте в C:\spark\spark-3.5.1-bin-hadoop3.
3. Проверка:
        dir C:\spark\
    

### 4.3 Настройка переменных окружения
Нужны три переменные: JAVA_HOME, SPARK_HOME, PATH.

Шаги:
1. Откройте Переменные среды Windows.
2. Создайте системные переменные:
    - JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x
    - SPARK_HOME = C:\spark\spark-3.5.1-bin-hadoop3
3. В Path добавьте:
    
    %JAVA_HOME%\bin
    %SPARK_HOME%\bin
    
4. Проверка:
        echo "JAVA_HOME: $env:JAVA_HOME"
    echo "SPARK_HOME: $env:SPARK_HOME"
    java -version
    spark-shell --version 2>&1 | Select-String "version"
    

---

## 5. Запуск Spark‑кластера (standalone)

### 5.1 Запуск Master
cd $env:SPARK_HOME
mkdir logs
\bin\spark-class.cmd org.apache.spark.deploy.master.Master
В выводе найдите:
SparkUI available at http://ВАШ_IP:8080
Master spark://ВАШ_КОМПЬЮТЕР:7077
Сохраните оба адреса.

### 5.2 Запуск Worker
В новом PowerShell:
cd $env:SPARK_HOME
\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker spark://ВАШ_КОМПЬЮТЕР:7077

Проверка:
- В Spark UI (http://localhost:8080) должен появиться Worker.

Типовые ошибки:

| Ошибка | Причина | Решение |
|--------|---------|---------|
| Failed to connect to master | Master не запущен | Запустить Master первым |
| Address already in use | Порт занят | Освободить порт |
| Connection refused | Брандмауэр | Добавить исключение |

### 5.3 Скрипты для запуска (опционально)
start_master.ps1
Start-Process powershell -Verb RunAs -ArgumentList @"
cd $env:SPARK_HOME
\bin\spark-class.cmd org.apache.spark.deploy.master.Master
"@
start_worker.ps1
$master_url = "spark://ВАШ_КОМПЬЮТЕР:7077"
Start-Process powershell -Verb RunAs -ArgumentList @"
cd $env:SPARK_HOME
\bin\spark-class.cmd org.apache.spark.deploy.worker.Worker $master_url
"@

---

## 6. Разработка ETL‑пайплайна

### 6.1 Структура проекта
spark_etl_project/
├── data/
├── src/
├── notebooks/
├── output/
├── docs/
└── README.md

### 6.2 Виртуальное окружение и зависимости
cd spark_etl_project
python -m venv venv
\venv\Scripts\activate
pip install pyspark==3.5.1 pandas==2.0.3 faker==20.1.0 openpyxl==3.1.2

requirements.txt
pyspark==3.5.1
pandas==2.0.3
faker==20.1.0
openpyxl==3.1.2

### 6.3 Генерация тестовых данных
src/generate_data.py
"""
Генерация реалистичных данных для ETL-пайплайна.
Создает CSV-файл с 50 000 записей о действиях пользователей.
"""
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

def generate_clickstream_data(num_records=50000, output_path="../data/clickstream.csv"):
    """
    Генерирует данные о кликах пользователей
    
    Args:
        num_records: количество записей
        output_path: путь для сохранения файла
    """
    
    # Инициализация генератора случайных данных
    fake = Faker('ru_RU')
    np.random.seed(42)
    random.seed(42)
    
    print("🚀 Начинаем генерацию данных...")
    
    # Списки возможных значений
    actions = ['click', 'view', 'purchase', 'login', 'logout', 'search', 'add_to_cart']
    devices = ['mobile', 'desktop', 'tablet']
    regions = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 
               'Казань', 'Нижний Новгород', 'Челябинск', 'Самара']
    
    data = []
    
    # Генерация "хороших" данных
    for i in range(num_records):
        if i % 10000 == 0:
            print(f"  Сгенерировано {i} записей...")
            
        record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(
                days=random.randint(0, 30),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }
        data.append(record)
    
    # Добавление "плохих" данных (10% от общего числа)
    bad_records = num_records // 10
    print(f"Добавляем {bad_records} 'плохих' записей для тестирования очистки...")
    
    for i in range(bad_records):
        # Создаем записи с проблемами
        problems = [
            {'user_id': ''},  # Пустой ID
            {'session_id': None},  # Отсутствующее значение
            {'timestamp': '2024-13-45 25:61:61'},  # Некорректная дата
            {'duration_sec': -random.randint(1, 100)},  # Отрицательное время
            {'device': 'smart_watch'},  # Нестандартное устройство
            {'price': -random.uniform(10, 100)},  # Отрицательная цена
            {'action': 'unknown_action'},  # Неизвестное действие
            {'region': ''}  # Пустой регион
        ]
        
        base_record = {
            'user_id': fake.uuid4()[:8],
            'session_id': f"sess_{fake.random_number(digits=8)}",
            'action': random.choice(actions),
            'timestamp': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d %H:%M:%S'),
            'region': random.choice(regions),
            'device': random.choice(devices),
            'duration_sec': random.randint(1, 600),
            'product_id': f"prod_{random.randint(1000, 9999)}",
            'price': round(random.uniform(10, 1000), 2)
        }
        
        # Добавляем случайную проблему
        problem = random.choice(problems)
        base_record.update(problem)
        data.append(base_record)
    
    # Создаем DataFrame

    df = pd.DataFrame(data)
    
    # Перемешиваем данные
    df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    
    # Сохраняем в CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    # Выводим статистику
    print("\n" + "="*60)
    print("✅ ДАННЫЕ УСПЕШНО СГЕНЕРИРОВАНЫ")
    print("="*60)
    print(f"📊 Общее количество записей: {len(df):,}")
    print(f"📁 Файл сохранен: {output_path}")
    print("\n📈 Статистика данных:")
    print(f"  - Уникальных пользователей: {df['user_id'].nunique()}")
    print(f"  - Уникальных регионов: {df['region'].nunique()}")
    print(f"  - Диапазон дат: {df['timestamp'].min()} - {df['timestamp'].max()}")
    print(f"  - Пустых user_id: {df['user_id'].isna().sum() + (df['user_id'] == '').sum()}")
    print(f"  - Отрицательных duration_sec: {(df['duration_sec'] < 0).sum()}")
    
    # Показываем пример данных
    print("\n👀 Пример данных (первые 3 строки):")
    print(df.head(3).to_string())
    
    return df

if __name__ == "__main__":
    # Генерируем данные
    df = generate_clickstream_data(50000)
    
    # Дополнительная проверка
    print("\n🔍 Проверка качества данных:")
    print(df.info())

Запустите генератор:
cd src
python generate_data.py

Проверка:
- В папке data/ должен появиться файл clickstream.csv
- Файл должен открываться в Excel
- Размер файла примерно 10-15 MB

Типовые ошибки:

| Ошибка | Причина | Решение |
|--------|---------|---------|
| No module named 'faker' | Библиотека не установлена | pip install faker |
| PermissionError | Нет прав на запись | Запустить VS Code от администратора |
| MemoryError | Не хватает памяти | Уменьшить num_records до 10000 |

### 6.4 Создание ETL‑пайплайна

`src/etl_pipeline.py`:
"""
Профессиональный ETL-пайплайн для обработки данных кликов
Выполняет: очистку, трансформацию, агрегацию и сохранение данных
"""
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    TimestampType, IntegerType, DoubleType, DateType
)
from pyspark.sql.window import Window


class SparkETLPipeline:
    """Класс для управления ETL-пайплайном Spark"""
    
    def __init__(self, master_url="spark://localhost:7077", app_name="ETL_Pipeline"):
        """
        Инициализация Spark сессии
        
        Args:
            master_url: URL Spark Master
            app_name: Имя приложения
        """
        self.start_time = datetime.now()
        
        try:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .master(master_url) \
                .config("spark.sql.shuffle.partitions", "8") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.memory", "2g") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            self.log("="*60)
            self.log(f"🚀 ИНИЦИАЛИЗАЦИЯ SPARK СЕССИИ")
            self.log(f"   Приложение: {app_name}")
            self.log(f"   Master URL: {master_url}")
            self.log(f"   Версия Spark: {self.spark.version}")
            self.log("="*60)
            
        except Exception as e:
            self.log(f"❌ ОШИБКА при создании Spark сессии: {e}", level="ERROR")
            sys.exit(1)
    
    def log(self, message, level="INFO"):
        """Логирование сообщений"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] [{level}] {message}")
    
    def extract(self, file_path):
        """
        Этап EXTRACT: Загрузка данных из CSV
        
        Args:
            file_path: Путь к CSV файлу
            
        Returns:
            DataFrame с загруженными данными
        """
        self.log("📥 Этап EXTRACT: Загрузка данных...")
        
        # Определяем схему данных для контроля типов
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("region", StringType(), True),
            StructField("device", StringType(), True),
            StructField("duration_sec", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        try:
            # Загружаем данные с обработкой ошибок
            raw_df = self.spark.read \
                .option("header", "true") \
                .option("encoding", "utf-8-sig") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(schema) \
                .csv(file_path)
            
            initial_count = raw_df.count()
            corrupt_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()
            
            self.log(f"   Загружено записей: {initial_count:,}")
            self.log(f"   Некорректных записей: {corrupt_count}")
            
            if corrupt_count > 0:
                self.log(f"   ⚠️  Обнаружены некорректные записи", level="WARN")
                # Сохраняем некорректные записи для анализа
                corrupt_df = raw_df.filter(F.col("_corrupt_record").isNotNull())
                corrupt_path = "../output/corrupt_records"
                corrupt_df.select("_corrupt_record") \
                    .write \
                    .mode("overwrite") \
                    .csv(corrupt_path)
                self.log(f"   Некорректные записи сохранены в: {corrupt_path}")
            
            # Фильтруем некорректные записи
            clean_df = raw_df.filter(F.col("_corrupt_record").isNull()) \
                .drop("_corrupt_record")
            
            return clean_df
            
        except Exception as e:
            self.log(f"❌ ОШИБКА при загрузке данных: {e}", level="ERROR")
            raise
    
    def transform(self, df):
        """
        Этап TRANSFORM: Очистка и трансформация данных
        
        Args:

            df: Входной DataFrame
            
        Returns:
            Очищенный и преобразованный DataFrame
        """
        self.log("🔧 Этап TRANSFORM: Обработка данных...")
        
        # 1. Базовая очистка
        self.log("   1. Базовая очистка...")
        cleaned_df = df \
            .dropDuplicates(["user_id", "session_id", "timestamp"]) \
            .fillna({
                "region": "Неизвестно",
                "device": "unknown",
                "user_id": "unknown_user"
            }) \
            .filter(F.col("user_id") != "") \
            .filter(F.col("duration_sec") > 0) \
            .filter(F.col("price") >= 0)
        
        # 2. Преобразование типов и добавление полей
        self.log("   2. Преобразование типов...")
        transformed_df = cleaned_df \
            .withColumn("event_timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss")) \
            .withColumn("event_date", F.to_date("event_timestamp")) \
            .withColumn("event_hour", F.hour("event_timestamp")) \
            .withColumn("event_dayofweek", F.dayofweek("event_timestamp")) \
            .withColumn("session_category",
                       F.when(F.col("duration_sec") < 60, "short")
                        .when(F.col("duration_sec") <= 300, "medium")
                        .otherwise("long")) \
            .withColumn("price_category",
                       F.when(F.col("price") < 100, "low")
                        .when(F.col("price") <= 500, "medium")
                        .otherwise("high"))
        
        # 3. Удаление некорректных дат
        transformed_df = transformed_df.filter(F.col("event_timestamp").isNotNull())
        
        self.log(f"   После очистки: {transformed_df.count():,} записей")
        
        return transformed_df
    
    def analyze(self, df):
        """
        Этап ANALYZE: Агрегация и анализ данных
        
        Args:
            df: Очищенный DataFrame
            
        Returns:
            Словарь с агрегированными DataFrame
        """
        self.log("📊 Этап ANALYZE: Агрегация данных...")
        
        results = {}
        
        # 1. Активность по регионам и часам
        self.log("   1. Агрегация по регионам и времени...")
        results["activity_by_region_hour"] = df.groupBy("region", "event_date", "event_hour") \
            .agg(
                F.count("*").alias("total_events"),
                F.countDistinct("user_id").alias("unique_users"),
                F.avg("duration_sec").alias("avg_duration"),
                F.sum("price").alias("total_revenue"),
                F.avg("price").alias("avg_price")
            ) \
            .orderBy("region", "event_date", "event_hour")
        
        # 2. Статистика по устройствам
        self.log("   2. Статистика по устройствам...")
        results["device_statistics"] = df.groupBy("device", "session_category") \
            .agg(
                F.count("*").alias("session_count"),
                F.avg("duration_sec").alias("avg_duration"),
                F.countDistinct("user_id").alias("unique_users")
            ) \
            .orderBy("device", F.col("session_count").desc())
        
        # 3. Топ пользователей по активности
        self.log("   3. Топ пользователей...")
        window_spec = Window.orderBy(F.col("total_sessions").desc())
        results["top_users"] = df.groupBy("user_id", "region") \
            .agg(
                F.count("*").alias("total_sessions"),
                F.sum("duration_sec").alias("total_time"),
                F.sum("price").alias("total_spent")
            ) \
            .withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") <= 100) \
            .orderBy("rank")
        
        # 4. Ежедневная активность
        self.log("   4. Ежедневная активность...")
        results["daily_activity"] = df.groupBy("event_date") \
            .agg(
                F.count("*").alias("daily_events"),
                F.countDistinct("user_id").alias("daily_users"),

                F.sum("price").alias("daily_revenue")
            ) \
            .orderBy("event_date")
        
        return results
    
    def load(self, cleaned_df, results_dict):
        """
        Этап LOAD: Сохранение результатов
        
        Args:
            cleaned_df: Очищенный DataFrame
            results_dict: Словарь с агрегированными данными
        """
        self.log("💾 Этап LOAD: Сохранение результатов...")
        
        # Создаем папки для результатов
        base_path = "../output"
        os.makedirs(f"{base_path}/cleaned_data", exist_ok=True)
        os.makedirs(f"{base_path}/aggregated", exist_ok=True)
        os.makedirs(f"{base_path}/reports", exist_ok=True)
        
        # 1. Сохраняем очищенные данные в Parquet (оптимизированный формат)
        self.log("   1. Сохранение очищенных данных...")
        cleaned_df.write \
            .mode("overwrite") \
            .partitionBy("event_date") \
            .parquet(f"{base_path}/cleaned_data/clickstream_cleaned")
        
        # 2. Сохраняем агрегированные результаты
        self.log("   2. Сохранение агрегированных данных...")
        for name, df in results_dict.items():
            # Сохраняем в Parquet для дальнейшего анализа
            df.write \
                .mode("overwrite") \
                .parquet(f"{base_path}/aggregated/{name}")
            
            # Сохраняем в CSV для удобства просмотра
            df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .option("header", "true") \
                .option("delimiter", ";") \
                .csv(f"{base_path}/reports/{name}_report")
        
        # 3. Создаем текстовый отчет
        self.create_report(cleaned_df, results_dict, base_path)
        
        self.log(f"   📁 Результаты сохранены в: {base_path}/")
    
    def create_report(self, cleaned_df, results_dict, base_path):
        """Создание текстового отчета о выполнении"""
        report_path = f"{base_path}/execution_report.txt"
        
        with open(report_path, "w", encoding="utf-8") as f:
            f.write("="*60 + "\n")
            f.write("ОТЧЕТ О ВЫПОЛНЕНИИ ETL-ПАЙПЛАЙНА\n")
            f.write("="*60 + "\n\n")
            
            f.write(f"Дата выполнения: {datetime.now()}\n")
            f.write(f"Имя приложения: {self.spark.conf.get('spark.app.name')}\n")
            f.write(f"Версия Spark: {self.spark.version}\n\n")
            
            f.write("СТАТИСТИКА ДАННЫХ:\n")
            f.write("-"*40 + "\n")
            f.write(f"Количество записей после очистки: {cleaned_df.count():,}\n")
            f.write(f"Колонок в данных: {len(cleaned_df.columns)}\n")
            f.write(f"Диапазон дат: {cleaned_df.agg(F.min('event_date')).collect()[0][0]} "
                   f"- {cleaned_df.agg(F.max('event_date')).collect()[0][0]}\n")
            f.write(f"Уникальных пользователей: {cleaned_df.select('user_id').distinct().count():,}\n")
            f.write(f"Уникальных регионов: {cleaned_df.select('region').distinct().count()}\n\n")
            
            f.write("СОХРАНЕННЫЕ ФАЙЛЫ:\n")
            f.write("-"*40 + "\n")
            f.write("1. Очищенные данные: output/cleaned_data/\n")
            f.write("2. Агрегированные данные: output/aggregated/\n")
            f.write("3. Отчеты в CSV: output/reports/\n")
            
            # Добавляем примеры данных
            f.write("\nПРИМЕРЫ ДАННЫХ:\n")
            f.write("-"*40 + "\n")
            
            # Пример очищенных данных
            f.write("Очищенные данные (первые 5 записей):\n")
            sample_data = cleaned_df.limit(5).collect()
            for row in sample_data:
                f.write(str(row) + "\n")
            
            # Пример агрегированных данных
            f.write("\nАгрегированные данные (первые 3 записи):\n")
            first_key = list(results_dict.keys())[0]
            sample_agg = results_dict[first_key].limit(3).collect()
            for row in sample_agg:
                f.write(str(row) + "\n")

        self.log(f"   📄 Отчет создан: {report_path}")
    
    def run(self, input_path):
        """Запуск полного ETL-пайплайна"""
        try:
            # EXTRACT
            raw_data = self.extract(input_path)
            
            # TRANSFORM
            cleaned_data = self.transform(raw_data)
            
            # ANALYZE
            analysis_results = self.analyze(cleaned_data)
            
            # LOAD
            self.load(cleaned_data, analysis_results)
            
            # Вывод времени выполнения
            execution_time = datetime.now() - self.start_time
            self.log("="*60)
            self.log(f"✅ ETL-ПАЙПЛАЙН УСПЕШНО ВЫПОЛНЕН")
            self.log(f"   Общее время выполнения: {execution_time}")
            self.log("="*60)
            
            # Показываем примеры результатов
            self.show_samples(cleaned_data, analysis_results)
            
            return True
            
        except Exception as e:
            self.log(f"❌ ОШИБКА В ПАЙПЛАЙНЕ: {e}", level="ERROR")
            import traceback
            traceback.print_exc()
            return False
    
    def show_samples(self, cleaned_df, results_dict):
        """Показ примеров результатов"""
        print("\n" + "="*60)
        print("ОБРАЗЦЫ РЕЗУЛЬТАТОВ:")
        print("="*60)
        
        print("\n1. ОЧИЩЕННЫЕ ДАННЫЕ (первые 5 строк):")
        cleaned_df.select("user_id", "action", "region", "device", "duration_sec", "event_date") \
            .show(5, truncate=False)
        
        print("\n2. АКТИВНОСТЬ ПО РЕГИОНАМ (топ-5):")
        results_dict["activity_by_region_hour"] \
            .groupBy("region") \
            .agg(F.sum("total_events").alias("total_events")) \
            .orderBy(F.col("total_events").desc()) \
            .show(5, truncate=False)
        
        print("\n3. ЕЖЕДНЕВНАЯ АКТИВНОСТЬ (последние 5 дней):")
        results_dict["daily_activity"] \
            .orderBy(F.col("event_date").desc()) \
            .show(5, truncate=False)
    
    def stop(self):
        """Остановка Spark сессии"""
        self.log("Остановка Spark сессии...")
        self.spark.stop()

def main():
    """Основная функция"""
    # Конфигурация
    INPUT_FILE = "../data/clickstream.csv"
    MASTER_URL = "spark://localhost:7077"  # Измените на свой адрес Master
    
    # Создаем и запускаем пайплайн
    pipeline = SparkETLPipeline(
        master_url=MASTER_URL,
        app_name="Student_ETL_Pipeline"
    )
    
    try:
        success = pipeline.run(INPUT_FILE)
        if success:
            print("\n🎉 Поздравляем! ETL-пайплайн успешно выполнен!")
            print("📋 Что делать дальше:")
            print("   1. Проверьте папку output/")
            print("   2. Откройте Web UI Spark: http://localhost:8080")
            print("   3. Сделайте скриншоты для отчета")
        else:
            print("\n💥 В процессе выполнения возникли ошибки")
            print("   Проверьте вывод выше и исправьте ошибки")
            
    finally:
        pipeline.stop()

if __name__ == "__main__":
    main()

Запуск пайплайна:
cd src
python etl_pipeline.py

Типовые ошибки:

| Ошибка | Причина | Решение |
|--------|---------|---------|
| Py4JNetworkError | Нет подключения к Spark Master | Проверить, запущен ли Master и Worker |
| java.lang.OutOfMemoryError | Не хватает памяти | Увеличить spark.executor.memory |
| AnalysisException | Неправильный синтаксис SQL | Проверить имена колонок |
| FileNotFoundException | Файл данных не найден | Проверить путь к файлу |

### 6.5 Проверка результатов
Создайте скрипт для проверки:
`src/verify_results.py`:
import os
from pyspark.sql import SparkSession

def verify_results():
    """Проверка результатов выполнения ETL"""
    
    spark = SparkSession.builder \
        .appName("VerifyResults") \
        .master("local[*]") \
        .getOrCreate()
    
    print("🔍 ПРОВЕРКА РЕЗУЛЬТАТОВ ETL")
    print("="*60)
    
    # Проверяем наличие файлов
    base_path = "../output"
    
    if not os.path.exists(base_path):
        print("❌ Папка output не найдена!")
        return
    
    # 1. Проверяем очищенные данные
    parquet_path = f"{base_path}/cleaned_data/clickstream_cleaned"
    if os.path.exists(parquet_path):
        print("\n1. ОЧИЩЕННЫЕ ДАННЫЕ:")
        df = spark.read.parquet(parquet_path)
        print(f"   Записей: {df.count():,}")
        print(f"   Колонок: {len(df.columns)}")
        print("   Пример данных:")
        df.select("user_id", "action", "event_date").show(3, truncate=False)
    else:
        print("❌ Очищенные данные не найдены")
    
    # 2. Проверяем агрегированные данные
    aggregated_path = f"{base_path}/aggregated"
    if os.path.exists(aggregated_path):
        print("\n2. АГРЕГИРОВАННЫЕ ДАННЫЕ:")
        for folder in os.listdir(aggregated_path):
            folder_path = os.path.join(aggregated_path, folder)
            if os.path.isdir(folder_path):
                df = spark.read.parquet(folder_path)
                print(f"   {folder}: {df.count()} записей")
    else:
        print("❌ Агрегированные данные не найдены")
    
    # 3. Проверяем отчеты
    reports_path = f"{base_path}/reports"
    if os.path.exists(reports_path):
        print("\n3. ОТЧЕТЫ:")
        for folder in os.listdir(reports_path):
            folder_path = os.path.join(reports_path, folder)
            if os.path.isdir(folder_path):
                # Ищем CSV файлы
                for file in os.listdir(folder_path):
                    if file.endswith('.csv'):
                        csv_path = os.path.join(folder_path, file)
                        df = spark.read.csv(csv_path, header=True, sep=";")
                        print(f"   {file}: {df.count()} записей")
    else:
        print("❌ Отчеты не найдены")
    
    # 4. Проверяем текстовый отчет
    report_file = f"{base_path}/execution_report.txt"
    if os.path.exists(report_file):
        print("\n4. ТЕКСТОВЫЙ ОТЧЕТ:")
        with open(report_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            for line in lines[:10]:  # Показываем первые 10 строк
                print(f"   {line.strip()}")
    else:
        print("❌ Текстовый отчет не найден")
    
    spark.stop()
    
    print("\n" + "="*60)
    print("✅ ПРОВЕРКА ЗАВЕРШЕНА")
    print("="*60)

if __name__ == "__main__":
    verify_results()

---

## 7. Оформление репозитория и отчета

### 7.1 Структура репозитория GitHub
student_spark_project/
├── .gitignore
├── README.md
├── requirements.txt
├── data/
│   └── clickstream.csv
├── src/
│   ├── generate_data.py
│   ├── etl_pipeline.py
│   └── verify_results.py
├── output/ (не загружать в GitHub)
├── docs/
│   ├── screenshots/
│   │   ├── spark_ui.png
│   │   ├── terminal_output.png
│   │   ├── code.png
│   │   └── results_folder.png
│   └── report.md
└── notebooks/ (опционально)
    └── analysis.ipynb

.gitignore (минимум):
venv/
__pycache__/
.ipynb_checkpoints/
output/
*.parquet
*.log

### 7.2 Файл README.md
# Проект: ETL-пайплайн на Apache Spark

## Описание проекта
Проект демонстрирует создание ETL-пайплайна для обработки больших данных с использованием Apache Spark.

## Структура проекта
- `data/` - исходные данные
- `src/` - исходный код
- `docs/` - документация и скриншоты
- `notebooks/` - Jupyter notebooks для анализа

## Требования
- Python 3.9–3.11
- JDK 11
- Apache Spark 3.5.1

## Установка
1. Установите зависимости:
```bash
pip install -r requirements.txt

2. Настройте переменные окружения (см. инструкцию)

## Запуск
1. Запустите Spark кластер
2. Сгенерируйте данные:
python src/generate_data.py

3. Запустите ETL-пайплайн:
python src/etl_pipeline.py
## Результаты
После выполнения в папке output/ будут:
- Очищенные данные в формате Parquet
- Агрегированные отчеты
- Текстовый отчет о выполнении

### 7.3 Отчет в docs/report.md
```markdown
# Отчет по проекту: ETL-пайплайн на Apache Spark

## Информация о студенте
- ФИО: [Ваше ФИО]
- Группа: [Номер группы]
- Дата: [Дата выполнения]

## 1. Выполненные шаги

### 1.1 Установка и настройка
- [x] Установлен JDK 11
- [x] Установлен Apache Spark 3.5.1
- [x] Настроены переменные окружения
- [x] Проверена работоспособность

### 1.2 Запуск Spark кластера
- [x] Запущен Master
- [x] Запущен Worker
- [x] Проверен Web UI

### 1.3 Разработка ETL-пайплайна
- [x] Сгенерированы тестовые данные
- [x] Реализован ETL-пайплайн
- [x] Выполнена очистка данных
- [x] Выполнена агрегация данных
- [x] Сохранены результаты

## 2. Скриншоты

### 2.1 Spark Web UI
![Spark Web UI](screenshots/spark_ui.png)

### 2.2 Выполнение ETL-пайплайна
![Terminal Output](screenshots/terminal_output.png)

### 2.3 Результаты
![Results Folder](screenshots/results_folder.png)

## 3. Результаты выполнения

### Статистика данных:
- Исходных записей: 55,000
- После очистки: [ваше число]
- Уникальных пользователей: [ваше число]
- Диапазон дат: [диапазон]

### Время выполнения:
- Генерация данных: [время]
- ETL-пайплайн: [время]
- Общее время: [время]

## 4. Проблемы и решения

| Проблема | Решение |
|----------|---------|
| [Описание проблемы] | [Как решили] |
| [Описание проблемы] | [Как решили] |

## 5. Выводы
[Ваши выводы о проделанной работе]

### 7.4 Необходимые скриншоты
1. Spark Web UI (docs/screenshots/spark_ui.png):
   - Главная страница Master (localhost:8080)
   - Страница с запущенным Worker
   - История приложений

2. Терминал (docs/screenshots/terminal_output.png):
   - Запуск Master и Worker
   - Выполнение ETL-пайплайна
   - Проверка результатов

3. Файлы результатов (docs/screenshots/results_folder.png):
   - Содержимое папки output/
   - Примеры файлов Parquet и CSV

4. Код в VS Code (docs/screenshots/code.png):
   - Скриншот с открытыми файлами проекта

### 7.5 Чек‑лист готовности к сдаче
- [ ] Spark Master и Worker запускаются без ошибок
- [ ] generate_data.py создает data/clickstream.csv
- [ ] etl_pipeline.py отрабатывает без ошибок и пишет в output/
- [ ] verify_results.py находит все артефакты
- [ ] Репозиторий соответствует структуре
- [ ] README заполнен
- [ ] Отчет и скриншоты добавлены

---

## 8. Критерии оценки

### Обязательные требования (10 баллов):
1. Установка и настройка (2 балла):
   - ✓ Spark и Java установлены
   - ✓ Переменные окружения настроены
   - ✓ Проверочные команды работают

2. Запуск кластера (3 балла):
   - ✓ Master запущен
   - ✓ Worker подключен
   - ✓ Web UI доступен
   - ✓ Скриншоты приложены

3. ETL-пайплайн (3 балла):
   - ✓ Данные сгенерированы
   - ✓ ETL выполнен без ошибок
   - ✓ Результаты сохранены
   - ✓ Проверка результатов работает

4. Отчет и репозиторий (2 балла):
   - ✓ Репозиторий на GitHub
   - ✓ Полная структура проекта
   - ✓ README.md заполнен
   - ✓ Отчет с скриншотами

### Дополнительные баллы (до +5 баллов):
- Качество кода: Использование классов, обработка ошибок, логирование (+1)
- Оптимизация: Настройка параметров Spark, использование кэширования (+1)
- Дополнительный анализ: Реализация дополнительных агрегаций или визуализаций (+1)
- Документация: Подробные комментарии в коде, описание архитектуры (+1)
- Креативность: Реализация дополнительных фич (веб-интерфейс, дашборд) (+1)

### Штрафы:
- -1 балл за каждую неделю просрочки
- -2 балла за плагиат (код должен быть написан самостоятельно)
- -1 балл за отсутствие скриншотов

---

## 9. Полезные команды и диагностика

### Проверка сети:
# Проверить доступность портов
Test-NetConnection localhost -Port 8080
Test-NetConnection localhost -Port 7077

# Посмотреть открытые порты
netstat -ano | findstr :8080

### Очистка логов:
# Очистить логи Spark
Remove-Item C:\spark\spark-3.5.1-bin-hadoop3\logs\* -Recurse -Force

# Очистить выходные данные
Remove-Item output\* -Recurse -Force
### Перезапуск кластера:
# Закрыть все Java процессы Spark
taskkill /F /IM java.exe

# Проверить, что процессы закрыты
tasklist | findstr java

---

## 10. Рекомендации по работе с нейросетями

### Как задавать вопросы:
1. Конкретно: "Как исправить ошибку Py4JNetworkError при подключении к Spark Master?"
2. С контекстом: Приложите текст ошибки и свой код
3. По шагам: Разбейте сложную проблему на простые вопросы

### Примеры запросов:
"Объясни, что делает эта строка кода в PySpark: 
df.groupBy('region').agg(F.count('*').alias('total'))"

"Помоги написать обработку ошибок для Spark ETL пайплайна"

"Как настроить spark.executor.memory для данных размером 1GB?"

### Полезные нейросети:
- DeepSeek: Хорош для программирования и объяснения кода
- GigaChat: Отлично объясняет теорию на русском языке
- ChatGPT: Широкие возможности, но может требовать уточнений

---

## 11. Финальные шаги

1. Создайте репозиторий на GitHub
2. Загрузите все файлы проекта
3. Выполните задание полностью
4. Сделайте скриншоты всех этапов
5. Напишите отчет в docs/report.md
6. Отправьте ссылку на репозиторий преподавателю

Срок сдачи: 2 недели с момента выдачи (если преподаватель не указал иначе)

Важно: Начинайте выполнение задания заранее, чтобы было время решить возможные проблемы!

---

## 12. Контакты для помощи
- Преподаватель: [Имя преподавателя, email]
- Чат курса: [Ссылка на Telegram/Discord]
- Консультации: [Дни и время консультаций]

Удачи в выполнении задания! 🚀