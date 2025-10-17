"""
مثال‌های ساده استفاده از پایپلاین پردازش داده
"""

from main_pipeline import DataProcessingPipeline

def example_1_simple_cleaning():
    """مثال ۱: تمیزکاری ساده داده"""
    print("=" * 60)
    print("مثال ۱: تمیزکاری ساده داده")
    print("=" * 60)
    
    pipeline = DataProcessingPipeline()
    
    # داده نمونه با مشکلات
    dirty_data = [
        {"name": "علی", "age": 25, "city": "تهران"},
        {"name": "علی", "age": 25, "city": "تهران"},  # تکراری
        {"name": "رضا", "age": None, "city": "مشهد"},  # مقدار گمشده
        {"name": "محمد", "age": 30, "city": "اصفهان"},
        {"name": "فاطمه", "age": 22, "city": "شیراز"}
    ]
    
    result = pipeline.process_data(
        data=dirty_data,
        operations=["تمیزکاری"]
    )
    
    print("📊 داده اصلی:", len(dirty_data), "رکورد")
    print("🧹 داده تمیز شده:", len(result["داده_پردازش_شده"]), "رکورد")
    print("✅ عملیات انجام شده:", result["عملیات_انجام_شده"])

def example_2_format_conversion():
    """مثال ۲: تبدیل فرمت داده"""
    print("\n" + "=" * 60)
    print("مثال ۲: تبدیل فرمت JSON به CSV")
    print("=" * 60)
    
    pipeline = DataProcessingPipeline()
    
    # داده JSON
    json_data = [
        {"product": "لپ‌تاپ", "price": 15000000, "stock": 5},
        {"product": "موبایل", "price": 8000000, "stock": 10},
        {"product": "تبلت", "price": 6000000, "stock": 7}
    ]
    
    result = pipeline.process_data(
        data=json_data,
        operations=["تمیزکاری"],
        output_format="csv"
    )
    
    print("📄 داده CSV تولید شده:")
    print(result["داده_پردازش_شده"])

def example_3_etl_pipeline():
    """مثال ۳: پایپلاین کامل ETL"""
    print("\n" + "=" * 60)
    print("مثال ۳: پایپلاین کامل ETL")
    print("=" * 60)
    
    pipeline = DataProcessingPipeline()
    
    # داده نمونه برای ETL
    sample_data = [
        {"employee": "علی رضایی", "salary": 5000000, "department": "فروش"},
        {"employee": "محمد حسینی", "salary": 6000000, "department": "توسعه"},
        {"employee": "فاطمه محمدی", "salary": 5500000, "department": "مارکتینگ"}
    ]
    
    result = pipeline.run_etl_pipeline(
        source=sample_data,
        transformations=["تمیزکاری"],
        destination="console"
    )
    
    print("🎯 نتیجه پایپلاین ETL:")
    print(f"وضعیت: {result['پایپلاین']}")

def example_4_batch_processing():
    """مثال ۴: پردازش دسته‌ای"""
    print("\n" + "=" * 60)
    print("مثال ۴: پردازش دسته‌ای داده‌ها")
    print("=" * 60)
    
    pipeline = DataProcessingPipeline()
    
    # چند دسته داده مختلف
    batches = [
        [{"id": 1, "value": 100}, {"id": 2, "value": 200}],
        [{"id": 3, "value": 150}, {"id": 4, "value": 250}, {"id": 5, "value": 300}],
        [{"id": 6, "value": 400}]
    ]
    
    result = pipeline.batch_process(
        data_list=batches,
        operations=["تمیزکاری"]
    )
    
    print(f"📦 پردازش {result['تعداد_دسته‌ها']} دسته داده")
    print(f"📊 میانگین {result['میانگین_رکوردها']:.1f} رکورد در هر دسته")

if __name__ == "__main__":
    print("🚀 اجرای تمام مثال‌های پایپلاین پردازش داده")
    print("توسعه‌دهنده: شما")
    print("ایمیل: ramin.edjlal1359@gmail.com")
    print()
    
    example_1_simple_cleaning()
    example_2_format_conversion() 
    example_3_etl_pipeline()
    example_4_batch_processing()
    
    print("\n" + "🎉 " + "=" * 50)
    print("تمام مثال‌ها با موفقیت اجرا شدند!")
    print("=" * 50)
