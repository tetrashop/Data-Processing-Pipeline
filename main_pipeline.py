"""
پایپلاین اصلی پردازش داده
ورژن ۱.۰.۰
"""

from pipelines.data_cleaner import DataCleaner
from pipelines.format_converter import FormatConverter
from pipelines.etl_engine import ETLEngine

class DataProcessingPipeline:
    """کلاس اصلی مدیریت کل پایپلاین"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.converter = FormatConverter()
        self.etl = ETLEngine()
        self.version = "1.0.0"
        self.author = "تیم توسعه فارسی"
    
    def process_data(self, data, operations=None, output_format=None):
        """پردازش داده با عملیات مشخص"""
        if operations is None:
            operations = ["تمیزکاری"]
        
        print(f"🎯 شروع پردازش {len(data) if isinstance(data, list) else 1} رکورد...")
        
        # تمیزکاری داده
        if "تمیزکاری" in operations:
            print("🧹 در حال تمیزکاری داده...")
            data = self.cleaner.clean_data(data)
        
        # تبدیل فرمت
        if output_format and "تبدیل_فرمت" in operations:
            print(f"🔄 در حال تبدیل به {output_format}...")
            data = self.converter.convert(data, "json", output_format)
        
        return {
            "عملیات_انجام_شده": operations,
            "قالب_خروجی": output_format or "json",
            "داده_پردازش_شده": data,
            "تعداد_مراحل": len(operations)
        }
    
    def run_etl_pipeline(self, source, transformations=None, destination="console"):
        """اجرای پایپلاین کامل ETL"""
        return self.etl.run_pipeline(source, transformations, destination)
    
    def batch_process(self, data_list, operations=None):
        """پردازش دسته‌ای داده‌ها"""
        results = []
        
        for i, data in enumerate(data_list):
            print(f"📦 پردازش دسته {i+1} از {len(data_list)}...")
            result = self.process_data(data, operations)
            results.append(result)
        
        return {
            "تعداد_دسته‌ها": len(results),
            "نتایج": results,
            "میانگین_رکوردها": sum(len(r["داده_پردازش_شده"]) if isinstance(r["داده_پردازش_شده"], list) else 1 for r in results) / len(results)
        }
    
    def get_pipeline_info(self):
        """اطلاعات پایپلاین"""
        return {
            "نام": "پایپلاین پردازش داده فارسی",
            "ورژن": self.version,
            "توسعه_دهنده": self.author,
            "ماژول‌های_فعال": [
                "DataCleaner", "FormatConverter", "ETLEngine"
            ],
            "فرمت‌های_پشتیبانی_شده": self.converter.supported_formats
        }

# تابع اصلی برای استفاده سریع
def create_pipeline():
    """ایجاد یک نمونه از پایپلاین"""
    return DataProcessingPipeline()

def quick_process(data):
    """پردازش سریع داده"""
    pipeline = create_pipeline()
    return pipeline.process_data(data)

# مثال استفاده
if __name__ == "__main__":
    # ایجاد پایپلاین
    pipeline = DataProcessingPipeline()
    
    # نمایش اطلاعات
    info = pipeline.get_pipeline_info()
    print("🔧 اطلاعات پایپلاین:")
    for key, value in info.items():
        print(f"   {key}: {value}")
    
    print("\n" + "="*50)
    
    # پردازش نمونه
    sample_data = [
        {"نام": "علی", "مقدار": 100},
        {"نام": "رضا", "مقدار": 200},
        {"نام": "محمد", "مقدار": 150}
    ]
    
    result = pipeline.process_data(
        data=sample_data,
        operations=["تمیزکاری"],
        output_format="json"
    )
    
    print("\n🎉 پردازش کامل شد!")
    print(f"تعداد مراحل: {result['تعداد_مراحل']}")
    print(f"قالب خروجی: {result['قالب_خروجی']}")
