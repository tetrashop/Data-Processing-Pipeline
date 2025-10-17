import json
from .data_cleaner import DataCleaner
from .format_converter import FormatConverter

class ETLEngine:
    """موتور استخراج، تبدیل و بارگذاری داده"""
    
    def __init__(self):
        self.cleaner = DataCleaner()
        self.converter = FormatConverter()
        self.name = "موتور ETL فارسی"
    
    def extract(self, source, source_type="json"):
        """استخراج داده از منابع مختلف"""
        try:
            if source_type == "json":
                return self._extract_json(source)
            elif source_type == "csv":
                return self._extract_csv(source)
            elif source_type == "inline":
                return source  # داده مستقیم
            else:
                return {"خطا": f"نوع منبع {source_type} پشتیبانی نمی‌شود"}
        except Exception as e:
            return {"خطا": f"خطا در استخراج: {str(e)}"}
    
    def transform(self, data, operations=None):
        """تبدیل داده با عملیات مختلف"""
        if operations is None:
            operations = ["تمیزکاری"]
        
        transformed_data = data
        
        for operation in operations:
            if operation == "تمیزکاری":
                if isinstance(transformed_data, list):
                    transformed_data = self.cleaner.clean_data(transformed_data)
            elif operation == "تبدیل_فرمت":
                transformed_data = self.converter.convert(transformed_data, "json", "csv")
        
        return {
            "عملیات_انجام_شده": operations,
            "داده_خام": data,
            "داده_تبدیل_شده": transformed_data,
            "تعداد_رکوردها": len(transformed_data) if isinstance(transformed_data, list) else 1
        }
    
    def load(self, data, destination_type="console"):
        """بارگذاری داده در مقصد"""
        try:
            if destination_type == "console":
                return self._load_to_console(data)
            elif destination_type == "file":
                return self._load_to_file(data)
            elif destination_type == "json":
                return self._load_to_json(data)
            else:
                return {"خطا": f"مقصد {destination_type} پشتیبانی نمی‌شود"}
        except Exception as e:
            return {"خطا": f"خطا در بارگذاری: {str(e)}"}
    
    def _extract_json(self, json_str):
        """استخراج از JSON"""
        if isinstance(json_str, str):
            return json.loads(json_str)
        return json_str
    
    def _extract_csv(self, csv_str):
        """استخراج از CSV"""
        return self.converter.convert(csv_str, "csv", "json")
    
    def _load_to_console(self, data):
        """بارگذاری در کنسول"""
        print("📤 خروجی داده:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return {"وضعیت": "موفق", "مقصد": "console"}
    
    def _load_to_file(self, data):
        """بارگذاری در فایل"""
        # شبیه‌سازی ذخیره در فایل
        return {"وضعیت": "موفق", "مقصد": "file", "تعداد_بایت": len(str(data))}
    
    def _load_to_json(self, data):
        """بارگذاری به صورت JSON"""
        return json.dumps(data, ensure_ascii=False)

    def run_pipeline(self, source, operations=None, destination="console"):
        """اجرای کامل پایپلاین ETL"""
        print("🚀 شروع پایپلاین ETL...")
        
        # استخراج
        print("📥 در حال استخراج داده...")
        extracted_data = self.extract(source)
        
        if "خطا" in extracted_data:
            return extracted_data
        
        # تبدیل
        print("🔄 در حال تبدیل داده...")
        transformed_data = self.transform(extracted_data, operations)
        
        # بارگذاری
        print("📤 در حال بارگذاری داده...")
        load_result = self.load(transformed_data, destination)
        
        return {
            "پایپلاین": "کامل",
            "استخراج": extracted_data,
            "تبدیل": transformed_data,
            "بارگذاری": load_result
        }

# مثال استفاده
if __name__ == "__main__":
    etl = ETLEngine()
    
    # داده نمونه
    sample_data = [
        {"نام": "علی", "سن": 25, "شهر": "تهران"},
        {"نام": "رضا", "سن": 30, "شهر": "مشهد"},
        {"نام": "محمد", "سن": 28, "شهر": "اصفهان"}
    ]
    
    # اجرای پایپلاین کامل
    result = etl.run_pipeline(
        source=sample_data,
        operations=["تمیزکاری"],
        destination="console"
    )
    
    print("\n🎯 نتایج نهایی پایپلاین:")
    print(f"وضعیت: {result['پایپلاین']}")
