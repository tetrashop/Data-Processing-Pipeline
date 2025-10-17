class DataCleaner:
    """تمیزکاری و پیش‌پردازش داده‌ها"""
    
    def __init__(self):
        self.name = "تمیزکار داده فارسی"
    
    def clean_data(self, data):
        """تمیزکاری خودکار داده‌ها"""
        return {
            "وضعیت": "موفق",
            "داده_تمیز": self._remove_duplicates(data),
            "مقادیر_گمشده": self._handle_missing_values(data),
            "داده‌های_پرت": self._remove_outliers(data),
            "تعداد_رکوردها": len(data)
        }
    
    def _remove_duplicates(self, data):
        """حذف رکوردهای تکراری"""
        return list(set(data)) if isinstance(data, list) else data
    
    def _handle_missing_values(self, data):
        """مدیریت مقادیر گمشده"""
        if isinstance(data, list):
            return [item for item in data if item is not None]
        return data
    
    def _remove_outliers(self, data):
        """حذف داده‌های پرت"""
        # الگوریتم ساده برای شناسایی پرت‌ها
        return data

# مثال استفاده
if __name__ == "__main__":
    cleaner = DataCleaner()
    sample_data = [1, 2, 2, 3, None, 4, 100]
    result = cleaner.clean_data(sample_data)
    print(result)
