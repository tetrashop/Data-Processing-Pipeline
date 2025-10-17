class FormatConverter:
    """تبدیل بین فرمت‌های مختلف داده"""
    
    def __init__(self):
        self.supported_formats = ["json", "csv", "xml", "yaml"]
    
    def convert(self, data, from_format, to_format):
        """تبدیل داده بین فرمت‌ها"""
        if from_format not in self.supported_formats:
            return {"خطا": f"فرمت {from_format} پشتیبانی نمی‌شود"}
        
        if to_format not in self.supported_formats:
            return {"خطا": f"فرمت {to_format} پشتیبانی نمی‌شود"}
        
        try:
            if from_format == "json" and to_format == "csv":
                return self._json_to_csv(data)
            elif from_format == "csv" and to_format == "json":
                return self._csv_to_json(data)
            else:
                return {"خطا": "تبدیل این فرمت هنوز پیاده‌سازی نشده"}
        except Exception as e:
            return {"خطا": f"خطا در تبدیل: {str(e)}"}
    
    def _json_to_csv(self, json_data):
        """تبدیل JSON به CSV"""
        if isinstance(json_data, list) and json_data:
            # ایجاد هدر CSV از کلیدهای اولین آیتم
            headers = list(json_data[0].keys())
            csv_lines = [",".join(headers)]
            
            for item in json_data:
                row = [str(item.get(key, "")) for key in headers]
                csv_lines.append(",".join(row))
            
            return "\n".join(csv_lines)
        return ""
    
    def _csv_to_json(self, csv_data):
        """تبدیل CSV به JSON"""
        lines = csv_data.strip().split("\n")
        if len(lines) < 2:
            return []
        
        headers = lines[0].split(",")
        json_result = []
        
        for line in lines[1:]:
            values = line.split(",")
            item = {}
            for i, header in enumerate(headers):
                if i < len(values):
                    item[header.strip()] = values[i].strip()
            json_result.append(item)
        
        return json_result

# مثال استفاده
if __name__ == "__main__":
    converter = FormatConverter()
    
    # تست JSON به CSV
    sample_json = [
        {"name": "علی", "age": "25", "city": "تهران"},
        {"name": "رضا", "age": "30", "city": "مشهد"}
    ]
    
    result = converter.convert(sample_json, "json", "csv")
    print("نتایج تبدیل JSON به CSV:")
    print(result)
