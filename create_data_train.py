import os
import time
import json
import requests
import csv
import googlemaps
import json_repair
import re
import random
import pandas as pd  # Nếu cần xử lý dataframe trong tương lai
from unidecode import unidecode
from prompt.prompts import *  # Giả sử các prompt như prompt_province, prompt_district, ... được định nghĩa ở đây

class AddressCleaner:

    DEFAULT_VALUE = "Không xác định"
    MUNICIPAL_CITIES = {"Hồ Chí Minh", "Hà Nội", "Hải Phòng", "Huế", "Cần Thơ", "Đà Nẵng"}
    
    def __init__(self, map_key: str, gemini_key: str, gemini_model_name: str,
                 outliers_path: str = "./utils/outliers_province_district_ward.json",
                 data_address_path: str = "./utils/province_district_ward.json",
                 sleep_time: float = 2.0):
        self.map_key = map_key
        self.gemini_key = gemini_key
        self.gemini_model_name = gemini_model_name
        self.sleep_time = sleep_time

        with open(outliers_path, "r", encoding="utf-8") as fi:
            self.outliers = json.load(fi)
        with open(data_address_path, "r", encoding="utf-8") as fi:
            self.data_address_dict = json.load(fi)

        # Danh sách các tỉnh và phiên bản đã chuyển về dạng không dấu, viết thường
        self.source_province_list = list(self.data_address_dict.keys())
        self.unsign_lower_source_province_list = [unidecode(province).lower() for province in self.source_province_list]

    def _gemini_caller(self, content: str) -> dict:

        url = f"https://generativelanguage.googleapis.com/v1/models/{self.gemini_model_name}:generateContent?key={self.gemini_key}"
        headers = {'Content-Type': 'application/json'}
        data = {
            'contents': [{'parts': [{'text': content}]}],
            'generationConfig': {
                'temperature': 0.2,
                'topP': 0.95,
                'topK': 10
            },
            'safetySettings': [
                {'category': 'HARM_CATEGORY_DANGEROUS_CONTENT', 'threshold': 'BLOCK_NONE'},
                {'category': 'HARM_CATEGORY_HARASSMENT', 'threshold': 'BLOCK_NONE'},
                {'category': 'HARM_CATEGORY_HATE_SPEECH', 'threshold': 'BLOCK_NONE'},
                {'category': 'HARM_CATEGORY_SEXUALLY_EXPLICIT', 'threshold': 'BLOCK_NONE'}
            ],
        }
        time.sleep(5)  # Chờ trước khi gọi API
        try:
            response = requests.post(url=url, headers=headers, json=data)
            if response.status_code == 200:
                response_json = response.json()
                if "candidates" in response_json and response_json["candidates"]:
                    # Trả về nội dung dưới dạng dict sau khi sửa lỗi JSON
                    return json_repair.loads(response_json["candidates"][0]["content"]["parts"][0]["text"])
                else:
                    print("Error: Unexpected response format:", response_json)
            else:
                print(f"Error: Received status code {response.status_code} for key {self.gemini_key}")
        except Exception as e:
            print("Gemini call error:", e)
        return {}

    def _get_zero_shot_prompt(self, prompt: str) -> str:

        start = prompt.find("###########")
        end = prompt.rfind("###########")
        if start != -1 and end != -1:
            return prompt[:start].strip() + "\n" + prompt[end + len("###########"):].strip()
        return prompt

    def _apply_prompt_template(self, template: str, replacements: dict) -> (str, str):

        completed = template
        for key, value in replacements.items():
            completed = completed.replace(f'{{{key}}}', str(value))
        zero_shot = self._get_zero_shot_prompt(completed)
        return completed, zero_shot

    def _get_municipal_city(self, province: str) -> str:

        if province in self.MUNICIPAL_CITIES:
            return f"Thành phố {province}"
        elif province != self.DEFAULT_VALUE:
            return f"Tỉnh {province}"
        return self.DEFAULT_VALUE

    def _province_verification(self, province: str) -> str:

        try:
            index = self.unsign_lower_source_province_list.index(unidecode(province).lower())
            verified = self.source_province_list[index]
        except ValueError:
            verified = self.DEFAULT_VALUE
        if verified == self.DEFAULT_VALUE:
            for src_province in self.source_province_list:
                if unidecode(src_province).lower() in unidecode(province).lower():
                    verified = src_province
                    break
        return verified

    def _get_province_via_google_api(self, raw_address: str) -> str:

        province = self.DEFAULT_VALUE
        try:
            map_client = googlemaps.Client(key=self.map_key)
            geocode_result = map_client.geocode(address=raw_address, components={"country": "VN"})
            if geocode_result:
                for component in geocode_result[0].get("address_components", []):
                    if "administrative_area_level_1" in component.get("types", []):
                        province = component.get("long_name", self.DEFAULT_VALUE)
                        break
        except Exception as e:
            print("Google API error:", e)
        return province

    def clean_province(self, raw_address: str) -> dict:

        province_list_str = ", ".join(self.source_province_list)
        replacements = {
            "province_list_str": province_list_str,
            "raw_address": raw_address
        }
        prompt_completed, zero_shot = self._apply_prompt_template(prompt_province, replacements)
        gemini_output = self._gemini_caller(prompt_completed)
        gemini_province = gemini_output.get("province", self.DEFAULT_VALUE)
        
        if gemini_province != self.DEFAULT_VALUE:
            province = self._get_municipal_city(gemini_province)
        else:
            province_via_google = self._get_province_via_google_api(raw_address)
            verified_province = self._province_verification(province_via_google)
            province = self._get_municipal_city(verified_province) if verified_province != self.DEFAULT_VALUE else self.DEFAULT_VALUE
        
        return {
            "prompt_name_file": "clean_province.txt",
            "origin_prompt": prompt_province,
            "data_to_fill": {"province_list_str": province_list_str, "raw_address": raw_address},
            "completed_prompt": prompt_completed,
            "zero_shot_completed_prompt": zero_shot,
            "gemini_output": {"province": province},
            "quality": "Good" if province != self.DEFAULT_VALUE else "False"
        }

    def clean_district(self, raw_address: str, province: str) -> dict:
        """
        Làm sạch thông tin huyện dựa trên raw_address và province.
        """
        # Xử lý province để loại bỏ tiền tố nếu có
        province_clean = province.replace("Thành phố ", "").replace("Tỉnh ", "")
        district_dict = self.data_address_dict.get(province_clean, {})
        district_list = list(district_dict.keys())
        district_list_str = ", ".join(district_list)
        replacements = {
            "num_district": str(len(district_list)),
            "province": province_clean,
            "district_list_str": district_list_str,
            "raw_address": raw_address
        }
        prompt_completed, zero_shot = self._apply_prompt_template(prompt_district, replacements)
        gemini_output = self._gemini_caller(prompt_completed)
        district = gemini_output.get("district", self.DEFAULT_VALUE)
        
        # Tiền xử lý danh sách district để so sánh không dấu, viết thường
        if district != self.DEFAULT_VALUE:
            masked_list = []
            for d in district_list:
                d_masked = re.sub(r"^(Huyện |Quận |Thành phố |Thị xã )", "", d)
                masked_list.append((unidecode(d_masked).lower(), d))
            masked_list = sorted(masked_list, key=lambda x: len(x[0]), reverse=True)
            district_lower = unidecode(district).lower()
            for masked, original in masked_list:
                if masked in district_lower:
                    district = original
                    break

        return {
            "prompt_name_file": "clean_district.txt",
            "origin_prompt": prompt_district,
            "data_to_fill": {
                "province": province_clean,
                "district_list_str": district_list_str,
                "num_district": str(len(district_list)),
                "raw_address": raw_address
            },
            "completed_prompt": prompt_completed,
            "zero_shot_completed_prompt": zero_shot,
            "gemini_output": {"district": district},
            "quality": "Good" if district != self.DEFAULT_VALUE else "False"
        }

    def clean_ward(self, raw_address: str, province: str, district: str) -> dict:
        """
        Làm sạch thông tin xã/phường dựa trên raw_address, province và district.
        """
        province_clean = province.replace("Thành phố ", "").replace("Tỉnh ", "")
        ward_dict = self.data_address_dict.get(province_clean, {}).get(district, {})
        ward_list = list(ward_dict.keys())
        ward_list_str = ", ".join(ward_list)
        replacements = {
            "num_ward": str(len(ward_list)),
            "district": district,
            "ward_list_str": ward_list_str,
            "raw_address": raw_address
        }
        prompt_completed, zero_shot = self._apply_prompt_template(prompt_ward, replacements)
        gemini_output = self._gemini_caller(prompt_completed)
        ward = gemini_output.get("ward", self.DEFAULT_VALUE)
        
        masked_list = []
        for w in ward_list:
            w_masked = re.sub(r"^(Phường |Thị trấn |Xã )", "", w)
            masked_list.append((unidecode(w_masked).lower(), w))
        masked_list = sorted(masked_list, key=lambda x: len(x[0]), reverse=True)
        ward_lower = unidecode(ward).lower()
        verified_ward = self.DEFAULT_VALUE
        for masked, original in masked_list:
            if masked in ward_lower:
                verified_ward = original
                break

        return {
            "prompt_name_file": "clean_ward.txt",
            "origin_prompt": prompt_ward,
            "data_to_fill": {
                "district": district,
                "ward_list_str": ward_list_str,
                "num_ward": str(len(ward_list)),
                "raw_address": raw_address
            },
            "completed_prompt": prompt_completed,
            "zero_shot_completed_prompt": zero_shot,
            "gemini_output": {"ward": verified_ward},
            "quality": "Good" if verified_ward != self.DEFAULT_VALUE else "False"
        }

    def clean_district_ward(self, raw_address: str, province: str) -> dict:

        province_clean = province.replace("Thành phố ", "").replace("Tỉnh ", "")
        district_dict = self.data_address_dict.get(province_clean, {})
        district_ward_list = []
        for district, ward_dict in district_dict.items():
            for ward in ward_dict.keys():
                district_ward_list.append(f"{ward}, {district}")
        district_ward_list_str = "\n- ".join(district_ward_list)
        replacements = {
            "num_district_ward": str(len(district_ward_list)),
            "province": province_clean,
            "district_ward_list_str": district_ward_list_str,
            "raw_address": raw_address
        }
        prompt_completed, zero_shot = self._apply_prompt_template(prompt_ward_district, replacements)
        gemini_output = self._gemini_caller(prompt_completed)
        ward = gemini_output.get("ward", self.DEFAULT_VALUE)
        district = gemini_output.get("district", self.DEFAULT_VALUE)

        def verified_ward_district(ward_val, district_val, prov):
            if district_val not in self.data_address_dict.get(prov, {}):
                return self.DEFAULT_VALUE, self.DEFAULT_VALUE
            if ward_val not in self.data_address_dict[prov][district_val]:
                return self.DEFAULT_VALUE, self.DEFAULT_VALUE
            if f"{prov}, {district_val}, {ward_val}" in self.outliers:
                return self.DEFAULT_VALUE, self.DEFAULT_VALUE
            return ward_val, district_val

        ward, district = verified_ward_district(ward, district, province_clean)
        return {
            "prompt_name_file": "clean_district_with_ward_provided.txt",
            "origin_prompt": prompt_ward_district,
            "data_to_fill": {
                "province": province_clean,
                "num_district_ward": str(len(district_ward_list)),
                "district_ward_list_str": district_ward_list_str,
                "raw_address": raw_address
            },
            "completed_prompt": prompt_completed,
            "zero_shot_completed_prompt": zero_shot,
            "gemini_output": {"district": district, "ward": ward},
            "quality": "Good" if (ward != self.DEFAULT_VALUE and district != self.DEFAULT_VALUE) else "False"
        }

    def clean_full_address(self, raw_address: str, province: str, district: str, ward: str) -> dict:

        replacements = {
            "province": province,
            "district": district,
            "ward": ward,
            "dirty_address": raw_address
        }
        prompt_completed, zero_shot = self._apply_prompt_template(prompt_full_hint, replacements)
        gemini_output = self._gemini_caller(prompt_completed)
        vn_address = gemini_output.get("vi_address", self.DEFAULT_VALUE)
        en_address = gemini_output.get("en_address", self.DEFAULT_VALUE)
        return {
            "prompt_name_file": "clean_final_address.txt",
            "origin_prompt": prompt_full_hint,
            "data_to_fill": {
                "province": province,
                "district": district,
                "ward": ward,
                "dirty_address": raw_address
            },
            "completed_prompt": prompt_completed,
            "zero_shot_completed_prompt": zero_shot,
            "gemini_output": {"vn_address": vn_address, "en_address": en_address},
            "quality": "Good" if (ward != self.DEFAULT_VALUE and district != self.DEFAULT_VALUE) else "False"
        }

    def cleaned_address_pipeline(self, raw_address: str) -> dict:

        result = {
            "raw_address": raw_address,
            "clean_province": {},
            "clean_district": {},
            "clean_ward": {},
            "clean_district_ward": {},
            "clean_full_address": {}
        }

        province_result = self.clean_province(raw_address)
        result["clean_province"] = province_result
        province = province_result.get("gemini_output", {}).get("province", self.DEFAULT_VALUE)

        if province != self.DEFAULT_VALUE:
            district_result = self.clean_district(raw_address, province)
            result["clean_district"] = district_result
            district = district_result.get("gemini_output", {}).get("district", self.DEFAULT_VALUE)
            time.sleep(self.sleep_time)

            if district != self.DEFAULT_VALUE:
                ward_result = self.clean_ward(raw_address, province, district)
                result["clean_ward"] = ward_result
                ward = ward_result.get("gemini_output", {}).get("ward", self.DEFAULT_VALUE)
            else:
                ward_district_result = self.clean_district_ward(raw_address, province)
                result["clean_district_ward"] = ward_district_result
                district = ward_district_result.get("gemini_output", {}).get("district", self.DEFAULT_VALUE)
                ward = ward_district_result.get("gemini_output", {}).get("ward", self.DEFAULT_VALUE)
            time.sleep(self.sleep_time)
            full_address_result = self.clean_full_address(raw_address, province, district, ward)
            result["clean_full_address"] = full_address_result

        return result