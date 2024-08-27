import scrapy
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
import csv
import time
import openpyxl
import pandas as pd
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

class DataCompleterSpider(scrapy.Spider):
    name = "data_completer"

    def __init__(self, *args, **kwargs):
        super(DataCompleterSpider, self).__init__(*args, **kwargs)
        self.file_path = 'C:\\Users\\inyur\\Scraper\\345\\myproject\\myproject\\spiders\\results.xlsx'
        self.df = pd.read_excel(self.file_path)
        self.driver = webdriver.Firefox(service=Service('C:/Users/inyur/Favorites/geckodriver.exe'))

    def start_requests(self):
        # Определяем неполные строки
        incomplete_rows = self.df[self.df.isnull().any(axis=1)]

        for index, row in incomplete_rows.iterrows():
            url = row['url']  # Замените 'url' на имя колонки с URL
            yield scrapy.Request(url=url, callback=self.parse, meta={'index': index})

    def parse(self, response):
        index = response.meta['index']
        self.driver.get(response.url)
        self.logger.info(f"Processing URL: {response.url}")

        # Добавление задержки в 5 секунд
        time.sleep(5)

        # Извлечение данных
        product_name = self.extract_data(By.CSS_SELECTOR, 'div.options', 'div.name', 'Product name')
        price = self.extract_data(By.CSS_SELECTOR, 'span.p.opensans', None, 'Price')
        description = self.extract_data(By.CSS_SELECTOR, 'div.product_content_desc', None, 'Description')
        main_image = self.extract_data(By.CSS_SELECTOR, 'img[src*="/shopimg_new/"]', None, 'Main image', attribute='src')
        other_pictures = self.extract_list_data(By.CSS_SELECTOR, 'div.swiper-slide img', 'Other pictures', attribute='src')
        description_images = self.extract_list_data(By.CSS_SELECTOR, 'div.add_contents img', 'Description images', attribute='src')
        product_code = self.extract_data(By.CSS_SELECTOR, 'div.product_content_desc span.desc_col_text', None, 'Product code')
        size = self.extract_data(By.XPATH, '//div[contains(text(), "크기")]/following-sibling::div', None, 'Size')
        tip = self.extract_data(By.CSS_SELECTOR, 'div.info_row div.text', None, 'Tip')
        thickness = self.extract_data(By.XPATH, '//div[contains(text(), "두께")]/following-sibling::div', None, 'Thickness')
        material = self.extract_data(By.XPATH, '//div[contains(text(), "재질")]/following-sibling::div', None, 'Material')
        color = self.extract_data(By.XPATH, '//div[contains(text(), "색상")]/following-sibling::div', None, 'Color')
        quantity_in_box = self.extract_data(By.XPATH, '//div[contains(text(), "포장단위")]/following-sibling::div', None, 'Quantity in box')

        # Обновляем DataFrame
        self.update_results(index, product_name, price, description, main_image, other_pictures, description_images, product_code, size, tip, thickness, material, color, quantity_in_box)

    def extract_data(self, by, value, fallback_by=None, field_name='', attribute=None):
        try:
            if attribute:
                element = self.driver.find_element(by, value)
                return element.get_attribute(attribute).strip()
            else:
                element = self.driver.find_element(by, value)
                return element.text.strip()
        except Exception as e:
            if fallback_by:
                try:
                    element = self.driver.find_element(fallback_by[0], fallback_by[1])
                    return element.text.strip()
                except Exception as e:
                    self.logger.error(f"Error extracting {field_name}: {e}")
            else:
                self.logger.error(f"Error extracting {field_name}: {e}")
            return None

    def extract_list_data(self, by, value, field_name='', attribute=None):
        try:
            elements = self.driver.find_elements(by, value)
            if attribute:
                return [element.get_attribute(attribute).strip() for element in elements]
            else:
                return [element.text.strip() for element in elements]
        except Exception as e:
            self.logger.error(f"Error extracting {field_name}: {e}")
            return []

    def update_results(self, index, product_name, price, description, main_image, other_pictures, description_images, product_code, size, tip, thickness, material, color, quantity_in_box):
        # Обновляем данные в DataFrame
        self.df = pd.read_excel(self.file_path)
        self.df.loc[index] = [
            self.df.loc[index, 'url'], product_name, price, description, main_image,
            ','.join(other_pictures), ','.join(description_images), product_code, size,
            tip, thickness, material, color, quantity_in_box
        ]
        self.df.to_excel(self.file_path, index=False)

    def closed(self, reason):
        self.driver.quit()
        self.workbook.save('C:/Users/inyur/Scraper/345/myproject/myproject/spiders/results.xlsx')

# Запуск паука
if __name__ == "__main__":
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(DataCompleterSpider)
    process.start()
