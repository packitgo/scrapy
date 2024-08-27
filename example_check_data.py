import scrapy
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
import csv
import time
import openpyxl
import pandas as pd

class ExampleSpider(scrapy.Spider):
    name = "example"
    
    def __init__(self, *args, **kwargs):
        super(ExampleSpider, self).__init__(*args, **kwargs)
        self.driver = webdriver.Firefox(service=Service('C:/Users/inyur/Favorites/geckodriver.exe'))
        self.existing_data_file = 'C:/Users/inyur/Scraper/345/myproject/myproject/spiders/results.xlsx'
        self.update_data()

    def update_data(self):
        # Обновляем данные, проверяя на неполные строки
        df = pd.read_excel(self.existing_data_file)
        incomplete_rows = df[df.isnull().any(axis=1)]
        urls_to_retry = incomplete_rows['url'].tolist()  # Замените 'url' на имя колонки с URL
        
        for url in urls_to_retry:
            yield scrapy.Request(url=url, callback=self.parse)

    def start_requests(self):
        with open('C:/Users/inyur/Scraper/product_links.csv', 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader)  # Пропустить заголовок
            links = [row[0] for row in reader if row[0].startswith('http://') or row[0].startswith('https://')]
            for url in links:
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        self.driver.get(response.url)
        self.logger.info(f"Processing URL: {response.url}")

        # Добавление задержки в 5 секунд
        time.sleep(5)

        product_name = None
        price = None
        description = None
        main_image = None
        other_pictures = []
        description_images = []
        product_code = None
        size = None
        tip = None
        thickness = None
        material = None
        color = None
        quantity_in_box = None

        # Попытка извлечь название продукта
        try:
            product_name_element = self.driver.find_element(By.CSS_SELECTOR, 'div.options')
            product_name = product_name_element.text.strip()
        except Exception as e:
            self.logger.warning(f"Primary product name selector failed: {e}")
            try:
                product_name_element = self.driver.find_element(By.CSS_SELECTOR, 'div.name')
                product_name = product_name_element.text.strip()
            except Exception as e:
                self.logger.error(f"Error extracting product name with fallback selector: {e}")

        if product_name:
            self.logger.info(f"Extracted product name: {product_name}")
        else:
            self.logger.warning(f"Product name not found for URL: {response.url}")

        # Попытка извлечь цену продукта
        try:
            price_element = self.driver.find_element(By.CSS_SELECTOR, 'span.p.opensans')
            price = price_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting price: {e}")

        if price:
            self.logger.info(f"Extracted price: {price}")
        else:
            self.logger.warning(f"Price not found for URL: {response.url}")

        # Извлечение описания продукта
        try:
            description_element = self.driver.find_element(By.CSS_SELECTOR, 'div.product_content_desc')
            description = description_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting description: {e}")

        if description:
            self.logger.info(f"Extracted description: {description}")
        else:
            self.logger.warning(f"Description not found for URL: {response.url}")

        # Извлечение главного изображения
        try:
            main_image_element = self.driver.find_element(By.CSS_SELECTOR, 'img[src*="/shopimg_new/"]')
            main_image = main_image_element.get_attribute('src')
        except Exception as e:
            self.logger.error(f"Error extracting main image: {e}")

        if main_image:
            self.logger.info(f"Extracted main image: {main_image}")
        else:
            self.logger.warning(f"Main image not found for URL: {response.url}")

        # Извлечение дополнительных картинок
        try:
            other_pictures_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.swiper-slide img')
            other_pictures = [img.get_attribute('src') for img in other_pictures_elements]
        except Exception as e:
            self.logger.error(f"Error extracting other pictures: {e}")

        if other_pictures:
            self.logger.info(f"Extracted other pictures: {other_pictures}")
        else:
            self.logger.warning(f"Other pictures not found for URL: {response.url}")

        # Извлечение изображений в описании
        try:
            description_images_elements = self.driver.find_elements(By.CSS_SELECTOR, 'div.add_contents img')
            description_images = [img.get_attribute('src') for img in description_images_elements]
        except Exception as e:
            self.logger.error(f"Error extracting description images: {e}")

        if description_images:
            self.logger.info(f"Extracted description images: {description_images}")
        else:
            self.logger.warning(f"Description images not found for URL: {response.url}")

        # Извлечение кода продукта
        try:
            product_code_element = self.driver.find_element(By.CSS_SELECTOR, 'div.product_content_desc span.desc_col_text')
            product_code = product_code_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting product code: {e}")

        if product_code:
            self.logger.info(f"Extracted product code: {product_code}")
        else:
            self.logger.warning(f"Product code not found for URL: {response.url}")

        # Извлечение размера
        try:
            size_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "크기")]/following-sibling::div')
            size = size_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting size: {e}")

        if size:
            self.logger.info(f"Extracted size: {size}")
        else:
            self.logger.warning(f"Size not found for URL: {response.url}")

        # Извлечение типа
        try:
            tip_element = self.driver.find_element(By.CSS_SELECTOR, 'div.info_row div.text')
            tip = tip_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting tip: {e}")

        if tip:
            self.logger.info(f"Extracted tip: {tip}")
        else:
            self.logger.warning(f"Tip not found for URL: {response.url}")

        # Извлечение толщины
        try:
            thickness_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "두께")]/following-sibling::div')
            thickness = thickness_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting thickness: {e}")

        if thickness:
            self.logger.info(f"Extracted thickness: {thickness}")
        else:
            self.logger.warning(f"Thickness not found for URL: {response.url}")

        # Извлечение материала
        try:
            material_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "재질")]/following-sibling::div')
            material = material_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting material: {e}")

        if material:
            self.logger.info(f"Extracted material: {material}")
        else:
            self.logger.warning(f"Material not found for URL: {response.url}")

        # Извлечение цвета
        try:
            color_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "색상")]/following-sibling::div')
            color = color_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting color: {e}")

        if color:
            self.logger.info(f"Extracted color: {color}")
        else:
            self.logger.warning(f"Color not found for URL: {response.url}")

        # Извлечение количества в коробке
        try:
            quantity_in_box_element = self.driver.find_element(By.XPATH, '//div[contains(text(), "포장단위")]/following-sibling::div')
            quantity_in_box = quantity_in_box_element.text.strip()
        except Exception as e:
            self.logger.error(f"Error extracting quantity in box: {e}")

        if quantity_in_box:
            self.logger.info(f"Extracted quantity in box: {quantity_in_box}")
        else:
            self.logger.warning(f"Quantity in box not found for URL: {response.url}")

        # Обновляем данные в Excel
        self.update_excel(response.url, product_name, price, description, main_image, 
                          other_pictures, description_images, product_code, size, 
                          tip, thickness, material, color, quantity_in_box)

    def update_excel(self, url, product_name, price, description, main_image, 
                      other_pictures, description_images, product_code, size, 
                      tip, thickness, material, color, quantity_in_box):
        df = pd.read_excel(self.existing_data_file)
        if url in df['url'].values:
            index = df.index[df['url'] == url].tolist()[0]
            df.loc[index] = [url, product_name, price, description, main_image, 
                             ','.join(other_pictures), ','.join(description_images), product_code, size, 
                             tip, thickness, material, color, quantity_in_box]
        else:
            df = df.append({
                'url': url, 'product_name': product_name, 'price': price, 
                'description': description, 'main_image': main_image, 
                'other_pictures': ','.join(other_pictures), 'description_images': ','.join(description_images),
                'product_code': product_code, 'size': size, 'tip': tip, 
                'thickness': thickness, 'material': material, 'color': color, 
                'quantity_in_box': quantity_in_box
            }, ignore_index=True)
        df.to_excel(self.existing_data_file, index=False)

    def closed(self, reason):
        self.driver.quit()
        self.update_excel()  # Ensure to save data when spider is closed
