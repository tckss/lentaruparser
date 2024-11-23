import requests
import sqlite3
import colorama
import datetime
from datetime import timedelta
import user_agent
from bs4 import BeautifulSoup

class Logger:
    @staticmethod
    def log(message : str) -> None:
        print(colorama.Fore.GREEN + f"[{datetime.datetime.now()}] - {message}" + colorama.Fore.RESET)
    
    @staticmethod
    def error(message : str) -> None:
        print(colorama.Fore.RED + f"[{datetime.datetime.now()}] - {message}" + colorama.Fore.RESET)
    
    @staticmethod
    def inform(message : str) -> None:
        print(colorama.Fore.CYAN + f"[{datetime.datetime.now()}] - {message}" + colorama.Fore.RESET)
    
    @staticmethod
    def main_message(message : str) -> None:
        print(colorama.Fore.YELLOW + f"\n[{datetime.datetime.now()}] - {message}" + colorama.Fore.RESET)

class Parser:
    def __init__(self) -> None:
        self.session = requests.Session()

        self.host = "https://lenta.ru"

        self.sql_service = SqlService("news_data.sqlite")
    
    def get_request(self, url : str) -> str:
        self.session.headers.update(user_agent.generate_navigator())
        Logger.inform(f"(GET) {url}")
        return self.session.get(url).text

    def parse_all_pages(self) -> None:
        current_date = datetime.datetime(2019, 11, 20)
        temp_upload = []
        while True:
            if current_date >= datetime.datetime(2024, 11, 1):
                break
            
            Logger.main_message(f"Обработка даты {current_date.strftime("%Y/%m/%d")}")
            page = 1
            while True:
                try:
                    soup = BeautifulSoup(self.get_request(f"https://lenta.ru/rubrics/russia/{current_date.strftime("%Y/%m/%d")}/page/{page}/"), "html.parser")
                except:
                    Logger.error("Ошибка при запросе на страницу, повторная попытка...")
                    continue

                if soup.find("h1", {"class" : "not-found__title"}) != None:
                    break
                
                Logger.main_message(f"Обработка страницы {page}")

                for news in soup.find_all("li", {"class" : ["archive-page__item", "_news"]}):
                    try:
                        Logger.log(f"Найдена новость: {news.find("h3").get_text()}")
                    except:
                        continue
                    try:
                        soup = BeautifulSoup(self.get_request(self.host + news.find("a").get("href")), "html.parser")
                    
                    except:
                        Logger.error("Новость-статья (нет необходимых данных)")
                        continue

                    if len(temp_upload) >= 10:
                        self.sql_service.upload_exporting_data(temp_upload)
                        temp_upload = []

                    try:
                        temp_upload.append({
                            "text" : soup.find("span", {"class" : "topic-body__title"}).get_text(strip=True) + "\n" + soup.find("div", {"class" : "topic-body__content"}).get_text(strip=True),
                            "date" : soup.find("a", {"class" : "topic-header__time"}).get_text(strip=True)
                        })
                    
                    except:
                        Logger.log("Ошибка чтения данных")
                        continue
                
                page += 1

            current_date += timedelta(days = 1)


class SqlService:
    def __init__(self, db_path : str) -> None:
        self.db_path = db_path
    
    def upload_exporting_data(self, data : list) -> None:
        Logger.log(f"Подключение к базе данных {self.db_path}...")
        try:
            connection = sqlite3.connect(self.db_path)
        
        except:
            Logger.error("Некорректный ввод пути до базы данных")
            raise Exception("Некорректный ввод пути до базы данных")
        cursor = connection.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS NewsData (
        date VARCHAR(20),
        text TEXT               
        )         
        """)

        cursor.execute("SELECT * FROM NewsData")
        db_before_update = cursor.fetchall()

        uploaded_textes = [x[1] for x in db_before_update]
        to_upload_textes = []

        for element in data:
            if (element["text"] in uploaded_textes):
                continue

            else:
                to_upload_textes.append(element)

        print("\n")
        Logger.log("Выгрузка элементов в базу данных...")

        cursor.executemany("INSERT or IGNORE INTO NewsData(date, text) VALUES(:date, :text)", to_upload_textes)

        connection.commit()

        Logger.log(f"Данные выгружены успешно! (Всего выгружено: {len(to_upload_textes)} элементов)\n")

        connection.close()

if __name__ == "__main__":
    parser = Parser()
    parser.parse_all_pages()
