"""
Модуль для работы с подключениями к Elasticsearch
"""

import requests
from typing import Optional, Dict, Tuple
from rich.console import Console
from rich.markup import escape


class ElasticsearchConnection:
    """Класс для управления подключением к Elasticsearch"""
    
    def __init__(self):
        self.console = Console()
        self.elastic_url = None
        self.elastic_auth = None
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
    
    def set_connection(self, url: str, username: str = None, password: str = None) -> None:
        """Устанавливает параметры подключения"""
        self.elastic_url = url
        if username and password:
            self.elastic_auth = (username, password)
            self.session.auth = self.elastic_auth
        else:
            self.elastic_auth = None
            self.session.auth = None
    
    def check_connection(self) -> bool:
        """Проверяет подключение к Elasticsearch"""
        if not self.elastic_url:
            self.console.print("[red]Не настроено подключение к Elasticsearch. Используйте команду 'connect'[/red]")
            return False
        
        try:
            response = self.session.get(f"{self.elastic_url}/")
            if response.status_code == 200:
                return True
            else:
                self.console.print(f"[red]Ошибка подключения: {escape(response.text)}[/red]")
                return False
        except Exception as e:
            self.console.print(f"[red]Ошибка подключения: {escape(str(e))}[/red]")
            return False
    
    def make_request(self, endpoint: str, method: str = 'GET', data: Dict = None) -> Optional[Dict]:
        """Выполняет запрос к Elasticsearch API"""
        if not self.check_connection():
            return None
        
        try:
            url = f"{self.elastic_url}{endpoint}"
            if method == 'GET':
                response = self.session.get(url)
            elif method == 'POST':
                response = self.session.post(url, json=data)
            elif method == 'PUT':
                response = self.session.put(url, json=data)
            elif method == 'DELETE':
                response = self.session.delete(url)
            
            if response.status_code in [200, 201]:
                if response.content:
                    try:
                        return response.json()
                    except Exception:
                        self.console.print("[red]Ошибка декодирования JSON ответа[/red]")
                        return None
                else:
                    return {"success": True}
            else:
                self.console.print(f"[red]Ошибка API: {response.status_code} - {escape(response.text)}[/red]")
                return None
        except Exception as e:
            self.console.print(f"[red]Ошибка запроса: {escape(str(e))}[/red]")
            return None
    
    def get_connection_info(self) -> Tuple[Optional[str], Optional[Tuple]]:
        """Возвращает информацию о текущем подключении"""
        return self.elastic_url, self.elastic_auth
    
    def clear_connection(self) -> None:
        """Очищает текущее подключение"""
        self.elastic_url = None
        self.elastic_auth = None
        self.session.auth = None
