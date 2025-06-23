#!/usr/bin/env python3
"""
Elasticsearch CLI - Интерактивный инструмент для управления Elasticsearch
"""

import os
import sys
import json
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
import yaml
from dotenv import load_dotenv

# Rich для красивого вывода
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.prompt import Prompt, Confirm
from rich.text import Text
from rich import box
from rich.syntax import Syntax
from rich.markup import escape

# cmd2 для интерактивной оболочки
import cmd2
from cmd2 import with_argparser, with_category
import argparse

# Загружаем переменные окружения
load_dotenv()

class ElasticsearchCLI(cmd2.Cmd):
    """Интерактивный CLI для управления Elasticsearch"""
    
    def __init__(self):
        super().__init__()
        self.console = Console()
        self.prompt = "elastic-cli> "
        self.config_file = os.path.expanduser("~/.elastic-cli/config.yml")
        
        self.elastic_url = None
        self.elastic_auth = None
        
        self.contexts = {}
        self.current_context_name = None

        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json'})
        
        # Создаем директорию для конфига если её нет
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
        
        # Загружаем конфигурацию
        self.load_config()
    
    def _update_prompt(self):
        if self.current_context_name:
            self.prompt = f"({self.current_context_name}) elastic-cli> "
        else:
            self.prompt = "(no context) elastic-cli> "

    def _switch_context(self, context_name: str) -> bool:
        """Переключает активный контекст и проверяет подключение."""
        if context_name not in self.contexts:
            self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
            return False

        context = self.contexts[context_name]
        self.elastic_url = context.get('url')
        username = context.get('username')
        password = context.get('password')

        if username and password:
            self.elastic_auth = (username, password)
            self.session.auth = self.elastic_auth
        else:
            self.elastic_auth = None
            self.session.auth = None

        if self.check_connection():
            self.current_context_name = context_name
            self._update_prompt()
            self.console.print(f"[green]Переключились на контекст: [bold]{context_name}[/bold] ({self.elastic_url})[/green]")
            return True
        else:
            self.console.print(f"[red]Не удалось подключиться к кластеру в контексте '{context_name}'.[/red]")
            # Сбрасываем, чтобы не было путаницы
            self.elastic_url = None
            self.elastic_auth = None
            self.session.auth = None
            self.current_context_name = None
            self._update_prompt()
            return False

    def format_bytes(self, size, decimals=2):
        if not isinstance(size, (int, float)) or size == 0:
            return "0 Bytes"
        import math
        k = 1024
        dm = decimals if decimals >= 0 else 0
        sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        i = math.floor(math.log(size) / math.log(k)) if size > 0 else 0
        return f"{round(size / math.pow(k, i), dm)} {sizes[i]}"

    def _show_index_info(self, index_name: str):
        """Показывает детальную информацию об одном индексе."""
        with self.console.status(f"Загрузка информации для индекса [bold]{index_name}[/bold]..."):
            index_data = self.make_request(f"/{index_name}")
            index_stats = self.make_request(f"/{index_name}/_stats/docs,store")
            sim_data = self.make_request(f"/_index_template/_simulate_index/{index_name}", method='POST')
        
        if not index_data or not index_stats:
            self.console.print(f"[red]Не удалось получить информацию об индексе '{index_name}'. Проверьте имя.[/red]")
            return

        index_details = index_data.get(index_name)
        if not index_details:
            self.console.print(f"[red]Не найден индекс с именем '{index_name}'[/red]")
            return
            
        index_stats_total = index_stats.get('indices', {}).get(index_name, {}).get('total', {})

        stats_store = index_stats_total.get('store', {})
        stats_docs = index_stats_total.get('docs', {})
        settings = index_details.get('settings', {}).get('index', {})

        main_info_table = Table(box=box.MINIMAL, show_header=False)
        main_info_table.add_column(style="dim")
        main_info_table.add_column()
        main_info_table.add_row("UUID", settings.get('uuid', 'N/A'))
        main_info_table.add_row("Документов", f"{stats_docs.get('count', 0):,}")
        main_info_table.add_row("Размер", self.format_bytes(stats_store.get('size_in_bytes', 0)))
        main_info_table.add_row("Шарды", settings.get('number_of_shards', 'N/A'))
        main_info_table.add_row("Реплики", settings.get('number_of_replicas', 'N/A'))
        
        ilm_policy = settings.get('lifecycle', {}).get('name')
        if ilm_policy:
            main_info_table.add_row("ILM Политика", f"[bold green]{ilm_policy}[/bold green]")

        if sim_data and sim_data.get('overlapping_templates'):
            template_names = [t['name'] for t in sim_data['overlapping_templates']]
            main_info_table.add_row("Шаблон(ы)", f"[bold blue]{', '.join(template_names)}[/bold blue]")
        elif sim_data is None:
            main_info_table.add_row("Шаблон(ы)", "[dim]Не удалось получить информацию[/dim]")
        
        self.console.print(Panel(
            main_info_table, 
            title=f"Основная информация: [bold cyan]{index_name}[/bold cyan]",
            border_style="green"
        ))
        
        aliases = index_details.get('aliases', {})
        if aliases:
            aliases_table = Table(title="Алиасы", box=box.ROUNDED)
            aliases_table.add_column("Имя алиаса", style="cyan")
            for alias in aliases:
                aliases_table.add_row(alias)
            self.console.print(aliases_table)

        settings_str = json.dumps(settings, indent=2, ensure_ascii=False)
        self.console.print(Panel(
            Syntax(settings_str, "json", theme="monokai", line_numbers=True),
            title="⚙️ Настройки", border_style="blue", expand=False
        ))

        mappings_str = json.dumps(index_details.get('mappings', {}), indent=2, ensure_ascii=False)
        self.console.print(Panel(
            Syntax(mappings_str, "json", theme="monokai", line_numbers=True),
            title="🗺️ Маппинги", border_style="blue", expand=False
        ))

    def preloop(self):
        """Выполняется перед запуском основного цикла команд."""
        intro_text = """
[bold blue]╔══════════════════════════════════════════════════════════════╗[/bold blue]
[bold blue]║                    [white]Elasticsearch CLI[/white]                         ║[/bold blue]
[bold blue]║              [white]Интерактивное управление кластером[/white]              ║[/bold blue]
[bold blue]╚══════════════════════════════════════════════════════════════╝[/bold blue]

[dim]Подключитесь к кластеру с помощью команды [bold white]connect[/bold white][/dim]
[dim]Введите [bold white]help[/bold white] для получения списка команд[/dim]
"""
        self.console.print(Panel(intro_text, title="Добро пожаловать", border_style="blue", expand=False))
    
    def load_config(self):
        """Загружает конфигурацию из файла"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = yaml.safe_load(f) or {}
                
                self.contexts = config.get('contexts', {})
                current_context = config.get('current_context')

                if current_context and current_context in self.contexts:
                    self._switch_context(current_context)
                else:
                    self._update_prompt()

            except Exception as e:
                self.console.print(f"[red]Ошибка загрузки конфигурации: {escape(str(e))}[/red]")
    
    def save_config(self):
        """Сохраняет конфигурацию в файл"""
        config = {
            'current_context': self.current_context_name,
            'contexts': self.contexts,
        }
        try:
            with open(self.config_file, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
        except Exception as e:
            self.console.print(f"[red]Ошибка сохранения конфигурации: {escape(str(e))}[/red]")
    
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
                    except json.JSONDecodeError:
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
    
    # ==================== КОМАНДЫ ====================
    
    def do_connect(self, arg):
        """Добавить новый контекст подключения: connect <context_name>"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]🔗 Подключение к Elasticsearch кластеру[/bold blue]

[bold]Описание:[/bold]
Создает новый контекст подключения к Elasticsearch кластеру и сохраняет его для последующего использования.

[bold]Синтаксис:[/bold]
[cyan]connect <имя_контекста>[/cyan]

[bold]Параметры:[/bold]
• [cyan]<имя_контекста>[/cyan] - уникальное имя для сохранения подключения

[bold]Процесс подключения:[/bold]
1. Введите URL Elasticsearch (по умолчанию: http://localhost:9200)
2. Укажите имя пользователя (опционально)
3. Введите пароль (если указан пользователь)
4. Система проверит подключение и сохранит контекст

[bold]Примеры:[/bold]
• connect production
• connect staging
• connect local-dev

[bold]После подключения:[/bold]
Используйте [cyan]context use <имя>[/cyan] для переключения между контекстами.

[dim]Примечание: Контекст автоматически становится активным после успешного подключения[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: connect", border_style="blue", expand=False))
            return

        if not arg:
            self.console.print("[red]Необходимо указать имя для нового контекста.[/red]")
            self.console.print("[yellow]Пример: connect my-prod-cluster[/yellow]")
            return
        
        context_name = arg.strip()
        if context_name in self.contexts:
            if not Confirm.ask(f"[yellow]Контекст '{context_name}' уже существует. Перезаписать?[/yellow]"):
                return
        
        url = Prompt.ask("URL Elasticsearch", default="http://localhost:9200")
        username = Prompt.ask("Имя пользователя (или Enter для пропуска)")
        password = Prompt.ask("Пароль", password=True) if username else ""
        
        # Временно устанавливаем для проверки
        self.elastic_url = url
        if username and password:
            self.session.auth = (username, password)
        else:
            self.session.auth = None

        with self.console.status("Проверка подключения..."):
            if self.check_connection():
                self.contexts[context_name] = {
                    'url': url,
                    'username': username,
                    'password': password
                }
                self.console.print(f"[green]Контекст '{context_name}' сохранен.[/green]")
                self._switch_context(context_name) # Переключаемся на новый контекст
                self.save_config()
            else:
                self.console.print("[red]Не удалось подключиться к кластеру. Контекст не сохранен.[/red]")

    def do_context(self, arg):
        """Управление контекстами подключений.
Использование:
- context list: Показать все сохраненные контексты.
- context use <name>: Переключиться на другой контекст.
- context delete <name>: Удалить контекст.
- context show <name>: Показать детали контекста.
"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]🗂️ Управление контекстами подключений[/bold blue]

[bold]Описание:[/bold]
Контексты позволяют сохранять и быстро переключаться между различными подключениями к Elasticsearch кластерам.

[bold]Команды:[/bold]
• [cyan]context list[/cyan] - показать все сохраненные контексты подключений
• [cyan]context use <имя>[/cyan] - переключиться на указанный контекст
• [cyan]context delete <имя>[/cyan] - удалить контекст (с подтверждением)
• [cyan]context show <имя>[/cyan] - показать детали контекста

[bold]Примеры:[/bold]
• context list
• context use production
• context delete old-cluster
• context show staging

[bold]Создание контекста:[/bold]
Используйте команду [cyan]connect <имя_контекста>[/cyan] для создания нового контекста.

[dim]Примечание: Контексты сохраняются в ~/.elastic-cli/config.yml[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: context", border_style="blue", expand=False))
            return

        parts = arg.split()
        if not parts:
            self.console.print("[red]Необходимо указать команду для 'context'. Доступные: list, use, delete, show.[/red]")
            return

        command = parts[0]
        if command == 'list':
            table = Table(title="🗂️ Контексты подключений", box=box.ROUNDED)
            table.add_column("Активный", style="green")
            table.add_column("Имя", style="cyan")
            table.add_column("URL", style="magenta")
            table.add_column("Пользователь", style="yellow")
            
            for name, details in self.contexts.items():
                is_active = "✅" if name == self.current_context_name else ""
                table.add_row(is_active, name, details.get('url'), details.get('username', 'N/A'))
            self.console.print(table)
        
        elif command == 'use':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для переключения.[/red]")
                return
            context_name = parts[1]
            if self._switch_context(context_name):
                self.save_config()

        elif command == 'delete':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для удаления.[/red]")
                return
            context_name = parts[1]
            if context_name not in self.contexts:
                self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
                return
            
            if Confirm.ask(f"Вы уверены, что хотите удалить контекст '{context_name}'?"):
                del self.contexts[context_name]
                if self.current_context_name == context_name:
                    self.current_context_name = None
                    self.elastic_url = None
                    self.session.auth = None
                    self._update_prompt()
                self.save_config()
                self.console.print(f"[green]Контекст '{context_name}' удален.[/green]")

        elif command == 'show':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для просмотра.[/red]")
                return
            context_name = parts[1]
            if context_name not in self.contexts:
                self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
                return
            
            details = self.contexts[context_name]
            panel = Panel(
                f"[bold]URL:[/] {details.get('url')}\n"
                f"[bold]Пользователь:[/] {details.get('username') or 'N/A'}",
                title=f"Детали контекста: [cyan]{context_name}[/cyan]",
                border_style="blue"
            )
            self.console.print(panel)
        else:
            self.console.print(f"[red]Неизвестная команда для 'context': '{command}'.[/red]")
            self.console.print("[yellow]Доступные команды: list, use, delete, show.[/yellow]")
    
    def do_health(self, arg):
        """Показать здоровье кластера"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]🏥 Здоровье кластера Elasticsearch[/bold blue]

[bold]Описание:[/bold]
Показывает текущее состояние здоровья кластера Elasticsearch, включая статус, количество узлов и шардов.

[bold]Синтаксис:[/bold]
[cyan]health[/cyan]

[bold]Отображаемая информация:[/bold]
• [bold]Имя кластера[/bold] - название кластера
• [bold]Статус[/bold] - green/yellow/red (цветовая индикация)
• [bold]Количество узлов[/bold] - общее число узлов в кластере
• [bold]Активные шарды[/bold] - количество активных шардов
• [bold]Активные первичные шарды[/bold] - количество первичных шардов
• [bold]Перемещающиеся шарды[/bold] - шарды в процессе перемещения
• [bold]Инициализирующиеся шарды[/bold] - шарды в процессе инициализации
• [bold]Неназначенные шарды[/bold] - шарды без назначенного узла

[bold]Статусы кластера:[/bold]
• [green]green[/green] - все шарды активны
• [yellow]yellow[/yellow] - все первичные шарды активны, но некоторые реплики недоступны
• [red]red[/red] - некоторые первичные шарды недоступны

[bold]Пример:[/bold]
• health

[dim]Примечание: Команда не требует дополнительных параметров[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: health", border_style="blue", expand=False))
            return

        data = self.make_request("/_cluster/health")
        if not data:
            return
        
        # Создаем красивую таблицу
        table = Table(title="🏥 Здоровье кластера", box=box.ROUNDED)
        table.add_column("Параметр", style="cyan", no_wrap=True)
        table.add_column("Значение", style="magenta")
        
        status_color = {
            'green': 'green',
            'yellow': 'yellow', 
            'red': 'red'
        }
        
        table.add_row("Имя кластера", data.get('cluster_name', 'N/A'))
        table.add_row("Статус", f"[{status_color.get(data.get('status', 'white'), 'white')}]{data.get('status', 'N/A')}[/{status_color.get(data.get('status', 'white'), 'white')}]")
        table.add_row("Количество узлов", str(data.get('number_of_nodes', 0)))
        table.add_row("Активные шарды", str(data.get('active_shards', 0)))
        table.add_row("Активные первичные шарды", str(data.get('active_primary_shards', 0)))
        table.add_row("Перемещающиеся шарды", str(data.get('relocating_shards', 0)))
        table.add_row("Инициализирующиеся шарды", str(data.get('initializing_shards', 0)))
        table.add_row("Неназначенные шарды", str(data.get('unassigned_shards', 0)))
        
        self.console.print(table)
    
    def do_nodes(self, arg):
        """Показать информацию об узлах кластера"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]🖥️ Информация об узлах кластера[/bold blue]

[bold]Описание:[/bold]
Показывает детальную информацию о всех узлах в кластере Elasticsearch, включая их роли, использование ресурсов и статус.

[bold]Синтаксис:[/bold]
[cyan]nodes[/cyan]

[bold]Отображаемая информация:[/bold]
• [bold]Имя узла[/bold] - название узла
• [bold]ID[/bold] - уникальный идентификатор узла (сокращенный)
• [bold]Роли[/bold] - роли узла в кластере (data, master, ingest)
• [bold]CPU %[/bold] - процент использования процессора
• [bold]Память %[/bold] - процент использования памяти
• [bold]Диск %[/bold] - процент использования дискового пространства

[bold]Роли узлов:[/bold]
• [cyan]data[/cyan] - узел данных (хранит индексы)
• [cyan]master[/cyan] - мастер-узел (управляет кластером)
• [cyan]ingest[/cyan] - узел обработки (выполняет pipeline)

[bold]Пример:[/bold]
• nodes

[bold]Мониторинг:[/bold]
Используйте эту команду для мониторинга производительности и состояния узлов кластера.

[dim]Примечание: Команда не требует дополнительных параметров[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: nodes", border_style="blue", expand=False))
            return

        data = self.make_request("/_nodes/stats")
        if not data:
            return
        
        table = Table(title="🖥️ Узлы кластера", box=box.ROUNDED)
        table.add_column("Имя узла", style="cyan")
        table.add_column("ID", style="blue")
        table.add_column("Роли", style="green")
        table.add_column("CPU %", style="yellow")
        table.add_column("Память %", style="magenta")
        table.add_column("Диск %", style="red")
        
        for node_id, node_data in data['nodes'].items():
            stats = node_data.get('os', {})
            process = node_data.get('process', {})
            
            cpu_percent = stats.get('cpu', {}).get('percent', 0)
            mem_percent = stats.get('mem', {}).get('used_percent', 0)
            
            # Расчет использования диска
            fs_stats = node_data.get('fs', {})
            total_disk = fs_stats.get('total', {}).get('total_in_bytes', 0)
            free_disk = fs_stats.get('total', {}).get('free_in_bytes', 0)
            disk_percent = ((total_disk - free_disk) / total_disk * 100) if total_disk > 0 else 0
            
            # Роли узла
            roles = []
            if node_data.get('settings', {}).get('node', {}).get('data', False):
                roles.append('data')
            if node_data.get('settings', {}).get('node', {}).get('master', False):
                roles.append('master')
            if node_data.get('settings', {}).get('node', {}).get('ingest', False):
                roles.append('ingest')
            
            table.add_row(
                node_data.get('name', 'N/A'),
                node_id[:8] + '...',
                ', '.join(roles),
                f"{cpu_percent:.1f}%",
                f"{mem_percent:.1f}%",
                f"{disk_percent:.1f}%"
            )
        
        self.console.print(table)
    
    def do_indices(self, arg):
        """Управление индексами.
Использование:
- indices: показать все индексы
- indices <имя_индекса>: показать детальную информацию
- indices <команда> <имя_индекса>: выполнить команду (delete, open, close, settings, forcemerge)
- indices forcemerge <имя_индекса> <тип>: выполнить forcemerge
  - тип: segments - для max_num_segments=N & waiting for completion=false
  - тип: expunge - для only_expunge_deletes=true & waiting for completion=false
"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]📚 Управление индексами Elasticsearch[/bold blue]

[bold]Основные команды:[/bold]
• [cyan]indices[/cyan] - показать список всех индексов с основной информацией
• [cyan]indices <имя_индекса>[/cyan] - показать детальную информацию об индексе

[bold]Операции с индексами:[/bold]
• [cyan]indices delete <имя_индекса>[/cyan] - удалить индекс (с подтверждением)
• [cyan]indices close <имя_индекса>[/cyan] - закрыть индекс
• [cyan]indices open <имя_индекса>[/cyan] - открыть индекс
• [cyan]indices settings <имя_индекса>[/cyan] - показать настройки индекса

[bold]Forcemerge операции:[/bold]
• [cyan]indices forcemerge <имя_индекса> segments[/cyan] - выполнить forcemerge с указанием количества сегментов
  - Запрашивает количество сегментов (N)
  - Выполняет: _forcemerge?max_num_segments=N&wait_for_completion=false
  
• [cyan]indices forcemerge <имя_индекса> expunge[/cyan] - выполнить forcemerge только для удаленных документов
  - Выполняет: _forcemerge?only_expunge_deletes=true&wait_for_completion=false

[bold]Примеры:[/bold]
• indices
• indices my-index
• indices delete old-index
• indices forcemerge my-index segments
• indices forcemerge my-index expunge

[dim]Примечание: Все операции с индексами требуют подтверждения для безопасности[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: indices", border_style="blue", expand=False))
            return

        if not arg:
            # Показать список индексов
            data = self.make_request("/_cat/indices?format=json&v")
            if not data:
                return
            
            table = Table(title="📚 Индексы", box=box.ROUNDED)
            table.add_column("Индекс", style="cyan")
            table.add_column("Статус", style="green")
            table.add_column("Документы", style="blue")
            table.add_column("Размер", style="yellow")
            table.add_column("Первичные шарды", style="magenta")
            table.add_column("Реплики", style="red")
            
            for index in data:
                status_color = {
                    'green': 'green',
                    'yellow': 'yellow',
                    'red': 'red'
                }
                
                table.add_row(
                    index.get('index', 'N/A'),
                    f"[{status_color.get(index.get('health', 'white'), 'white')}]{index.get('health', 'N/A')}[/{status_color.get(index.get('health', 'white'), 'white')}]",
                    index.get('docs.count', '0'),
                    index.get('store.size', '0b'),
                    index.get('pri', '0'),
                    index.get('rep', '0')
                )
            
            self.console.print(table)
        else:
            # Обработка команд для индексов
            parts = arg.split()
            command = parts[0]
            
            if command in ("delete", "close", "open", "settings", "forcemerge"):
                if len(parts) < 2:
                    self.console.print(f"[red]Ошибка: для команды '{command}' необходимо указать имя индекса.[/red]")
                    return
                
                index_name = parts[1]

                if command == "delete":
                    if Confirm.ask(f"Удалить индекс '{index_name}'?"):
                        result = self.make_request(f"/{index_name}", method="DELETE")
                        if result:
                            self.console.print(f"[green]Индекс '{index_name}' удален[/green]")
                
                elif command == "close":
                    result = self.make_request(f"/{index_name}/_close", method="POST")
                    if result:
                        self.console.print(f"[green]Индекс '{index_name}' закрыт[/green]")
                
                elif command == "open":
                    result = self.make_request(f"/{index_name}/_open", method="POST")
                    if result:
                        self.console.print(f"[green]Индекс '{index_name}' открыт[/green]")
                
                elif command == "settings":
                    data = self.make_request(f"/{index_name}/_settings")
                    if data:
                        settings_str = json.dumps(data, indent=2, ensure_ascii=False)
                        self.console.print(Panel(
                            Syntax(settings_str, "json", theme="monokai", line_numbers=True),
                            title=f"Настройки индекса {index_name}",
                            border_style="blue"
                        ))
                
                elif command == "forcemerge":
                    if len(parts) < 3:
                        self.console.print("[red]Ошибка: для команды 'forcemerge' необходимо указать тип операции.[/red]")
                        self.console.print("[yellow]Доступные типы: segments, expunge[/yellow]")
                        return
                    
                    merge_type = parts[2]
                    
                    if merge_type == "segments":
                        if Confirm.ask(f"Выполнить forcemerge с max_num_segments для индекса '{index_name}'?"):
                            # Запрашиваем количество сегментов
                            max_segments = Prompt.ask("Количество сегментов (N)", default="1")
                            try:
                                max_segments = int(max_segments)
                                result = self.make_request(f"/{index_name}/_forcemerge?max_num_segments={max_segments}&wait_for_completion=false", method="POST")
                                if result:
                                    self.console.print(f"[green]Forcemerge запущен для индекса '{index_name}' с max_num_segments={max_segments}[/green]")
                            except ValueError:
                                self.console.print("[red]Ошибка: количество сегментов должно быть числом.[/red]")
                    
                    elif merge_type == "expunge":
                        if Confirm.ask(f"Выполнить forcemerge с only_expunge_deletes для индекса '{index_name}'?"):
                            result = self.make_request(f"/{index_name}/_forcemerge?only_expunge_deletes=true&wait_for_completion=false", method="POST")
                            if result:
                                self.console.print(f"[green]Forcemerge запущен для индекса '{index_name}' с only_expunge_deletes=true[/green]")
                    
                    else:
                        self.console.print(f"[red]Неизвестный тип forcemerge: '{merge_type}'[/red]")
                        self.console.print("[yellow]Доступные типы: segments, expunge[/yellow]")
            else:
                # Если не команда, то это имя индекса
                index_name = command
                self._show_index_info(index_name)
    
    def do_shards(self, arg):
        """Показать информацию о шардах"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]🔗 Информация о шардах[/bold blue]

[bold]Описание:[/bold]
Показывает детальную информацию о всех шардах в кластере, сгруппированную по статусу. Помогает диагностировать проблемы с распределением данных.

[bold]Синтаксис:[/bold]
[cyan]shards[/cyan]

[bold]Отображаемая информация:[/bold]
• [bold]Индекс[/bold] - название индекса
• [bold]Шард[/bold] - номер шарда
• [bold]Узел[/bold] - узел, на котором размещен шард
• [bold]Размер[/bold] - размер шарда на диске
• [bold]Документы[/bold] - количество документов в шарде

[bold]Статусы шардов:[/bold]
• [green]STARTED[/green] - шард активен и работает
• [yellow]RELOCATING[/yellow] - шард перемещается на другой узел
• [blue]INITIALIZING[/blue] - шард инициализируется
• [red]UNASSIGNED[/red] - шард не назначен ни одному узлу

[bold]Группировка:[/bold]
Информация группируется по статусу для удобства анализа проблем.

[bold]Пример:[/bold]
• shards

[bold]Диагностика:[/bold]
Используйте для выявления проблем с распределением шардов и их состоянием.

[dim]Примечание: Команда не требует дополнительных параметров[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: shards", border_style="blue", expand=False))
            return

        data = self.make_request("/_cat/shards?format=json&v")
        if not data:
            return
        
        # Группируем по статусу
        status_groups = {}
        for shard in data:
            status = shard.get('state', 'unknown')
            if status not in status_groups:
                status_groups[status] = []
            status_groups[status].append(shard)
        
        for status, shards in status_groups.items():
            status_color = {
                'STARTED': 'green',
                'RELOCATING': 'yellow',
                'INITIALIZING': 'blue',
                'UNASSIGNED': 'red'
            }
            
            table = Table(
                title=f"🔗 Шарды - {status} ({len(shards)})",
                box=box.ROUNDED
            )
            table.add_column("Индекс", style="cyan")
            table.add_column("Шард", style="blue")
            table.add_column("Узел", style="green")
            table.add_column("Размер", style="yellow")
            table.add_column("Документы", style="magenta")
            
            for shard in shards:
                table.add_row(
                    shard.get('index', 'N/A'),
                    shard.get('shard', 'N/A'),
                    shard.get('node', 'N/A'),
                    shard.get('store', '0b'),
                    shard.get('docs', '0')
                )
            
            self.console.print(table)
    
    def do_tasks(self, arg):
        """Показать активные задачи"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]⚡ Активные задачи в кластере[/bold blue]

[bold]Описание:[/bold]
Показывает список всех активных задач, выполняющихся в кластере Elasticsearch. Включает задачи индексации, поиска, обновления и другие операции.

[bold]Синтаксис:[/bold]
[cyan]tasks[/cyan]

[bold]Отображаемая информация:[/bold]
• [bold]Узел[/bold] - узел, на котором выполняется задача
• [bold]ID задачи[/bold] - уникальный идентификатор задачи
• [bold]Тип[/bold] - тип задачи (transport, direct, etc.)
• [bold]Действие[/bold] - выполняемое действие
• [bold]Описание[/bold] - краткое описание задачи

[bold]Типы задач:[/bold]
• [cyan]transport[/cyan] - задачи, выполняемые через транспортный слой
• [cyan]direct[/cyan] - прямые задачи на узле
• [cyan]cluster:monitor[/cyan] - задачи мониторинга кластера
• [cyan]indices:data/read[/cyan] - операции чтения данных
• [cyan]indices:data/write[/cyan] - операции записи данных

[bold]Пример:[/bold]
• tasks

[bold]Мониторинг:[/bold]
Используйте для отслеживания активности кластера и выявления длительных операций.

[dim]Примечание: Команда не требует дополнительных параметров[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: tasks", border_style="blue", expand=False))
            return

        data = self.make_request("/_tasks")
        if not data:
            return
        
        tasks = data.get('nodes', {})
        if not tasks:
            self.console.print("[yellow]Нет активных задач[/yellow]")
            return
        
        table = Table(title="⚡ Активные задачи", box=box.ROUNDED)
        table.add_column("Узел", style="cyan")
        table.add_column("ID задачи", style="blue")
        table.add_column("Тип", style="green")
        table.add_column("Действие", style="yellow")
        table.add_column("Описание", style="magenta")
        
        for node_id, node_tasks in tasks.items():
            for task_id, task_data in node_tasks.get('tasks', {}).items():
                table.add_row(
                    node_id[:8] + '...',
                    task_id,
                    task_data.get('type', 'N/A'),
                    task_data.get('action', 'N/A'),
                    task_data.get('description', 'N/A')[:50] + '...' if len(task_data.get('description', '')) > 50 else task_data.get('description', 'N/A')
                )
        
        self.console.print(table)
    
    def do_snapshots(self, arg):
        """Управление снапшотами"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]📸 Управление снапшотами[/bold blue]

[bold]Описание:[/bold]
Снапшоты позволяют создавать резервные копии индексов и восстанавливать их при необходимости. Снапшоты сохраняются в репозиториях.

[bold]Команды:[/bold]
• [cyan]snapshots[/cyan] - показать все репозитории снапшотов
• [cyan]snapshots <репозиторий> list[/cyan] - показать все снапшоты в указанном репозитории

[bold]Отображаемая информация:[/bold]
• [bold]Репозиторий[/bold] - название репозитория для хранения снапшотов
• [bold]Тип[/bold] - тип репозитория (fs, s3, hdfs, etc.)
• [bold]Настройки[/bold] - конфигурация репозитория
• [bold]Имя снапшота[/bold] - название снапшота
• [bold]Статус[/bold] - состояние снапшота (SUCCESS, FAILED, IN_PROGRESS)
• [bold]Индексы[/bold] - количество индексов в снапшоте
• [bold]Размер[/bold] - общий размер снапшота
• [bold]Дата создания[/bold] - время создания снапшота

[bold]Примеры:[/bold]
• snapshots
• snapshots my-repo list

[bold]Создание репозиториев:[/bold]
Репозитории создаются через Elasticsearch API или Kibana.

[dim]Примечание: Снапшоты требуют предварительно настроенного репозитория[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: snapshots", border_style="blue", expand=False))
            return

        if not arg:
            # Показать репозитории
            data = self.make_request("/_snapshot")
            if not data:
                return
            
            table = Table(title="📸 Репозитории снапшотов", box=box.ROUNDED)
            table.add_column("Репозиторий", style="cyan")
            table.add_column("Тип", style="blue")
            table.add_column("Настройки", style="green")
            
            for repo_name, repo_data in data.items():
                table.add_row(
                    repo_name,
                    repo_data.get('type', 'N/A'),
                    str(repo_data.get('settings', {}))
                )
            
            self.console.print(table)
        else:
            parts = arg.split()
            if len(parts) >= 2:
                repo = parts[0]
                command = parts[1]
                
                if command == "list":
                    data = self.make_request(f"/_snapshot/{repo}/_all")
                    if data and 'snapshots' in data:
                        table = Table(title=f"📸 Снапшоты в {repo}", box=box.ROUNDED)
                        table.add_column("Имя", style="cyan")
                        table.add_column("Статус", style="blue")
                        table.add_column("Индексы", style="green")
                        table.add_column("Размер", style="yellow")
                        table.add_column("Дата создания", style="magenta")
                        
                        for snapshot in data['snapshots']:
                            table.add_row(
                                snapshot.get('snapshot', 'N/A'),
                                snapshot.get('state', 'N/A'),
                                str(len(snapshot.get('indices', []))),
                                snapshot.get('stats', {}).get('total_size', 'N/A'),
                                snapshot.get('start_time', 'N/A')
                            )
                        
                        self.console.print(table)
    
    def do_settings(self, arg):
        """Показать настройки кластера"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]⚙️ Настройки кластера[/bold blue]

[bold]Описание:[/bold]
Показывает текущие настройки кластера Elasticsearch, включая настройки по умолчанию и динамические настройки.

[bold]Синтаксис:[/bold]
[cyan]settings[/cyan]

[bold]Отображаемая информация:[/bold]
• [bold]Настройки по умолчанию[/bold] - статические настройки кластера
• [bold]Динамические настройки[/bold] - настройки, которые можно изменять без перезапуска
• [bold]Транзиентные настройки[/bold] - временные настройки, которые сбрасываются при перезапуске

[bold]Категории настроек:[/bold]
• [cyan]cluster[/cyan] - настройки кластера (имя, настройки узлов)
• [cyan]indices[/cyan] - настройки индексов (размер шардов, реплики)
• [cyan]path[/cyan] - пути к данным и логам
• [cyan]network[/cyan] - сетевые настройки
• [cyan]discovery[/cyan] - настройки обнаружения узлов

[bold]Пример:[/bold]
• settings

[bold]Изменение настроек:[/bold]
Настройки изменяются через Elasticsearch API или файл конфигурации.

[dim]Примечание: Команда не требует дополнительных параметров[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: settings", border_style="blue", expand=False))
            return

        data = self.make_request("/_cluster/settings")
        if not data:
            return
        
        self.console.print(Panel(
            json.dumps(data, indent=2, ensure_ascii=False),
            title="⚙️ Настройки кластера",
            border_style="blue"
        ))
    
    def do_quit(self, arg):
        """Выход из CLI"""
        self.console.print("[yellow]До свидания! 👋[/yellow]")
        return True
    
    def do_exit(self, arg):
        """Выход из CLI"""
        return self.do_quit(arg)
    
    def do_EOF(self, arg):
        """Выход по Ctrl+D"""
        return self.do_quit(arg)

    def do_ilm(self, arg):
        """Управление ILM политиками.
Использование:
- ilm list: Показать все ILM политики в кластере.
- ilm show <policy_name>: Показать JSON определение конкретной политики.
- ilm explain <index_name>: Показать текущий статус и фазу ILM для конкретного индекса.
"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]📜 Управление ILM политиками[/bold blue]

[bold]Описание:[/bold]
Index Lifecycle Management (ILM) позволяет автоматически управлять жизненным циклом индексов, включая создание, обновление и удаление на основе политик.

[bold]Команды:[/bold]
• [cyan]ilm list[/cyan] - показать все ILM политики в кластере
• [cyan]ilm show <имя_политики>[/cyan] - показать JSON определение конкретной политики
• [cyan]ilm explain <имя_индекса>[/cyan] - показать текущий статус и фазу ILM для конкретного индекса

[bold]Отображаемая информация:[/bold]
• [bold]Имя политики[/bold] - название политики ILM
• [bold]Версия[/bold] - версия политики
• [bold]Дата изменения[/bold] - дата последнего изменения политики
• [bold]Фаза[/bold] - текущая фаза жизненного цикла (hot, warm, cold, delete)
• [bold]Действие[/bold] - выполняемое действие в текущей фазе

[bold]Примеры:[/bold]
• ilm list
• ilm show my-policy
• ilm explain my-index-2024.01.01

[bold]Создание политик:[/bold]
Политики ILM создаются через Elasticsearch API или Kibana.

[dim]Примечание: ILM автоматически управляет индексами согласно настроенным политикам[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: ilm", border_style="blue", expand=False))
            return

        parts = arg.split()
        if not parts:
            self.console.print("[red]Необходимо указать команду: list, show <policy_name> или explain <index_name>[/red]")
            return

        command = parts[0]
        if command == "list":
            if len(parts) > 1:
                self.console.print("[red]Ошибка: команда 'list' не принимает дополнительных аргументов.[/red]")
                return

            data = self.make_request("/_ilm/policy")
            if not data:
                return

            table = Table(title="📜 ILM Политики", box=box.ROUNDED)
            table.add_column("Имя политики", style="cyan")
            table.add_column("Версия", style="blue")
            table.add_column("Дата изменения", style="yellow")
            
            for name, policy_data in data.items():
                table.add_row(
                    name,
                    str(policy_data.get('version', 'N/A')),
                    policy_data.get('modified_date', 'N/A')
                )
            self.console.print(table)
        
        elif command == "show":
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя политики[/red]")
                return
            policy_name = parts[1]
            data = self.make_request(f"/_ilm/policy/{policy_name}")
            if data:
                policy_content = data.get(policy_name, {}).get('policy', {})
                policy_str = json.dumps(policy_content, indent=2, ensure_ascii=False)
                self.console.print(Panel(
                    Syntax(policy_str, "json", theme="monokai", line_numbers=True),
                    title=f"📜 ILM Политика: {policy_name}",
                    border_style="blue"
                ))
            else:
                # Подсказка пользователю
                self.console.print(f"[yellow]Подсказка: не найдена политика с именем '{policy_name}'.[/yellow]")
                self.console.print(f"[yellow]Возможно, вы хотели узнать статус для индекса? Попробуйте: [bold]ilm explain {policy_name}[/bold][/yellow]")
        
        elif command == "explain":
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя индекса[/red]")
                return
            index_name = parts[1]
            data = self.make_request(f"/{index_name}/_ilm/explain")
            if data:
                index_info = data.get('indices', {}).get(index_name, {})
                
                # Создаем красивую таблицу для вывода
                table = Table(title=f"🌡️ Статус ILM для индекса [bold]{index_name}[/bold]", box=box.ROUNDED)
                table.add_column("Параметр", style="cyan", no_wrap=True)
                table.add_column("Значение", style="magenta")

                table.add_row("Управляется ILM?", "[green]Да[/green]" if index_info.get('managed', False) else "[red]Нет[/red]")
                table.add_row("Политика", index_info.get('policy', 'N/A'))
                table.add_row("Фаза", index_info.get('phase', 'N/A'))
                table.add_row("Действие", index_info.get('action', 'N/A'))
                table.add_row("Шаг", index_info.get('step', 'N/A'))
                
                step_info = index_info.get('step_info')
                if step_info:
                    step_info_str = json.dumps(step_info, indent=2, ensure_ascii=False)
                    table.add_row("Детали шага", Syntax(step_info_str, "json", theme="monokai"))

                self.console.print(table)
            else:
                self.console.print(f"[red]Не удалось получить информацию об ILM для индекса '{index_name}'.[/red]")
        else:
            self.console.print(f"[red]Неизвестная команда для 'ilm': '{command}'.[/red]")
            self.console.print("[yellow]Доступные команды: list, show, explain.[/yellow]")

    def do_templates(self, arg):
        """Управление шаблонами индексов.
Использование:
- templates list: Показать все шаблоны индексов.
- templates show <template_name>: Показать JSON определение конкретного шаблона.
"""
        # Обработка команды help
        if arg in ["-h", "--help", "help"]:
            help_text = """
[bold blue]📄 Управление шаблонами индексов[/bold blue]

[bold]Описание:[/bold]
Шаблоны индексов позволяют автоматически применять настройки, маппинги и алиасы к новым индексам, соответствующим определенным паттернам.

[bold]Команды:[/bold]
• [cyan]templates list[/cyan] - показать все шаблоны индексов в кластере
• [cyan]templates show <имя_шаблона>[/cyan] - показать JSON определение конкретного шаблона

[bold]Отображаемая информация:[/bold]
• [bold]Имя шаблона[/bold] - название шаблона
• [bold]Приоритет[/bold] - приоритет применения шаблона (высокий приоритет применяется первым)
• [bold]Паттерн индексов[/bold] - паттерны имен индексов, к которым применяется шаблон

[bold]Примеры:[/bold]
• templates list
• templates show my-template
• templates show logstash-*

[bold]Создание шаблонов:[/bold]
Шаблоны создаются через Elasticsearch API или Kibana.

[dim]Примечание: Шаблоны применяются автоматически при создании новых индексов[/dim]
"""
            self.console.print(Panel(help_text, title="Справка: templates", border_style="blue", expand=False))
            return

        parts = arg.split()
        if not parts:
            self.console.print("[red]Необходимо указать команду: list или show <template_name>[/red]")
            return

        command = parts[0]
        if command == "list":
            if len(parts) > 1:
                self.console.print("[red]Ошибка: команда 'list' не принимает дополнительных аргументов.[/red]")
                return

            data = self.make_request("/_index_template")
            if not data:
                return
            
            table = Table(title="📄 Шаблоны индексов", box=box.ROUNDED)
            table.add_column("Имя шаблона", style="cyan")
            table.add_column("Приоритет", style="blue")
            table.add_column("Паттерн индексов", style="yellow")
            
            for template in data.get('_index_templates', []):
                template_name = template.get('name', 'N/A')
                template_body = template.get('index_template', {})
                patterns = ', '.join(template_body.get('index_patterns', []))
                priority = str(template_body.get('priority', 'N/A'))
                table.add_row(template_name, priority, patterns)
            self.console.print(table)
            
        elif command == "show":
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя шаблона[/red]")
                return
            template_name = parts[1]
            data = self.make_request(f"/_index_template/{template_name}")
            if data:
                template_content = data.get('index_templates', [{}])[0].get('index_template', {})
                template_str = json.dumps(template_content, indent=2, ensure_ascii=False)
                self.console.print(Panel(
                    Syntax(template_str, "json", theme="monokai", line_numbers=True),
                    title=f"📄 Шаблон: {template_name}",
                    border_style="blue"
                ))
            else:
                self.console.print(f"[yellow]Подсказка: не найден шаблон с именем '{template_name}'.[/yellow]")
                self.console.print(f"[yellow]Чтобы узнать, какой шаблон применен к индексу, используйте: [bold]indices {template_name}[/bold][/yellow]")
        else:
            self.console.print(f"[red]Неизвестная команда для 'templates': '{command}'.[/red]")
            self.console.print("[yellow]Доступные команды: list, show.[/yellow]")

def main():
    """Главная функция"""
    try:
        cli = ElasticsearchCLI()
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n[yellow]До свидания! 👋[/yellow]")
    except Exception as e:
        print(f"[red]Ошибка: {e}[/red]")

if __name__ == "__main__":
    main()
