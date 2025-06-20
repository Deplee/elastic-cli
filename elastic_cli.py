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
- indices <команда> <имя_индекса>: выполнить команду (delete, open, close, settings)
"""
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
            
            if command in ("delete", "close", "open", "settings"):
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
            else:
                # Если не команда, то это имя индекса
                index_name = command
                self._show_index_info(index_name)
    
    def do_shards(self, arg):
        """Показать информацию о шардах"""
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
                self.console.print(f"[red]Неизвестная команда для 'ilm': '{command}'.[/red]")
                self.console.print("[yellow]Доступные команды: list, show, explain.[/yellow]")

    def do_templates(self, arg):
        """Управление шаблонами индексов.
Использование:
- templates list: Показать все шаблоны индексов.
- templates show <template_name>: Показать JSON определение конкретного шаблона.
"""
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
