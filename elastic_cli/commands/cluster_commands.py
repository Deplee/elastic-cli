import json
from typing import Dict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich import box

from .base import BaseCommand


class ClusterCommands(BaseCommand):
    
    def do_health(self, arg):
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

        data = self.cli.make_request("/_cluster/health")
        if not data:
            return
        

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

        data = self.cli.make_request("/_nodes/stats")
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
            
            fs_stats = node_data.get('fs', {})
            total_disk = fs_stats.get('total', {}).get('total_in_bytes', 0)
            free_disk = fs_stats.get('total', {}).get('free_in_bytes', 0)
            disk_percent = ((total_disk - free_disk) / total_disk * 100) if total_disk > 0 else 0
            
            roles = node_data.get('roles', [])
            
            table.add_row(
                node_data.get('name', 'N/A'),
                node_id[:8] + '...',
                ', '.join(roles),
                f"{cpu_percent:.1f}%",
                f"{mem_percent:.1f}%",
                f"{disk_percent:.1f}%"
            )
        
        self.console.print(table)
    
    def do_shards(self, arg):
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

        data = self.cli.make_request("/_cat/shards?format=json&v")
        if not data:
            return
        
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

        data = self.cli.make_request("/_tasks")
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
                description = task_data.get('description', 'N/A')
                if len(description) > 50:
                    description = description[:50] + '...'
                
                table.add_row(
                    node_id[:8] + '...',
                    task_id,
                    task_data.get('type', 'N/A'),
                    task_data.get('action', 'N/A'),
                    description
                )
        
        self.console.print(table)
    
    def do_settings(self, arg):
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

        data = self.cli.make_request("/_cluster/settings")
        if not data:
            return
        
        self.console.print(Panel(
            json.dumps(data, indent=2, ensure_ascii=False),
            title="⚙️ Настройки кластера",
            border_style="blue"
        ))
