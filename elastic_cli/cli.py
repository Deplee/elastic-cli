#!/usr/bin/env python3
import os
import sys
from dotenv import load_dotenv

from rich.console import Console
from rich.panel import Panel

import cmd2

load_dotenv()

from .config import ConfigManager
from .connection import ElasticsearchConnection
from .commands import (
    ClusterCommands,
    IndexCommands,
    ILMCommands,
    TemplateCommands,
    SnapshotCommands
)


class ElasticsearchCLI(cmd2.Cmd):
    
    def __init__(self):
        super().__init__()
        self.console = Console()
        self.prompt = "elastic-cli> "
        self.config_file = os.path.expanduser("~/.elastic-cli/config.yml")
        

        self.config_manager = ConfigManager(self.config_file)
        self.connection = ElasticsearchConnection()
        
        self.cluster_commands = ClusterCommands(self)
        self.index_commands = IndexCommands(self)
        self.ilm_commands = ILMCommands(self)
        self.template_commands = TemplateCommands(self)
        self.snapshot_commands = SnapshotCommands(self)
        
        self.config_manager.load_config()
        
        current_context = self.config_manager.get_current_context()
        if current_context:
            self._switch_context(current_context)
    
    def format_bytes(self, size, decimals=2):
        if not isinstance(size, (int, float)) or size == 0:
            return "0 Bytes"
        import math
        k = 1024
        dm = decimals if decimals >= 0 else 0
        sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        i = math.floor(math.log(size) / math.log(k)) if size > 0 else 0
        return f"{round(size / math.pow(k, i), dm)} {sizes[i]}"
    
    def make_request(self, endpoint: str, method: str = 'GET', data: dict = None):
        return self.connection.make_request(endpoint, method, data)
    
    def _update_prompt(self):
        current_context = self.config_manager.get_current_context()
        if current_context:
            self.prompt = f"({current_context}) elastic-cli> "
        else:
            self.prompt = "(no context) elastic-cli> "

    def _switch_context(self, context_name: str) -> bool:
        context = self.config_manager.get_context(context_name)
        if not context:
            self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
            return False

        url = context.get('url')
        username = context.get('username')
        password = context.get('password')

        self.connection.set_connection(url, username, password)

        if self.connection.check_connection():
            self.config_manager.set_current_context(context_name)
            self._update_prompt()
            self.console.print(f"[green]Переключились на контекст: [bold]{context_name}[/bold] ({url})[/green]")
            return True
        else:
            self.console.print(f"[red]Не удалось подключиться к кластеру в контексте '{context_name}'.[/red]")
            self.connection.clear_connection()
            self.config_manager.set_current_context(None)
            self._update_prompt()
            return False

    def preloop(self):
        intro_text = """
[bold blue]╔══════════════════════════════════════════════════════════════╗[/bold blue]
[bold blue]║                    [white]Elasticsearch CLI[/white]                         ║[/bold blue]
[bold blue]║              [white]Интерактивное управление кластером[/white]              ║[/bold blue]
[bold blue]╚══════════════════════════════════════════════════════════════╝[/bold blue]

[dim]Подключитесь к кластеру с помощью команды [bold white]connect[/bold white][/dim]
[dim]Введите [bold white]help[/bold white] для получения списка команд[/dim]
"""
        self.console.print(Panel(intro_text, title="Добро пожаловать", border_style="blue", expand=False))
    

    
    def do_connect(self, arg):
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
        if self.config_manager.get_context(context_name):
            if not cmd2.Cmd2ArgumentParser().confirm(f"[yellow]Контекст '{context_name}' уже существует. Перезаписать?[/yellow]"):
                return
        
        from rich.prompt import Prompt
        url = Prompt.ask("URL Elasticsearch", default="http://localhost:9200")
        username = Prompt.ask("Имя пользователя (или Enter для пропуска)")
        password = Prompt.ask("Пароль", password=True) if username else ""
        
        self.connection.set_connection(url, username, password)

        with self.console.status("Проверка подключения..."):
            if self.connection.check_connection():
                context_data = {
                    'url': url,
                    'username': username,
                    'password': password
                }
                self.config_manager.add_context(context_name, context_data)
                self.console.print(f"[green]Контекст '{context_name}' сохранен.[/green]")
                self._switch_context(context_name)
            else:
                self.console.print("[red]Не удалось подключиться к кластеру. Контекст не сохранен.[/red]")

    def do_context(self, arg):
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
            from rich.table import Table
            from rich import box
            
            table = Table(title="🗂️ Контексты подключений", box=box.ROUNDED)
            table.add_column("Активный", style="green")
            table.add_column("Имя", style="cyan")
            table.add_column("URL", style="magenta")
            table.add_column("Пользователь", style="yellow")
            
            current_context = self.config_manager.get_current_context()
            for name, details in self.config_manager.contexts.items():
                is_active = "✅" if name == current_context else ""
                table.add_row(is_active, name, details.get('url'), details.get('username', 'N/A'))
            self.console.print(table)
        
        elif command == 'use':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для переключения.[/red]")
                return
            context_name = parts[1]
            self._switch_context(context_name)

        elif command == 'delete':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для удаления.[/red]")
                return
            context_name = parts[1]
            if not self.config_manager.get_context(context_name):
                self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
                return
            
            if cmd2.Cmd2ArgumentParser().confirm(f"Вы уверены, что хотите удалить контекст '{context_name}'?"):
                self.config_manager.remove_context(context_name)
                self.console.print(f"[green]Контекст '{context_name}' удален.[/green]")

        elif command == 'show':
            if len(parts) < 2:
                self.console.print("[red]Необходимо указать имя контекста для просмотра.[/red]")
                return
            context_name = parts[1]
            details = self.config_manager.get_context(context_name)
            if not details:
                self.console.print(f"[red]Контекст '{context_name}' не найден.[/red]")
                return
            
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
        self.cluster_commands.do_health(arg)
    
    def do_nodes(self, arg):
        self.cluster_commands.do_nodes(arg)
    
    def do_indices(self, arg):
        self.index_commands.do_indices(arg)
    
    def do_shards(self, arg):
        self.cluster_commands.do_shards(arg)
    
    def do_tasks(self, arg):
        self.cluster_commands.do_tasks(arg)
    
    def do_snapshots(self, arg):
        self.snapshot_commands.do_snapshots(arg)
    
    def do_settings(self, arg):
        self.cluster_commands.do_settings(arg)
    
    def do_ilm(self, arg):
        self.ilm_commands.do_ilm(arg)
    
    def do_templates(self, arg):
        self.template_commands.do_templates(arg)
    
    def do_quit(self, arg):
        self.console.print("[yellow]До свидания! 👋[/yellow]")
        return True
    
    def do_exit(self, arg):
        return self.do_quit(arg)
    
    def do_EOF(self, arg):
        return self.do_quit(arg)


def main():
    try:
        cli = ElasticsearchCLI()
        cli.cmdloop()
    except KeyboardInterrupt:
        print("\n[yellow]До свидания! 👋[/yellow]")
    except Exception as e:
        print(f"[red]Ошибка: {e}[/red]")


if __name__ == "__main__":
    main()
