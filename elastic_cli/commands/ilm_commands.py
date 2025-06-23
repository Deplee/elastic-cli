"""
Команды для работы с ILM политиками Elasticsearch
"""

import json
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich import box
from .base import BaseCommand


class ILMCommands(BaseCommand):
    """Команды для управления ILM политиками"""
    
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

            data = self.cli.make_request("/_ilm/policy")
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
            data = self.cli.make_request(f"/_ilm/policy/{policy_name}")
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
            data = self.cli.make_request(f"/{index_name}/_ilm/explain")
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
