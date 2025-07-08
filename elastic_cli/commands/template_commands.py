import json
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax
from rich import box
from .base import BaseCommand


class TemplateCommands(BaseCommand):
    
    def do_templates(self, arg):
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

            data = self.cli.make_request("/_index_template")
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
            data = self.cli.make_request(f"/_index_template/{template_name}")
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
