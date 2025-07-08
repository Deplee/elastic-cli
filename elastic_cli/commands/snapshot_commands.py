import json
from rich.table import Table
from rich.panel import Panel
from rich import box
from .base import BaseCommand


class SnapshotCommands(BaseCommand):
    
    def do_snapshots(self, arg):
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
            data = self.cli.make_request("/_snapshot")
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
                    data = self.cli.make_request(f"/_snapshot/{repo}/_all")
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
