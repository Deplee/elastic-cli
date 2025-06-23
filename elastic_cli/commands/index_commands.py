"""
Команды для работы с индексами Elasticsearch
"""

import json
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.syntax import Syntax
from rich import box
from .base import BaseCommand


class IndexCommands(BaseCommand):
    """Команды для управления индексами"""
    
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
            data = self.cli.make_request("/_cat/indices?format=json&v")
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
                        result = self.cli.make_request(f"/{index_name}", method="DELETE")
                        if result:
                            self.console.print(f"[green]Индекс '{index_name}' удален[/green]")
                
                elif command == "close":
                    result = self.cli.make_request(f"/{index_name}/_close", method="POST")
                    if result:
                        self.console.print(f"[green]Индекс '{index_name}' закрыт[/green]")
                
                elif command == "open":
                    result = self.cli.make_request(f"/{index_name}/_open", method="POST")
                    if result:
                        self.console.print(f"[green]Индекс '{index_name}' открыт[/green]")
                
                elif command == "settings":
                    data = self.cli.make_request(f"/{index_name}/_settings")
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
                                result = self.cli.make_request(f"/{index_name}/_forcemerge?max_num_segments={max_segments}&wait_for_completion=false", method="POST")
                                if result:
                                    self.console.print(f"[green]Forcemerge запущен для индекса '{index_name}' с max_num_segments={max_segments}[/green]")
                            except ValueError:
                                self.console.print("[red]Ошибка: количество сегментов должно быть числом.[/red]")
                    
                    elif merge_type == "expunge":
                        if Confirm.ask(f"Выполнить forcemerge с only_expunge_deletes для индекса '{index_name}'?"):
                            result = self.cli.make_request(f"/{index_name}/_forcemerge?only_expunge_deletes=true&wait_for_completion=false", method="POST")
                            if result:
                                self.console.print(f"[green]Forcemerge запущен для индекса '{index_name}' с only_expunge_deletes=true[/green]")
                    
                    else:
                        self.console.print(f"[red]Неизвестный тип forcemerge: '{merge_type}'[/red]")
                        self.console.print("[yellow]Доступные типы: segments, expunge[/yellow]")
            else:
                # Если не команда, то это имя индекса
                index_name = command
                self._show_index_info(index_name)
    
    def _show_index_info(self, index_name: str):
        """Показывает детальную информацию об одном индексе."""
        with self.console.status(f"Загрузка информации для индекса [bold]{index_name}[/bold]..."):
            index_data = self.cli.make_request(f"/{index_name}")
            index_stats = self.cli.make_request(f"/{index_name}/_stats/docs,store")
            sim_data = self.cli.make_request(f"/_index_template/_simulate_index/{index_name}", method='POST')
        
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
        main_info_table.add_row("Размер", self.cli.format_bytes(stats_store.get('size_in_bytes', 0)))
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
