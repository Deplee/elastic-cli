from abc import ABC, abstractmethod
from typing import Optional, Dict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.syntax import Syntax
from rich import box
import json


class BaseCommand(ABC):
    
    def __init__(self, cli):
        self.cli = cli
        self.console = cli.console
    
    def show_help(self, help_text: str, title: str) -> None:
        self.console.print(Panel(help_text, title=title, border_style="blue", expand=False))
    
    def show_json_panel(self, data: Dict, title: str) -> None:
        json_str = json.dumps(data, indent=2, ensure_ascii=False)
        self.console.print(Panel(
            Syntax(json_str, "json", theme="monokai", line_numbers=True),
            title=title,
            border_style="blue"
        ))
    
    def create_table(self, title: str, columns: list) -> Table:
        table = Table(title=title, box=box.ROUNDED)
        for col_name, style in columns:
            table.add_column(col_name, style=style)
        return table
    
    def format_bytes(self, size: int) -> str:
        return self.cli.format_bytes(size)
    
    def truncate_text(self, text: str, max_length: int = 50) -> str:
        if len(text) > max_length:
            return text[:max_length] + '...'
        return text
