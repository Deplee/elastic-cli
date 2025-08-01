import os
import yaml
from typing import Dict, Optional
from rich.console import Console
from rich.markup import escape


class ConfigManager:
    
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.console = Console()
        self.contexts = {}
        self.current_context_name = None
        
        os.makedirs(os.path.dirname(self.config_file), exist_ok=True)
    
    def load_config(self) -> None:
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = yaml.safe_load(f) or {}
                
                self.contexts = config.get('contexts', {})
                self.current_context_name = config.get('current_context')

            except Exception as e:
                self.console.print(f"[red]Ошибка загрузки конфигурации: {escape(str(e))}[/red]")
    
    def save_config(self) -> None:
        config = {
            'current_context': self.current_context_name,
            'contexts': self.contexts,
        }
        try:
            with open(self.config_file, 'w') as f:
                yaml.dump(config, f, default_flow_style=False)
        except Exception as e:
            self.console.print(f"[red]Ошибка сохранения конфигурации: {escape(str(e))}[/red]")
    
    def get_context(self, context_name: str) -> Optional[Dict]:
        return self.contexts.get(context_name)
    
    def add_context(self, context_name: str, context_data: Dict) -> None:
        self.contexts[context_name] = context_data
        self.save_config()
    
    def remove_context(self, context_name: str) -> None:
        if context_name in self.contexts:
            del self.contexts[context_name]
            if self.current_context_name == context_name:
                self.current_context_name = None
            self.save_config()
    
    def set_current_context(self, context_name: str) -> None:
        self.current_context_name = context_name
        self.save_config()
    
    def get_current_context(self) -> Optional[str]:
        return self.current_context_name
