"""
Утилиты для Elasticsearch CLI
"""

import math
from typing import Union


def format_bytes(size: Union[int, float], decimals: int = 2) -> str:
    """
    Форматирует размер в байтах в человекочитаемый вид
    
    Args:
        size: Размер в байтах
        decimals: Количество знаков после запятой
        
    Returns:
        Отформатированная строка с размером
    """
    if not isinstance(size, (int, float)) or size == 0:
        return "0 Bytes"
    
    k = 1024
    dm = decimals if decimals >= 0 else 0
    sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    i = math.floor(math.log(size) / math.log(k)) if size > 0 else 0
    return f"{round(size / math.pow(k, i), dm)} {sizes[i]}"


def truncate_text(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """
    Обрезает текст до указанной длины
    
    Args:
        text: Исходный текст
        max_length: Максимальная длина
        suffix: Суффикс для обрезанного текста
        
    Returns:
        Обрезанный текст
    """
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix
