from pathlib import Path


def load_prompt(name: str) -> str:
    """
    读取prompt内容
    :param name: 文件名，不包含后缀
    :return: 内容
    """
    prompt_path = Path(__file__).parents[2] / "prompts" / f"{name}.prompt"
    return prompt_path.read_text(encoding="utf-8")
