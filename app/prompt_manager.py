import os
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

class PromptManager:
    """
    Carrega, gerencia e renderiza todos os prompts da pasta de prompts.
    """
    def __init__(self, prompt_dir="prompts"):
        """
        Inicializa o gerenciador, carregando todos os prompts .yaml do diretório.

        Args:
            prompt_dir (str): O caminho para o diretório que contém os arquivos .yaml dos prompts.
        """
        # Define o caminho para o diretório de prompts de forma robusta
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.prompt_dir = os.path.join(base_dir, prompt_dir)
        
        # Configura o ambiente Jinja2 para carregar templates
        self.env = Environment(
            loader=FileSystemLoader(self.prompt_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        self.prompts = self._load_prompts()
        print(f"PromptManager inicializado. Prompts carregados: {list(self.prompts.keys())}")

    def _load_prompts(self) -> dict:
        """
        Carrega TODOS os arquivos .yaml do diretório de prompts dinamicamente.
        """
        loaded_prompts = {}
        if not os.path.isdir(self.prompt_dir):
            print(f"Aviso: Diretório de prompts '{self.prompt_dir}' não encontrado.")
            return loaded_prompts

        for filename in os.listdir(self.prompt_dir):
            if filename.endswith((".yaml", ".yml")):
                prompt_name = os.path.splitext(filename)[0]
                filepath = os.path.join(self.prompt_dir, filename)
                try:
                    with open(filepath, "r", encoding="utf-8") as f:
                        loaded_prompts[prompt_name] = yaml.safe_load(f)
                except Exception as e:
                    print(f"Erro ao carregar o prompt '{filename}': {e}")
        return loaded_prompts

    def render(self, prompt_name: str, **kwargs) -> str:
        """
        Renderiza um prompt específico com os dados fornecidos.

        Args:
            prompt_name: O nome do prompt a ser renderizado (sem a extensão .yaml).
            **kwargs: As variáveis a serem passadas para o template do prompt.

        Returns:
            O prompt renderizado como uma string.
        
        Raises:
            ValueError: Se o prompt solicitado não for encontrado.
        """
        if prompt_name not in self.prompts:
            raise ValueError(f"Prompt '{prompt_name}' não encontrado. Prompts disponíveis: {list(self.prompts.keys())}")
        
        prompt_data = self.prompts[prompt_name]
        if not isinstance(prompt_data, dict) or 'template' not in prompt_data:
             raise ValueError(f"O arquivo de prompt '{prompt_name}.yaml' é inválido ou não contém uma chave 'template'.")

        template_str = prompt_data['template']
        template = self.env.from_string(template_str)
        return template.render(**kwargs)