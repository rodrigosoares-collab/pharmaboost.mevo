# config/settings.py (Versão Corrigida e Completa)
import os
from pathlib import Path
from dotenv import load_dotenv

# Define o caminho para a raiz do projeto (pharmaBoost)
BASE_DIR = Path(__file__).resolve().parent.parent

# Carrega o arquivo .env da raiz do projeto
load_dotenv(BASE_DIR / ".env")

# Chave para a API Gemini
API_KEY = os.getenv("GEMINI_API_KEY")

# Chaves para a API Google Custom Search
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CSE_ID = os.getenv("GOOGLE_CSE_ID")
# --- FIM DA CORREÇÃO ---

# Configurações do Modelo Gemini
DEFAULT_MODEL = "gemini-2.5-flash" 
REQUEST_TIMEOUT = 120

# Caminhos de diretório
PROMPTS_DIR = BASE_DIR / "prompts"
LOGS_DIR = BASE_DIR / "logs"