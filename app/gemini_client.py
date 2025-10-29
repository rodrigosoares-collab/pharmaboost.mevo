# app/gemini_client.py (Versão 16.0 - Inicialização Correta da API)
import os
import traceback
from config import settings
from google.api_core import exceptions
import google.generativeai as genai

class GeminiClient:
    """
    Wrapper para interagir com a API Google Gemini usando o SDK mais recente.
    """
    def __init__(self):
        """
        Configura a API Key e inicializa o modelo generativo.
        """
        api_key = settings.API_KEY
        if not api_key:
            raise ValueError("A variável de ambiente GEMINI_API_KEY não foi encontrada. Verifique seu arquivo .env.")
        
        # CORREÇÃO: A forma correta de inicializar é com configure
        genai.configure(api_key=api_key)
        
        # Instancia o modelo que será usado para gerar conteúdo
        self.model = genai.GenerativeModel(settings.DEFAULT_MODEL)

    def execute_prompt(self, prompt_text: str, **kwargs) -> str:
        """
        Envia um prompt para a API Gemini e retorna a resposta de texto.
        """
        try:
            # CORREÇÃO: A chamada agora é feita diretamente no objeto do modelo
            response = self.model.generate_content(prompt_text)
            
            if response and hasattr(response, 'text') and response.text:
                return response.text
            else:
                raise RuntimeError("A API do Gemini retornou uma resposta vazia ou nula.")

        except exceptions.GoogleAPICallError as e:
            print(f"Erro na API Gemini detectado no cliente: {e.message}")
            raise e
        except Exception as e:
            print(f"Erro inesperado no cliente Gemini: {e}")
            traceback.print_exc()
            raise RuntimeError(f"Ocorreu um erro inesperado no cliente: {str(e)}") from e