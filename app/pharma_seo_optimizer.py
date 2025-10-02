# app/pharma_seo_optimizer.py (Versão Corrigida com Controlo de Concorrência)
import logging
from typing import List, Optional
from bs4 import BeautifulSoup
import re
from .google_search import GoogleSearch
import threading

# Ele garantirá que no máximo 5 threads acedam ao bloco de código protegido em simultâneo.
google_search_limiter = threading.Semaphore(5)

class SeoOptimizerAgent:
    """
    Um agente focado em otimizações de SEO, incluindo busca de palavras-chave,
    análise de perguntas frequentes e formatação final de conteúdo.
    """

    @staticmethod
    def _get_base_product_name(product_name_full: str, brand: Optional[str] = None) -> str:
        # ... (código existente sem alterações) ...
        logging.info(f"Extraindo nome base de '{product_name_full}' com a marca '{brand}'...")
        try:
            base_name = product_name_full
            # Padrões para remover dos nomes de produtos
            patterns = [
                r'\s+\d+(\.\d+)?(mg|g|ml|l)(\s*\/\s*\d+(\.\d+)?(mg|g|ml|l))?', r'\s+-\s+Caixa.*',
                r'\s+com\s+\d+\s+.*', r'\s+\d+\s+Comprimidos.*', r'\s+\d+\s+Seringas.*',
                r'\s+Gotas.*', r'\s+Xarope.*', r'\s+\(Refil\).*', r'\s+FPS\s*\d+.*',
                r'LH-01', r'500ML'
            ]
            for pattern in patterns:
                base_name = re.sub(pattern, '', base_name, flags=re.IGNORECASE)
            
            base_name = base_name.strip()

            # Aprimoramento: Anexa a marca se ela for fornecida e não estiver contida no nome
            if brand and brand.lower() not in base_name.lower():
                base_name = f"{base_name} {brand}"
            
            if base_name and len(base_name) > 3:
                logging.info(f"Nome base otimizado para busca: '{base_name}'")
                return base_name
        except Exception as e:
            logging.error(f"Erro ao extrair nome base para '{product_name_full}': {e}")
        
        return product_name_full

    @staticmethod
    def search_people_also_ask(query: str, brand: Optional[str] = None) -> str:
        """
        Simplifica a consulta, realiza uma busca no Google e extrai as perguntas.
        """
        simplified_query = SeoOptimizerAgent._get_base_product_name(query, brand=brand)
        logging.info(f"Executando busca 'People Also Ask' para a consulta simplificada: '{simplified_query}'")
        try:
            # --- INÍCIO DA CORREÇÃO ---
            # O bloco 'with' garante que a thread espera se 5 outras threads já estiverem
            # a fazer buscas. Assim que uma termina, esta pode prosseguir.
            with google_search_limiter:
                logging.info(f"Semáforo adquirido para 'People Also Ask': {simplified_query}")
                search_results = GoogleSearch.search(queries=[f"perguntas sobre {simplified_query}"])
            # --- FIM DA CORREÇÃO ---
            
            if not search_results or not search_results[0].get('related_questions'):
                logging.warning(f"Nenhuma pergunta 'People Also Ask' encontrada para '{simplified_query}'.")
                return "Nenhuma pergunta frequente relevante encontrada na pesquisa."

            questions = [q for q in search_results[0]['related_questions'] if q]
            
            if not questions:
                return "Nenhuma pergunta relevante encontrada."

            return "\n".join(f"- {q}" for q in questions)
        except Exception as e:
            logging.error(f"Erro ao buscar 'People Also Ask' para '{simplified_query}': {e}")
            return f"Erro ao buscar perguntas: {e}"

    @staticmethod
    def search_related_topics(query: str, brand: Optional[str] = None) -> str:
        """
        Simplifica a consulta, realiza uma busca no Google e extrai os tópicos relacionados.
        """
        simplified_query = SeoOptimizerAgent._get_base_product_name(query, brand=brand)
        logging.info(f"Executando busca de 'Tópicos Relacionados' para a consulta simplificada: '{simplified_query}'")
        try:
            # --- INÍCIO DA CORREÇÃO ---
            with google_search_limiter:
                logging.info(f"Semáforo adquirido para 'Tópicos Relacionados': {simplified_query}")
                search_results = GoogleSearch.search(queries=[f"tópicos sobre {simplified_query}"])
            # --- FIM DA CORREÇÃO ---

            if not search_results or not search_results[0].get('related_searches'):
                logging.warning(f"Nenhuma pesquisa relacionada encontrada para '{simplified_query}'.")
                return "Nenhuma palavra-chave relacionada encontrada na pesquisa."
            
            topics = [s for s in search_results[0]['related_searches'] if s]

            if not topics:
                return "Nenhum tópico relevante encontrado."

            return ", ".join(topics)
        except Exception as e:
            logging.error(f"Erro ao buscar 'Tópicos Relacionados' para '{simplified_query}': {e}")
            return f"Erro ao buscar tópicos: {e}"

    @staticmethod
    def _finalize_for_vtex(html_content: str, product_name: str) -> str:
        # ... (código existente sem alterações) ...
        if not html_content or not isinstance(html_content, str):
            logging.warning("Conteúdo HTML para finalização está vazio ou inválido.")
            return f"<p>Conteúdo para {product_name} não pôde ser gerado.</p>"
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            for p in soup.find_all('p'):
                if not p.get_text(strip=True):
                    p.decompose()
            for ul in soup.find_all(['ul', 'ol']):
                if not ul.find('li'):
                    ul.decompose()
            final_tag = soup.new_tag('p')
            soup.append(final_tag)
            return str(soup)
        except Exception as e:
            logging.error(f"Erro ao finalizar HTML para VTEX: {e}")
            return html_content