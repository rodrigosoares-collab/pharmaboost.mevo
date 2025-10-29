# app/google_search.py (Versão 2.0 - Corrigido com @staticmethod)
import os
import logging
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from typing import List, Dict, Any

class GoogleSearch:
    """
    Uma classe para interagir com a API de Pesquisa Personalizada do Google.
    """
    API_KEY = os.getenv("GOOGLE_API_KEY")
    CSE_ID = os.getenv("GOOGLE_CSE_ID")

    @staticmethod
    def search(queries: List[str]) -> List[Dict[str, Any]]:
        """
        Realiza buscas no Google para uma lista de consultas.

        Args:
            queries: Uma lista de strings de consulta para buscar.

        Returns:
            Uma lista de dicionários, cada um contendo os resultados para uma consulta.
        """
        if not GoogleSearch.API_KEY or not GoogleSearch.CSE_ID:
            logging.error("Chave de API do Google ou ID do CSE não configurados.")
            # Retorna uma estrutura vazia para evitar que a pipeline quebre
            return [{"query": q, "items": [], "related_questions": [], "related_searches": []} for q in queries]

        results = []
        try:
            # AVISO: O cache_discovery=False é importante em ambientes de desenvolvimento
            # para evitar problemas de cache entre reinicializações.
            service = build("customsearch", "v1", developerKey=GoogleSearch.API_KEY, cache_discovery=False)
            for query in queries:
                try:
                    # Usamos 'relatedSite:www.googleapis.com' como um truque para obter 'relatedSearches'
                    # A API oficial não retorna 'People Also Ask' de forma consistente
                    res = service.cse().list(q=query, cx=GoogleSearch.CSE_ID, gl='br', lr='lang_pt').execute()
                    
                    # A API do CSE não retorna 'related_questions' (People Also Ask) de forma confiável.
                    # A estrutura abaixo busca por 'relatedSearches', que é o mais próximo que a API oferece.
                    related_questions_from_search = []
                    related_searches_from_search = []

                    # Tentativa de extrair pesquisas relacionadas que podem servir como perguntas
                    if 'context' in res and 'facets' in res['context']:
                        for facet in res['context']['facets']:
                            if facet.get('anchor') == 'Pesquisas relacionadas':
                                for item in facet.get('buckets', []):
                                    related_searches_from_search.append(item['label'])

                    # Fallback para a chave 'items' se 'context' não estiver disponível
                    if not related_searches_from_search and 'items' in res:
                        for item in res.get('items', []):
                             if 'pagemap' in item and 'metatags' in item['pagemap']:
                                 # Lógica para extrair de metatags se necessário
                                 pass
                    
                    results.append({
                        "query": query,
                        "items": res.get('items', []),
                        "related_questions": related_searches_from_search, # Usamos related_searches como fonte para o FAQ
                        "related_searches": related_searches_from_search
                    })

                except HttpError as e:
                    logging.error(f"Erro na API do Google para a consulta '{query}': {e}")
                    results.append({"query": query, "items": [], "related_questions": [], "related_searches": [], "error": str(e)})
        
        except Exception as e:
            logging.error(f"Erro inesperado ao inicializar o serviço do Google Search: {e}")
            return [{"query": q, "items": [], "related_questions": [], "related_searches": [], "error": "Falha geral no serviço de busca."} for q in queries]
            
        return results