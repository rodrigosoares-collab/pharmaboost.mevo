# app/use_cases.py (Versão Final Corrigida)
import json
from typing import Dict, Any, AsyncGenerator, Optional
import asyncio
import traceback
import time
import re
import logging
from bs4 import BeautifulSoup
from google.api_core.exceptions import ResourceExhausted, ServiceUnavailable, DeadlineExceeded
from google.generativeai.types import HarmCategory, HarmBlockThreshold

from .pharma_seo_optimizer import SeoOptimizerAgent
from .prompt_manager import PromptManager
from .gemini_client import GeminiClient
from .strategy_manager import StrategyManager

prompt_manager = PromptManager()
gemini_client = GeminiClient()
strategy_manager = StrategyManager()

# --- Funções Utilitárias ---
def _extract_json_from_string(text: str) -> Optional[Dict[str, Any]]:
    """Extrai um objeto JSON de uma string, mesmo que esteja dentro de blocos de código markdown."""
    if not text:
        return None
    match = re.search(r'```json\s*(\{[\s\S]*?\})\s*```', text, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        start_index = text.find('{')
        end_index = text.rfind('}')
        if start_index != -1 and end_index != -1 and end_index > start_index:
            json_str = text[start_index:end_index + 1]
        else:
            return None
    try:
        json_str_cleaned = "".join(char for char in json_str if 31 < ord(char) or char in "\n\t\r")
        return json.loads(json_str_cleaned)
    except json.JSONDecodeError as e:
        logging.error(f"Falha ao decodificar JSON: {e}")
        return None

def _execute_prompt_with_backoff(prompt: str, max_retries: int = 5, timeout: int = 120) -> Optional[str]:
    """Executa um prompt contra a API Gemini com retentativas exponenciais."""
    wait_time = 2
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }
    for attempt in range(max_retries):
        try:
            logging.info(f"Enviando prompt (tentativa {attempt + 1}/{max_retries})...")
            response = gemini_client.model.generate_content(
                prompt, safety_settings=safety_settings, request_options={'timeout': timeout}
            )
            if response and hasattr(response, 'text') and response.text:
                return response.text
            raise ServiceUnavailable("Resposta vazia da API.")
        except (DeadlineExceeded, ResourceExhausted, ServiceUnavailable) as e:
            logging.warning(f"Tentativa {attempt + 1} falhou ({type(e).__name__}). Aguardando {wait_time}s...")
            time.sleep(wait_time)
            wait_time = min(wait_time * 2, 60)
        except Exception as e:
            logging.error(f"Erro inesperado na chamada da API: {e}")
            return None
    logging.error("Limite máximo de tentativas atingido.")
    return None

# --- Agentes de IA Especializados ---
def _run_sensitive_term_identifier_agent(bula_text: str) -> list:
    try:
        prompt = prompt_manager.render("identificador_termos_sensitivos", bula_text=bula_text)
        response_raw = _execute_prompt_with_backoff(prompt, timeout=90)
        if response_raw:
            json_data = _extract_json_from_string(response_raw)
            if json_data and "termos_proibidos" in json_data:
                return json_data["termos_proibidos"]
    except Exception as e:
        logging.error(f"Erro no Agente Identificador de Termos: {e}", exc_info=True)
    return []

def _run_master_generator_agent(product_name: str, product_info: dict) -> Optional[Dict[str, Any]]:
    prompt = prompt_manager.render("medicamento_generator", product_name=product_name, product_info=product_info)
    response_raw = _execute_prompt_with_backoff(prompt, timeout=180)
    return _extract_json_from_string(response_raw) if response_raw else None

def _run_seo_auditor_agent(full_page_json: dict) -> Dict[str, Any]:
    prompt = prompt_manager.render("auditor_seo_tecnico", full_page_json=json.dumps(full_page_json, ensure_ascii=False))
    response_raw = _execute_prompt_with_backoff(prompt, timeout=180)
    return _extract_json_from_string(response_raw) or {"total_score": 0, "feedback_geral": "Falha na auditoria."}

def _run_refiner_agent(product_name: str, product_info: dict, previous_json: dict, feedback_data: dict) -> Dict[str, Any]:
    prompt = prompt_manager.render(
        "refinador_qualidade",
        product_name=product_name,
        previous_json=json.dumps(previous_json, ensure_ascii=False),
        analise_automatica_anterior=json.dumps(feedback_data, ensure_ascii=False),
        product_info=product_info
    )
    response_raw = _execute_prompt_with_backoff(prompt, timeout=180)
    return _extract_json_from_string(response_raw) or previous_json

def _run_beauty_generator_agent(product_name: str, product_info: dict) -> Optional[Dict[str, Any]]:
    brand = product_info.get("brand")
    try:
        faq_research = SeoOptimizerAgent.search_people_also_ask(product_name, brand=brand)
        time.sleep(1)
        keyword_research = SeoOptimizerAgent.search_related_topics(product_name, brand=brand)
    except Exception as e:
        logging.error(f"Erro na busca SEO para '{product_name}': {e}")
        faq_research, keyword_research = "", ""
    product_info.update({
        'faq_research_context': faq_research,
        'keyword_research_context': keyword_research
    })
    prompt = prompt_manager.render("beleza_e_cuidado_generator", product_name=product_name, product_info=product_info)
    response_raw = _execute_prompt_with_backoff(prompt, timeout=120)
    return _extract_json_from_string(response_raw)

def _run_beauty_auditor_agent(full_page_json: dict) -> Dict[str, Any]:
    prompt = prompt_manager.render("auditor_beleza_e_cuidado", full_page_json=json.dumps(full_page_json, ensure_ascii=False))
    response_raw = _execute_prompt_with_backoff(prompt, timeout=180)
    return _extract_json_from_string(response_raw) or {"total_score": 0, "feedback_geral": "Falha na auditoria de beleza."}

def _run_beauty_refiner_agent(product_name: str, product_info: dict, previous_json: dict, feedback_data: dict) -> Dict[str, Any]:
    prompt = prompt_manager.render(
        "refinador_beleza_e_cuidado",
        product_name=product_name,
        previous_json=json.dumps(previous_json, ensure_ascii=False),
        feedback_usuario=feedback_data.get("feedback_usuario"),
        analise_automatica_anterior=json.dumps(feedback_data.get("analise_automatica_anterior"), ensure_ascii=False),
        product_info=product_info
    )
    response_raw = _execute_prompt_with_backoff(prompt, timeout=120)
    return _extract_json_from_string(response_raw) or previous_json

# --- Orquestrador de Pipeline ---
async def run_seo_pipeline_stream(
    product_type: str,
    product_name: str,
    product_info: Dict[str, Any],
    previous_content: Optional[Dict[str, Any]] = None,
    feedback_text: Optional[str] = None
) -> AsyncGenerator[str, None]:
    MIN_SCORE_TARGET = 95
    MAX_ATTEMPTS = 2

    async def _send_event(event_type: str, data: dict) -> str:
        await asyncio.sleep(0.05)
        return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

    try:
        brand = product_info.get("brand")
        nome_base = SeoOptimizerAgent._get_base_product_name(product_name, brand=brand)
        product_info["nome_base"] = nome_base

        if product_type == 'medicine':
            generator_agent, auditor_agent, refiner_agent = _run_master_generator_agent, _run_seo_auditor_agent, _run_refiner_agent
            bula_text = product_info.get("bula_text", "")
            if not bula_text: raise ValueError("Texto da bula não encontrado para pipeline de medicamento.")
            yield await _send_event("log", {"message": "Analisando bula para identificar termos sensíveis...", "type": "info"})
            dynamic_blacklist = await asyncio.to_thread(_run_sensitive_term_identifier_agent, bula_text)
            product_info["dynamic_blacklist"] = dynamic_blacklist
        elif product_type == 'beauty':
            generator_agent, auditor_agent, refiner_agent = _run_beauty_generator_agent, _run_beauty_auditor_agent, _run_beauty_refiner_agent
        else:
            raise ValueError(f"Tipo de produto '{product_type}' desconhecido.")

        best_attempt_content, highest_score = None, -1

        for attempt in range(1, MAX_ATTEMPTS + 1):
            yield await _send_event("log", {"message": f"<b>--- Ciclo de Qualidade {attempt}/{MAX_ATTEMPTS} ---</b>", "type": "info"})
            
            if attempt == 1:
                current_content_data = await asyncio.to_thread(generator_agent, product_name, product_info)
            else:
                current_content_data = await asyncio.to_thread(refiner_agent, product_name, product_info, best_attempt_content, audit_results)

            if not current_content_data:
                yield await _send_event("log", {"message": "❌ Falha na geração de conteúdo.", "type": "error"})
                break

            audit_results = await asyncio.to_thread(auditor_agent, current_content_data)
            final_score = audit_results.get("total_score", 0)
            yield await _send_event("log", {"message": f"<b>Score da Tentativa {attempt}: {final_score}/100</b>", "type": "info"})

            if final_score > highest_score:
                highest_score, best_attempt_content = final_score, current_content_data

            if final_score >= MIN_SCORE_TARGET:
                break
        
        if not best_attempt_content:
            raise RuntimeError("Falha crítica: Nenhum conteúdo pôde ser gerado.")

        final_html_vtex_safe = await asyncio.to_thread(SeoOptimizerAgent._finalize_for_vtex, best_attempt_content.get("html_content", ""), product_name)
        
        # --- CORREÇÃO APLICADA ---
        # Removida a chave 'product_type', pois não é mais necessária no frontend.
        final_data_for_review = {
            "final_score": highest_score,
            "final_content": final_html_vtex_safe,
            "seo_title": str(best_attempt_content.get("seo_title", product_name)),
            "meta_description": str(best_attempt_content.get("meta_description", "")),
            "raw_json_content": best_attempt_content
        }
        # --- FIM DA CORREÇÃO ---
        
        yield await _send_event("done", final_data_for_review)

    except Exception as e:
        logging.error(f"Erro fatal na pipeline para '{product_name}': {e}", exc_info=True)
        yield await _send_event("error", {"message": f"Erro crítico na pipeline: {str(e)}", "type": "error"})