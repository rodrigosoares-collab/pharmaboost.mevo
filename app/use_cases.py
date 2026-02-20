# app/use_cases.py
import os
import json
import html  
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

# =====================================================================
# --- Arquivo de Memória Contínua (Machine Learning Baseado em Contexto) ---
# =====================================================================
MEMORY_FILE = "merchant_success_memory.json"

def _load_memory() -> list:
    """Carrega o histórico de produtos que foram aprovados para a IA aprender com eles."""
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, 'r', encoding='utf-8') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return []
    return []

def _save_success_to_memory(product_name: str, original_html: str, approved_html: str):
    """
    Salva um caso de sucesso na memória. 
    Mantém apenas os últimos 3 sucessos para não estourar o limite de tokens do prompt,
    e trunca o texto para focar apenas na transformação semântica.
    """
    memory = _load_memory()
    
    # Evita salvar duplicatas do mesmo produto para manter a diversidade da memória
    if any(item.get('product') == product_name for item in memory):
        return
        
    novo_sucesso = {
        "product": product_name,
        # Salva um trecho representativo para a IA entender a "pegada" da aprovação
        "original_text_snippet": str(original_html)[:800] + "...", 
        "approved_text_snippet": str(approved_html)[:800] + "..."
    }
    
    memory.append(novo_sucesso)
    
    # Mantém apenas os 3 últimos sucessos para focar no aprendizado recente
    if len(memory) > 3:
        memory = memory[-3:]
        
    with open(MEMORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(memory, f, ensure_ascii=False, indent=4)
        
def _format_memory_for_prompt() -> str:
    """Formata a memória em texto plano para ser injetada no prompt da IA."""
    memory = _load_memory()
    if not memory:
        return "Nenhum histórico recente disponível. Siga as regras base rigorosamente."
    
    formatted = "### EXEMPLOS RECENTES DE SUCESSO (APRENDA COM ELES E REPLIQUE A ABORDAGEM):\n"
    formatted += "Note como os termos médicos originais foram camuflados no texto aprovado:\n\n"
    
    for item in memory:
        formatted += f"- Produto: {item['product']}\n"
        formatted += f"  Trecho Original: {item['original_text_snippet']}\n"
        formatted += f"  Como ficou Aprovado (Seguro): {item['approved_text_snippet']}\n\n"
        
    return formatted

# =====================================================================
# --- Funções Utilitárias ---
# =====================================================================

def _force_clean_html(text: str) -> str:
    """
    Purifica o HTML de forma robusta e nativa (sem gastar API com IA).
    Remove blocos Markdown indesejados e faz o unescape em loop para garantir 
    que casos de 'Double Escaping' (ex: &amp;lt;div&gt;) sejam totalmente decodificados.
    """
    if not text:
        return ""
        
    # 1. Remove qualquer markdown residual da IA (ex: ```html ... ```)
    text = re.sub(r'^```html\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^```\s*', '', text)
    text = re.sub(r'```$', '', text)
    
    # 2. Loop de Desescapar: continua decodificando até o texto estabilizar
    prev_text = None
    while text != prev_text:
        prev_text = text
        text = html.unescape(text)
        
    return text.strip()


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
        # Limpa caracteres de controle invisíveis que podem quebrar o JSON
        json_str_cleaned = "".join(char for char in json_str if 31 < ord(char) or char in "\n\t\r")
        return json.loads(json_str_cleaned)
    except json.JSONDecodeError as e:
        logging.error(f"Falha ao decodificar JSON: {e}")
        return None

def _execute_prompt_with_backoff(prompt: str, max_retries: int = 5, timeout: int = 120) -> Optional[str]:
    """Executa um prompt contra a API Gemini com retentativas exponenciais."""
    wait_time = 2
    # Desativa bloqueios de segurança do Google para evitar falsos positivos com bulas de remédio
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

# =====================================================================
# --- Agentes de IA Especializados (Para Pipeline Normal) ---
# =====================================================================
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
    prompt = prompt_manager.render("medicamento_generator", product_name=product_name, **product_info)
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
        **product_info
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

# =====================================================================
# --- Orquestrador de Pipeline (SEO Normal) ---
# =====================================================================
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
                yield await _send_event("log", {"message": f"❌ Falha na geração ou decodificação do conteúdo na tentativa {attempt}. A resposta da IA pode estar malformada.", "type": "warning"})
                continue

            audit_results = await asyncio.to_thread(auditor_agent, current_content_data)
            final_score = audit_results.get("total_score", 0)
            yield await _send_event("log", {"message": f"<b>Score da Tentativa {attempt}: {final_score}/100</b>", "type": "info"})

            if final_score > highest_score:
                highest_score, best_attempt_content = final_score, current_content_data

            if final_score >= MIN_SCORE_TARGET:
                break
        
        if not best_attempt_content:
            yield await _send_event("log", {"message": f"❌ <b>Processamento falhou para '{product_name}'.</b> Não foi possível gerar conteúdo válido após {MAX_ATTEMPTS} tentativas.", "type": "error"})
            return 

        final_html_vtex_safe = await asyncio.to_thread(SeoOptimizerAgent._finalize_for_vtex, best_attempt_content.get("html_content", ""), product_name)
        
        final_data_for_review = {
            "final_score": highest_score,
            "final_content": final_html_vtex_safe,
            "seo_title": str(best_attempt_content.get("seo_title", product_name)),
            "meta_description": str(best_attempt_content.get("meta_description", "")),
            "raw_json_content": best_attempt_content
        }
        
        yield await _send_event("done", final_data_for_review)

    except Exception as e:
        logging.error(f"Erro fatal na pipeline para '{product_name}': {e}", exc_info=True)
        yield await _send_event("error", {"message": f"Erro crítico na pipeline: {str(e)}", "type": "error"})

# =====================================================================
# --- Pipeline de Recuperação Merchant Center (Workflow Actor-Critic com Memória) ---
# =====================================================================
async def run_merchant_recovery_pipeline(row_data: Dict[str, Any]):
    """
    Processa a linha focando em conformidade Merchant Center.
    Utiliza um loop onde um Avaliador simula o Google, e um Refinador corrige.
    Possui "Memória" para aprender com aprovações passadas.
    """
    id_sku = row_data.get("_IDSKU")
    product_name = row_data.get("NomeProduto")
    titulo_site_atual = row_data.get("TituloSite")
    meta_description_atual = row_data.get("DescricaoMetaTag")
    html_content_atual = row_data.get("DescricaoProduto")
    
    # 0. Carrega a Memória de Aprendizado
    memoria_recente = await asyncio.to_thread(_format_memory_for_prompt)
    
    input_data = {
        "product_name": product_name,
        "titulo_site_atual": titulo_site_atual,
        "html_content_atual": html_content_atual,
        "memoria_recente": memoria_recente # Contexto injetado na IA
    }
    
    # --- 1. Geração Inicial (O Redator faz a primeira higienização) ---
    logging.info(f"[{id_sku}] Iniciando higienização primária com memória...")
    prompt_inicial = prompt_manager.render("refinador_merchant_safe", **input_data)
    response_raw = await asyncio.to_thread(_execute_prompt_with_backoff, prompt_inicial, timeout=90)
    refined_content = _extract_json_from_string(response_raw)
    
    status_processamento = "Recuperado"
    
    if not refined_content or not refined_content.get("html_content"):
        logging.warning(f"[{id_sku}] Falha na higienização inicial. Mantendo HTML original.")
        html_atual = html_content_atual
        seo_title = titulo_site_atual
        meta_desc = meta_description_atual
        status_processamento = "Erro (Mantido Original)"
    else:
        # A MÁGICA DE ENGENHARIA AQUI: Limpeza brutal do HTML gerado pela IA (Sem gastar com prompt)
        html_atual = _force_clean_html(refined_content.get("html_content", ""))
        
        # Pega o título já limpo sem as palavras críticas ("Quetiapina", "Carvedilol", etc.)
        seo_title = refined_content.get("seo_title", titulo_site_atual)
        meta_desc = refined_content.get("meta_description", meta_description_atual)

    # --- 2. Workflow Recursivo (O Juiz avalia e o Redator corrige) ---
    MAX_ATTEMPTS = 5 # Limite para não estourar tempo/tokens
    attempt = 0
    score = 0
    status_google = "REPROVADO"

    if status_processamento != "Erro (Mantido Original)" and len(str(html_atual).strip()) > 20:
        while attempt < MAX_ATTEMPTS and status_google != "APROVADO":
            attempt += 1
            logging.info(f"[{id_sku}] Avaliando no Merchant Simulator (Tentativa {attempt}/{MAX_ATTEMPTS})...")
            
            # O JUIZ: Avalia o HTML gerado usando o SEO_TITLE LIMPO!
            prompt_evaluator = prompt_manager.render(
                "gmc_simulator_evaluator",
                product_name=seo_title, 
                html_content=html_atual
            )
            eval_raw = await asyncio.to_thread(_execute_prompt_with_backoff, prompt_evaluator, timeout=60)
            eval_json = _extract_json_from_string(eval_raw)
            
            if not eval_json:
                logging.error(f"[{id_sku}] Falha ao extrair JSON do Simulador. Interrompendo loop.")
                break
                
            status_google = eval_json.get("status", "REPROVADO")
            score = eval_json.get("score", 0)
            feedbacks = eval_json.get("feedbacks_google", [])
            
            # APROVADO: Sai do loop e salva na memória para a IA ficar mais inteligente no próximo produto
            if status_google == "APROVADO" or score >= 90:
                logging.info(f"[{id_sku}] APROVADO no Simulator na tentativa {attempt} (Score: {score})!")
                status_processamento = "Recuperado (Aprovado)"
                
                # A MÁGICA: Aprende com o sucesso (mantemos product_name original para rastreio)
                await asyncio.to_thread(
                    _save_success_to_memory, 
                    product_name, 
                    html_content_atual, # Como era (sujo)
                    html_atual          # Como ficou (limpo e aprovado)
                )
                break
            
            logging.warning(f"[{id_sku}] Reprovado (Score {score}). Feedbacks: {feedbacks}")
            
            # Se for a última tentativa, aceita a derrota parcial
            if attempt == MAX_ATTEMPTS:
                logging.warning(f"[{id_sku}] Limite de tentativas alcançado. Retornando versão de score {score}.")
                status_processamento = f"Recuperado (Limitado - Score {score})"
                break
            
            # O REDATOR (REFINADOR): Aplica as correções com base no feedback do Juiz
            logging.info(f"[{id_sku}] Refinando texto baseado no feedback do Simulador...")
            prompt_refiner = prompt_manager.render(
                "refinador_recursivo_merchant",
                product_name=seo_title, # Usa o título limpo para não confundir o Refinador
                feedbacks_google=json.dumps(feedbacks, ensure_ascii=False),
                html_content_atual=html_atual,
                memoria_recente=memoria_recente
            )
            refiner_raw = await asyncio.to_thread(_execute_prompt_with_backoff, prompt_refiner, timeout=90)
            refiner_json = _extract_json_from_string(refiner_raw)
            
            if refiner_json and refiner_json.get("html_content") and len(str(refiner_json.get("html_content")).strip()) > 20:
                
                # DE NOVO: Limpeza brutal contra duplo HTML encoding antes de mandar pro Simulador de novo
                html_atual = _force_clean_html(refiner_json.get("html_content", ""))
                
                # ---> ADIÇÃO CRÍTICA: Atualiza o título se a IA sugerir um novo!
                if refiner_json.get("seo_title"):
                    seo_title = refiner_json.get("seo_title")
            else:
                logging.error(f"[{id_sku}] Refinador recursivo falhou. Mantendo HTML da tentativa anterior.")
                break

    # --- 3. Finalização para VTEX (Limpeza de resquícios Markdown) ---
    final_html = SeoOptimizerAgent._finalize_for_vtex(html_atual, seo_title)
    
    return {
        "id_sku": id_sku,
        "status": status_processamento,
        "content": {
            "seo_title": seo_title,
            "meta_description": meta_desc,
            "html_content": final_html
        }
    }