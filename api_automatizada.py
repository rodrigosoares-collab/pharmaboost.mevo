# api_automatizada.py (Versão Corrigida e Aprimorada)
import logging
from config import settings
from config.logging_config import setup_logging

setup_logging()

import asyncio
import io
import json
import os
import re
import pandas as pd
import openpyxl
import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pypdf import PdfReader
from typing import List, Optional, Dict, Any
import base64

from app import use_cases

# Verificação crítica de chaves de API na inicialização
if not settings.API_KEY or not os.getenv("GOOGLE_API_KEY") or not os.getenv("GOOGLE_CSE_ID"):
    logging.critical("ERRO CRÍTICO: Chaves de API não encontradas. Verifique o arquivo .env.")
    raise RuntimeError("ERRO CRÍTICO: Chaves de API não encontradas. Verifique o arquivo .env.")

app = FastAPI(
    title="PharmaBoost Automation API",
    description="API para processamento de conteúdo com curadoria humana e feedback loop para IA.",
    version="31.0-stable"
)

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Semáforos para controlar a concorrência e evitar sobrecarga
MAX_CONCURRENT_REQUESTS = 50
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(10)

# Constantes para nomes de colunas para evitar erros de digitação
COLUNA_EAN_SKU = '_EANSKU'
COLUNA_NOME_PRODUTO = '_NomeProduto (Obrigatório)'
COLUNA_TITULO_SITE = '_TituloSite'
COLUNA_META_DESCRICAO = '_DescricaoMetaTag'
COLUNA_DESCRICAO_PRODUTO = '_DescricaoProduto'
COLUNA_MARCA = '_Marca'

COLUNA_CODIGO_BARRAS_CATALOGO = 'CODIGO_BARRAS'
COLUNA_LINK_BULA = 'BULA'
COLUNA_LINK_VALIDO = 'LINK_VALIDACAO'

# Define o schema completo do modelo V-TEX para garantir a integridade do arquivo final
COLUNAS_MODELO_XLS = [
    '_IDSKU (Não alterável)', '_NomeSKU', '_AtivarSKUSePossível',
    '_SKUAtivo (Não alterável)', '_EANSKU', '_Altura', '_AlturaReal',
    '_Largura', '_LarguraReal', '_Comprimento', '_ComprimentoReal',
    '_Peso', '_PesoReal', '_UnidadeMedida', '_MultiplicadorUnidade',
    '_CodigoReferenciaSKU', '_ValorFidelidade', '_DataPrevisaoChegada',
    '_CodigoFabricante', '_IDProduto (Não alterável)', '_NomeProduto (Obrigatório)',
    '_BreveDescricaoProduto', '_ProdutoAtivo (Não alterável)',
    '_CodigoReferenciaProduto', '_MostrarNoSite', '_LinkTexto (Não alterável)',
    '_DescricaoProduto', '_DataLancamentoProduto', '_PalavrasChave',
    '_TituloSite', '_DescricaoMetaTag', '_IDFornecedor',
    '_MostrarSemEstoque', '_Kit (Não alterável)', '_IDDepartamento (Não alterável)',
    '_NomeDepartamento', '_IDCategoria', '_NomeCategoria', '_IDMarca',
    '_Marca', '_PesoCubico', '_CondicaoComercial', '_Lojas',
    '_Acessorios', '_Similares', '_Sugestoes', '_ShowTogether', '_Anexos'
]

async def _send_event(event_type: str, data: dict):
    """Envia um evento formatado para Server-Sent Events (SSE)."""
    await asyncio.sleep(0.01)
    return f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    """Extrai texto de um arquivo PDF fornecido em bytes."""
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "".join(page.extract_text() for page in reader.pages if page.extract_text())
    except Exception:
        logging.error("Falha ao extrair texto de bytes de PDF.", exc_info=True)
        return ""

def _convert_drive_url_to_download_url(url: str) -> Optional[str]:
    """Converte um link de visualização do Google Drive em um link de download direto."""
    match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
    if match:
        file_id = match.group(1)
        return f'https://drive.google.com/uc?export=download&id={file_id}'
    return None

async def get_bula_text_from_link(ean_sku: str, link_bula: str) -> str:
    """Baixa um arquivo PDF (bula) de um link e extrai seu texto."""
    os.makedirs('bulas_temp', exist_ok=True)
    output_path = f"bulas_temp/{ean_sku}.pdf"
    download_url = link_bula

    if "drive.google.com" in download_url:
        logging.info(f"Convertendo link do Google Drive para SKU {ean_sku}")
        download_url = _convert_drive_url_to_download_url(download_url)
        if not download_url:
            logging.error(f"Não foi possível extrair o ID do arquivo do link para SKU {ean_sku}")
            return ""

    async with DOWNLOAD_SEMAPHORE:
        try:
            logging.info(f"Baixando bula para SKU {ean_sku}")
            with requests.get(download_url, stream=True, timeout=30) as response:
                response.raise_for_status()
                if 'text/html' in response.headers.get('Content-Type', ''):
                    soup = BeautifulSoup(response.content, 'html.parser')
                    confirm_link = soup.find('a', {'id': 'uc-download-link'})
                    if confirm_link:
                        confirm_url = 'https://drive.google.com' + confirm_link['href']
                        response = requests.get(confirm_url, stream=True, timeout=30)
                        response.raise_for_status()

                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            with open(output_path, 'rb') as f:
                return extract_text_from_pdf_bytes(f.read())
        except Exception as e:
            logging.error(f"Erro no download para SKU {ean_sku}: {e}", exc_info=False)
            return ""
        finally:
            if os.path.exists(output_path):
                os.remove(output_path)

def read_spreadsheet(file_bytes: bytes, filename: str) -> pd.DataFrame:
    """Lê dados de uma planilha (Excel ou CSV) a partir de bytes."""
    try:
        logging.info(f"Lendo a planilha: {filename}")
        if filename.lower().endswith('.csv'):
            return pd.read_csv(io.BytesIO(file_bytes), encoding='utf-8-sig', sep=',')
        else:
            return pd.read_excel(io.BytesIO(file_bytes), engine='openpyxl')
    except Exception as e:
        logging.error(f"Não foi possível ler a planilha '{filename}'.", exc_info=True)
        raise ValueError(f"Não foi possível ler a planilha '{filename}'. Erro: {e}")

@app.post("/batch-process-and-generate-draft")
async def batch_process_stream(
    items_file: UploadFile = File(...),
    catalog_file: Optional[UploadFile] = File(None),
    context_file: Optional[UploadFile] = File(None)
):
    try:
        items_bytes = await items_file.read()
        items_filename = items_file.filename
        catalog_bytes = await catalog_file.read() if catalog_file else None
        context_text = (await context_file.read()).decode('utf-8', errors='ignore') if context_file else None
        logging.info("Arquivos de lote recebidos.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler os arquivos: {e}")

    async def event_stream(it_bytes, it_filename, cat_bytes, ctx_text):
        resultados_finais = []
        summary = {'success': 0, 'skipped': 0, 'errors': 0}

        is_medicine_batch = cat_bytes is not None
        pipeline_type = "medicine" if is_medicine_batch else "beauty"

        async def worker(row, semaphore, queue, counter, total_items, summary_dict, df_cat):
            async with semaphore:
                ean_sku = str(row.get(COLUNA_EAN_SKU, 'N/A'))
                nome_produto = str(row.get(COLUNA_NOME_PRODUTO, 'N/A'))
                marca_produto = str(row.get(COLUNA_MARCA, ''))
                
                try:
                    counter[0] += 1
                    await queue.put(await _send_event("progress", {"current": counter[0], "total": total_items, "sku": ean_sku}))
                    
                    product_info = {}
                    if pipeline_type == "medicine":
                        catalog_row = df_cat.loc[df_cat[COLUNA_CODIGO_BARRAS_CATALOGO] == ean_sku]
                        if catalog_row.empty: raise ValueError("SKU não encontrado no catálogo.")
                        
                        is_valid = str(catalog_row.iloc[0].get(COLUNA_LINK_VALIDO, '')).strip().lower() == 'sim'
                        if not is_valid: raise ValueError("Item não validado no catálogo.")
                        
                        link_bula = catalog_row.iloc[0].get(COLUNA_LINK_BULA)
                        if pd.isna(link_bula) or not str(link_bula).strip(): raise ValueError("Link da bula ausente no catálogo.")
                        
                        bula_text = await get_bula_text_from_link(ean_sku, link_bula)
                        if not bula_text.strip(): raise ValueError("Falha ao ler o PDF da bula.")
                        product_info = {"bula_text": bula_text, "brand": marca_produto}
                    
                    else: # pipeline_type == "beauty"
                        descricao_html = row.get(COLUNA_DESCRICAO_PRODUTO, "")
                        descricao_texto_puro = BeautifulSoup(str(descricao_html), 'html.parser').get_text(separator=' ', strip=True)
                        contexto_enriquecido = f"""
                        - Nome do Produto: {nome_produto}
                        - Marca: {marca_produto}
                        - Informações Adicionais: {descricao_texto_puro}
                        - Contexto Geral do Cliente: {ctx_text if ctx_text else "Nenhum contexto adicional fornecido."}
                        """
                        product_info = {"context_text": contexto_enriquecido, "brand": marca_produto}

                    async for chunk in use_cases.run_seo_pipeline_stream(pipeline_type, nome_produto, product_info):
                        if "event: done" in chunk:
                            final_data = json.loads(chunk.split('data: ')[1])
                            final_data.update({COLUNA_EAN_SKU: ean_sku, COLUNA_NOME_PRODUTO: nome_produto})
                            resultados_finais.append(final_data)
                            await queue.put(await _send_event("done", final_data))
                            summary_dict['success'] += 1
                        else:
                            await queue.put(chunk)
                except Exception as e:
                    summary_dict['skipped'] += 1
                    logging.warning(f"[SKU: {ean_sku}] Item ignorado. Razão: {e}")
                    await queue.put(await _send_event("log", {"message": f"<b>[SKU: {ean_sku}]</b> Ignorado. Motivo: {e}", "type": "warning"}))
                finally:
                    await queue.put(None)
        try:
            df_processar = read_spreadsheet(it_bytes, it_filename)
            df_catalogo = None
            if is_medicine_batch:
                yield await _send_event("log", {"message": "<b>Catálogo detectado.</b> Iniciando processamento em modo MEDICAMENTO.", "type": "info"})
                df_catalogo = read_spreadsheet(cat_bytes, "catalogo.xlsx")
                df_catalogo.columns = [str(col).strip().upper() for col in df_catalogo.columns]
                df_catalogo[COLUNA_CODIGO_BARRAS_CATALOGO] = df_catalogo[COLUNA_CODIGO_BARRAS_CATALOGO].astype(str)
            else:
                yield await _send_event("log", {"message": "<b>Catálogo não fornecido.</b> Iniciando processamento em modo BELEZA.", "type": "info"})

            total_items = len(df_processar)
            yield await _send_event("log", {"message": f"Planilha lida. {total_items} itens para processar...", "type": "info"})
            
            queue = asyncio.Queue()
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            processed_counter = [0]
            
            worker_tasks = [
                asyncio.create_task(worker(row, semaphore, queue, processed_counter, total_items, summary, df_catalogo))
                for _, row in df_processar.iterrows()
            ]
            
            finished_workers = 0
            while finished_workers < len(worker_tasks):
                item = await queue.get()
                if item is None:
                    finished_workers += 1
                elif item:
                    yield item

            await asyncio.gather(*worker_tasks)

            summary_message = f"<b>Processamento em lote finalizado.</b> Sumário: {summary['success']} com sucesso, {summary['skipped']} ignorados."
            yield await _send_event("log", {"message": summary_message, "type": "info"})
            
            if not resultados_finais:
                yield await _send_event("log", {"message": "<b>AVISO:</b> Nenhum produto foi processado com sucesso.", "type": "warning"})
                return

            yield await _send_event("log", {"message": "<b>Montando o rascunho para curadoria...</b>", "type": "info"})
            
            df_itens_original = read_spreadsheet(it_bytes, it_filename)
            df_itens_original[COLUNA_EAN_SKU] = df_itens_original[COLUNA_EAN_SKU].astype(str)
            
            for res in resultados_finais:
                sku = str(res.get(COLUNA_EAN_SKU))
                mask = df_itens_original[COLUNA_EAN_SKU].str.strip() == sku.strip()
                df_itens_original.loc[mask, COLUNA_TITULO_SITE] = res.get("seo_title", "Erro")
                df_itens_original.loc[mask, COLUNA_META_DESCRICAO] = res.get("meta_description", "Erro")
                df_itens_original.loc[mask, COLUNA_DESCRICAO_PRODUTO] = res.get("final_content", "Erro")
                        
            output_buffer = io.BytesIO()
            df_itens_original.to_excel(output_buffer, index=False)
            file_data_b64 = base64.b64encode(output_buffer.getvalue()).decode('utf-8')
            yield await _send_event("finished", {"filename": "rascunho_para_revisao.xlsx", "file_data": file_data_b64})

        except Exception as e:
            logging.exception("Erro fatal durante o processamento em lote.")
            yield await _send_event("log", {"message": f"Erro fatal no processamento: {e}", "type": "error"})
    
    return StreamingResponse(event_stream(items_bytes, items_filename, catalog_bytes, context_text), media_type="text/event-stream")

@app.post("/process-manual-single")
async def process_manual_single_stream(product_name: str = Form(...), ean_sku: str = Form(...), bula_file: UploadFile = File(...)):
    """Processa um único item manualmente (sempre como MEDICAMENTO), usando um PDF de bula."""
    try:
        pdf_bytes = await bula_file.read()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler o arquivo: {e}")

    async def event_stream(bytes_to_process: bytes):
        try:
            bula_text = await asyncio.to_thread(extract_text_from_pdf_bytes, bytes_to_process)
            if not bula_text.strip():
                raise ValueError("Não foi possível extrair texto do PDF.")

            yield await _send_event("log", {"message": f"PDF da bula lido com sucesso.", "type": "success"})

            async for chunk in use_cases.run_seo_pipeline_stream("medicine", product_name, {"bula_text": bula_text}):
                if "event: done" in chunk:
                    final_data = json.loads(chunk.split('data: ')[1])
                    final_data.update({COLUNA_EAN_SKU: ean_sku, COLUNA_NOME_PRODUTO: product_name})
                    yield await _send_event("done_manual", final_data)
                else:
                    yield chunk
        except Exception as e:
            logging.exception(f"Erro fatal durante o processamento manual para o SKU {ean_sku}.")
            yield await _send_event("log", {"message": f"ERRO FATAL (Manual): {e}", "type": "error"})

    return StreamingResponse(event_stream(pdf_bytes), media_type="text/event-stream")

@app.post("/finalize-spreadsheet")
async def finalize_spreadsheet(approved_data_json: str = Form(...), spreadsheet: UploadFile = File(...)):
    """Gera a planilha final contendo APENAS os itens aprovados, prontos para importação."""
    try:
        approved_data = json.loads(approved_data_json)
        if not approved_data: raise HTTPException(status_code=400, detail="Nenhum item aprovado enviado.")
        if not spreadsheet: raise HTTPException(status_code=400, detail="A planilha base é obrigatória.")

        df_base = pd.read_excel(io.BytesIO(await spreadsheet.read()), engine='openpyxl')
        df_base[COLUNA_EAN_SKU] = df_base[COLUNA_EAN_SKU].astype(str).str.strip()

        df_updates = pd.DataFrame(approved_data)
        
        # --- LÓGICA CORRIGIDA ---
        # 1. Renomeia as colunas de 'updates' ANTES de qualquer outra operação.
        df_updates.rename(columns={'sku': COLUNA_EAN_SKU, 'seoTitle': COLUNA_TITULO_SITE, 'metaDescription': COLUNA_META_DESCRICAO, 'htmlContent': COLUNA_DESCRICAO_PRODUTO}, inplace=True)
        df_updates[COLUNA_EAN_SKU] = df_updates[COLUNA_EAN_SKU].astype(str).str.strip()
        
        # 2. Filtra a planilha base para manter APENAS os SKUs aprovados.
        approved_skus = df_updates[COLUNA_EAN_SKU].unique()
        df_approved_only = df_base[df_base[COLUNA_EAN_SKU].isin(approved_skus)].copy()
        
        # 3. Atualiza os dados na planilha já filtrada.
        df_approved_only.set_index(COLUNA_EAN_SKU, inplace=True)
        df_updates.set_index(COLUNA_EAN_SKU, inplace=True)
        df_approved_only.update(df_updates)
        df_approved_only.reset_index(inplace=True)
        
        # 4. Garante que todas as colunas do modelo V-TEX existam.
        for col in COLUNAS_MODELO_XLS:
            if col not in df_approved_only.columns:
                df_approved_only[col] = None
        
        # 5. Organiza as colunas na ordem correta e exporta.
        df_final = df_approved_only[COLUNAS_MODELO_XLS]
        # --- FIM DA CORREÇÃO ---

        output_buffer = io.BytesIO()
        df_final.to_excel(output_buffer, index=False)
        return Response(content=output_buffer.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=planilha_final_aprovados.xlsx"})
    except Exception as e:
        logging.exception("Erro ao finalizar a planilha de aprovados.")
        raise HTTPException(status_code=500, detail=f"Erro ao finalizar planilha: {str(e)}")


@app.post("/finalize-disapproved-spreadsheet")
async def finalize_disapproved_spreadsheet(spreadsheet: UploadFile = File(...), disapproved_data_json: str = Form(...)):
    """Gera uma planilha contendo APENAS os itens que foram reprovados."""
    try:
        df_original = pd.read_excel(io.BytesIO(await spreadsheet.read()), engine='openpyxl')
        disapproved_data = json.loads(disapproved_data_json)
        if not disapproved_data: raise HTTPException(status_code=400, detail="Nenhum item reprovado enviado.")
        
        disapproved_skus = [str(item['sku']).strip() for item in disapproved_data]
        df_original[COLUNA_EAN_SKU] = df_original[COLUNA_EAN_SKU].astype(str).str.strip()
        df_disapproved = df_original[df_original[COLUNA_EAN_SKU].isin(disapproved_skus)].copy()

        output_buffer = io.BytesIO()
        df_disapproved.to_excel(output_buffer, index=False)
        return Response(content=output_buffer.getvalue(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=planilha_reprovados.xlsx"})
    except Exception as e:
        logging.exception("Erro ao gerar a planilha de reprovados.")
        raise HTTPException(status_code=500, detail=f"Erro ao gerar planilha: {str(e)}")


@app.post("/reprocess-items")
async def reprocess_items(
    items_to_reprocess_json: str = Form(...),
    catalog_file: Optional[UploadFile] = File(None),
    context_file: Optional[UploadFile] = File(None) 
):
    """Reprocessa itens reprovados, usando feedback e a mesma lógica de detecção de tipo."""
    try:
        items_to_reprocess = json.loads(items_to_reprocess_json)
        catalog_bytes = await catalog_file.read() if catalog_file else None
        context_text = (await context_file.read()).decode('utf-8', errors='ignore') if context_file else None
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler dados para reprocessamento: {e}")

    async def event_stream(it_to_reprocess, cat_bytes, ctx_text):
        is_medicine_batch = cat_bytes is not None
        pipeline_type = "medicine" if is_medicine_batch else "beauty"
        
        df_catalogo = None
        if is_medicine_batch:
            logging.info("REPROCESSAMENTO: Catálogo detectado. Usando pipeline de MEDICAMENTO.")
            df_catalogo = read_spreadsheet(cat_bytes, "catalogo.xlsx")
            df_catalogo.columns = [str(col).strip().upper() for col in df_catalogo.columns]
            df_catalogo[COLUNA_CODIGO_BARRAS_CATALOGO] = df_catalogo[COLUNA_CODIGO_BARRAS_CATALOGO].astype(str)
        else:
            logging.info("REPROCESSAMENTO: Catálogo não fornecido. Usando pipeline de BELEZA.")

        for item in it_to_reprocess:
            ean_sku = str(item.get("sku"))
            nome_produto = item.get("productName")
            feedback = item.get("feedback")
            previous_content = json.loads(item.get("rawJsonContent", "{}"))
            
            try:
                product_info = {}
                if pipeline_type == "medicine":
                    if df_catalogo is None: raise ValueError("Catálogo de bulas é obrigatório para reprocessar medicamentos.")
                    catalog_row = df_catalogo.loc[df_catalogo[COLUNA_CODIGO_BARRAS_CATALOGO] == ean_sku]
                    if catalog_row.empty: raise ValueError("SKU não encontrado no catálogo para reprocessamento.")
                    
                    link_bula = catalog_row.iloc[0].get(COLUNA_LINK_BULA)
                    if pd.isna(link_bula) or not str(link_bula).strip(): raise ValueError("Link da bula ausente.")
                    
                    bula_text = await get_bula_text_from_link(ean_sku, link_bula)
                    if not bula_text.strip(): raise ValueError("Falha ao ler o PDF da bula.")
                    product_info = {"bula_text": bula_text}
                else: # pipeline_type == "beauty"
                    product_info = {"context_text": ctx_text if ctx_text else "Nenhum contexto adicional fornecido."}

                async for chunk in use_cases.run_seo_pipeline_stream(pipeline_type, nome_produto, product_info, previous_content=previous_content, feedback_text=feedback):
                    if "event: done" in chunk:
                        data = json.loads(chunk.split('data: ')[1])
                        data.update({COLUNA_EAN_SKU: ean_sku, COLUNA_NOME_PRODUTO: nome_produto})
                        yield f"event: done\ndata: {json.dumps(data)}\n\n"
                    else:
                        yield chunk
            except Exception as e:
                logging.warning(f"Falha no reprocessamento do SKU {ean_sku}: {e}")
                yield await _send_event("log", {"message": f"<b>[SKU: {ean_sku}]</b> Falha no reprocessamento. Motivo: {e}", "type": "error"})

    return StreamingResponse(event_stream(items_to_reprocess, catalog_bytes, context_text), media_type="text/event-stream")