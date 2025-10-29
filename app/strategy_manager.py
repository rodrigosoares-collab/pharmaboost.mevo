# app/strategy_manager.py
import os
import json
from datetime import datetime, timezone
from typing import Tuple

from collections import defaultdict

class StrategyManager:
    """
    Gerencia a leitura e escrita do ledger de estratégias de SEO,
    fornecendo aprendizado histórico para o otimizador.
    """
    def __init__(self, ledger_file='estrategias_pharma_seo.json'):
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.ledger_file = os.path.join(project_root, ledger_file)

    def _read_ledger(self) -> list:
        """Lê o arquivo de estratégias de forma segura."""
        try:
            if os.path.exists(self.ledger_file) and os.path.getsize(self.ledger_file) > 0:
                with open(self.ledger_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return []
        except (json.JSONDecodeError, FileNotFoundError):
            return []

    def _write_ledger(self, ledger: list):
        """Escreve no arquivo de estratégias de forma segura."""
        with open(self.ledger_file, 'w', encoding='utf-8') as f:
            json.dump(ledger, f, indent=2, ensure_ascii=False)

    def _derive_strategy_from_feedback(self, analysis_before: dict, analysis_after: dict) -> str:
        """
        Identifica qual ponto de feedback foi resolvido para descrever a estratégia.
        """
        feedback_before = set()
        for category in analysis_before.get("breakdown", {}).values():
            feedback_before.update(category.get("feedback", []))
            
        feedback_after = set()
        for category in analysis_after.get("breakdown", {}).values():
            feedback_after.update(category.get("feedback", []))

        resolved_feedback = feedback_before - feedback_after
        if resolved_feedback:
            return f"Correção aplicada: '{resolved_feedback.pop()}'"
        return "Otimização geral de SEO."


    def log_strategy(self, analysis_before: dict, analysis_after: dict, product_type: str):
        """
        Registra o resultado de uma tentativa de otimização no ledger.
        """
        score_before = analysis_before.get("total_score", 0)
        score_after = analysis_after.get("total_score", 0)
        score_improvement = score_after - score_before

        if score_improvement == 0:
            return

        strategy_description = self._derive_strategy_from_feedback(analysis_before, analysis_after)

        record = {
            "estrategia_aplicada": strategy_description,
            "tipo_de_produto": product_type,
            "texto_original_score": score_before,
            "novo_texto_score": score_after,
            "melhora_score": score_improvement,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        ledger = self._read_ledger()
        ledger.append(record)
        self._write_ledger(ledger)
        print(f"Estratégia registrada: {strategy_description} (Melhora: {score_improvement})")

    def get_strategies(self, product_type: str, top_n: int = 3) -> Tuple[str, str]:
        """
        Obtém as melhores e piores estratégias do histórico para o tipo de produto.
        """
        default_success_msg = "Nenhuma estratégia de sucesso registrada. Usando conhecimento geral."
        default_fail_msg = "Nenhuma estratégia de falha registrada."

        ledger = self._read_ledger()
        if not ledger:
            return default_success_msg, default_fail_msg

        relevant_strategies = [s for s in ledger if s.get('tipo_de_produto') == product_type]
        if not relevant_strategies:
            relevant_strategies = ledger

        sorted_strategies = sorted(relevant_strategies, key=lambda x: x['melhora_score'], reverse=True)
        
        successful = sorted_strategies[:top_n]
        successful_str = "\n".join([f"- {s['estrategia_aplicada']} (Melhora de Score: +{s['melhora_score']})" for s in successful if s['melhora_score'] > 0])
        
        failed = [s for s in sorted_strategies if s['melhora_score'] <= 0]
        failed_str = "\n".join([f"- {s['estrategia_aplicada']} (Piora de Score: {s['melhora_score']})" for s in failed[:top_n]])

        return successful_str or default_success_msg, \
               failed_str or default_fail_msg