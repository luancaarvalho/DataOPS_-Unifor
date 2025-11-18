# -*- coding: utf-8 -*-
"""
Validações de Data Quality para o dataset GOLD,
compatível com Great Expectations 0.18.16 (sem PandasDataset).
"""

import pandas as pd


class GoldDataset:

    REQUIRED_COLUMNS = [
        "duracao_contato",
        "num_contatos_campanha",
        "faixa_idade",
        "faixa_duracao_contato",
        "resultado_ultimo_contato",
        "intensidade_contato",
        "eficiencia_campanha",
    ]

    VALID_FAIXA_DURACAO = ["0-1min", "1-3min", "3-10min", "10-30min", "30+min"]
    VALID_INTENSIDADE = ["single", "low", "medium", "high"]

    @staticmethod
    def validate(df: pd.DataFrame) -> dict:
        failed = []

        # 1️⃣ Verificar se colunas obrigatórias estão presentes
        missing = [c for c in GoldDataset.REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            failed.append(f"Colunas ausentes: {missing}")

        # 2️⃣ idade entre 18 e 100
        if "idade" in df.columns:
            if not df["idade"].dropna().between(18, 100).all():
                failed.append("Valores inválidos na coluna 'idade' (fora de 18-100).")

        # 3️⃣ Validar faixa_duracao_contato (somente categorias conhecidas)
        if "faixa_duracao_contato" in df.columns:
            invalid = df[~df["faixa_duracao_contato"].isin(GoldDataset.VALID_FAIXA_DURACAO)]
            if not invalid.empty:
                failed.append("Valores inválidos em 'faixa_duracao_contato'.")

        # 4️⃣ intensidade_contato deve ser uma das categorias válidas
        if "intensidade_contato" in df.columns:
            invalid = df[~df["intensidade_contato"].isin(GoldDataset.VALID_INTENSIDADE)]
            if not invalid.empty:
                failed.append("Valores inválidos em 'intensidade_contato'.")

        # 5️⃣ num_contatos_campanha não pode ser negativo
        if "num_contatos_campanha" in df.columns:
            if (df["num_contatos_campanha"] < 0).any():
                failed.append("num_contatos_campanha contém valores negativos.")

        # 6️⃣ eficiencia_campanha deve estar entre 0 e 1 (se não nulo)
        if "eficiencia_campanha" in df.columns:
            valid = df["eficiencia_campanha"].dropna().between(0, 1).all()
            if not valid:
                failed.append("eficiencia_campanha fora do intervalo esperado (0-1).")

        return {
            "success": len(failed) == 0,
            "failed_expectations": failed,
            "total_tests": 6,
        }
