"""Modelo Python do dbt: resolve nomes de farmaco no RxNorm.

Grao: uma linha por `nome_normalizado` distinto.

Por que um modelo Python e nao um passo separado
------------------------------------------------
A normalizacao poderia ser um script executado antes do dbt. Como modelo, ela vira um NO do
grafo de dependencias: o dbt sabe que `rxnorm_mapping` depende de `farmaco_nomes` e que
`dim_farmaco` depende de `rxnorm_mapping`. A ordem de execucao deixa de ser responsabilidade
de quem digita os comandos, o `dbt docs` mostra a linhagem completa e um `dbt run` seletivo
continua correto.

O custo de rede fica contido porque `farmaco_nomes` ja reduziu o universo a nomes distintos e
o cliente mantem cache em disco entre execucoes.
"""

from pharma_pipeline.config import Settings
from pharma_pipeline.rxnorm import TTY_INGREDIENTE, normalizar_nomes


def model(dbt, session):
    dbt.config(materialized="table")

    nomes_df = dbt.ref("farmaco_nomes").project("nome_normalizado").df()
    nomes = sorted({nome for nome in nomes_df["nome_normalizado"].tolist() if nome})

    settings = Settings.from_env()
    resultados = normalizar_nomes(
        nomes,
        cache_path=settings.rxnorm_cache_path,
        offline=settings.rxnorm_offline,
        max_lookups=settings.rxnorm_max_lookups,
    )

    linhas = [
        {
            "nome_normalizado": match.nome_normalizado,
            "rxcui": match.rxcui,
            "rxnorm_nome": match.rxnorm_nome,
            "rxnorm_tty": match.rxnorm_tty,
            "tipo_correspondencia": match.tipo_correspondencia,
            "score": match.score,
            # Um RxCUI de ingrediente (IN/MIN/PIN) agrupa apresentacoes diferentes do mesmo
            # principio ativo. Um RxCUI de produto (SCD/SBD) nao serve para isso, entao o
            # marcamos para que a dimensao possa decidir o que fazer.
            "nivel_ingrediente": match.rxnorm_tty in TTY_INGREDIENTE,
            "consultado_em": match.consultado_em,
        }
        for match in resultados
    ]

    if not linhas:
        # Sem nomes de entrada o modelo ainda precisa existir com o schema correto, senao os
        # modelos seguintes falham no parse em vez de simplesmente ficarem vazios.
        return session.sql(
            """
            select
                cast(null as varchar) as nome_normalizado,
                cast(null as varchar) as rxcui,
                cast(null as varchar) as rxnorm_nome,
                cast(null as varchar) as rxnorm_tty,
                cast(null as varchar) as tipo_correspondencia,
                cast(null as double)  as score,
                cast(null as boolean) as nivel_ingrediente,
                cast(null as varchar) as consultado_em
            where false
            """
        )

    import pandas as pd

    return pd.DataFrame(linhas).astype(
        {
            "nome_normalizado": "string",
            "rxcui": "string",
            "rxnorm_nome": "string",
            "rxnorm_tty": "string",
            "tipo_correspondencia": "string",
            "score": "float64",
            "nivel_ingrediente": "bool",
            "consultado_em": "string",
        }
    )
