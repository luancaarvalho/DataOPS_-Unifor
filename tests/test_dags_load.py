from airflow.models import DagBag


def test_dags_import():
    """
    Garante que as DAGs carregam sem erro.
    Esse teste falha se tiver erro de sintaxe ou import nas DAGs.
    """
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Erros ao importar DAGs: {dag_bag.import_errors}"


def test_monitoramento_dag_exists():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "monitoramento_cambio_anotacoes" in dag_bag.dags
    assert "fx_bi_relatorios_semanais" in dag_bag.dags
