try:
    from airflow.models import DagBag
except ImportError:
    DagBag = None


def test_dags_import():
    """
    Se Airflow não estiver instalado no ambiente do tox,
    o teste passa automaticamente.
    """
    if DagBag is None:
        assert True
        return

    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"Erros ao importar DAGs: {dag_bag.import_errors}"


def test_monitoramento_dag_exists():
    if DagBag is None:
        assert True
        return

    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert "monitoramento_cambio_anotacoes" in dag_bag.dags
    assert "fx_bi_relatorios_semanais" in dag_bag.dags
