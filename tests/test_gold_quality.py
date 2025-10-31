import pytest

@pytest.mark.integration
def test_dim_commune_pk_unique(spark):
    df = spark.table("hive_metastore.gold.dim_commune")
    assert df.select("commune_sk").distinct().count() == df.count(), "PK commune_sk non unique"

@pytest.mark.integration
def test_dim_reseau_pk_unique(spark):
    df = spark.table("hive_metastore.gold.dim_reseau")
    assert df.select("reseau_sk").distinct().count() == df.count(), "PK reseau_sk non unique"

@pytest.mark.integration
def test_fact_prelevement_fk_commune(spark):
    fact = spark.table("hive_metastore.gold.fact_prelevement")
    dim = spark.table("hive_metastore.gold.dim_commune")
    missing = fact.join(dim, "commune_sk", "left_anti")
    assert missing.count() == 0, f"Fact → Commune FK manquante : {missing.count()} lignes"

@pytest.mark.integration
def test_fact_resultat_fk_parametre(spark):
    fact = spark.table("hive_metastore.gold.fact_resultat")
    dim = spark.table("hive_metastore.gold.dim_parametre")
    missing = fact.join(dim, "parametre_sk", "left_anti")
    assert missing.count() == 0, f"Fact → Paramètre FK manquante : {missing.count()} lignes"
