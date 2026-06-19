crawl: 
	uv run python crawler/fetch_tiki_8322.py

dbt-run: 
	cd dbt && uv run --env-file ../.env dbt run


# docker exec -it tiki_superset pip install sqlalchemy-trino
# docker restart tiki_superset
