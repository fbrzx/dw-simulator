PY_SERVICE_DIR := services/dw-simulator
PY_TEST_CMD := PYTHONPATH=src pytest

.PHONY: install test build up up-detached down api ui clean

install:
	cd $(PY_SERVICE_DIR) && python -m pip install -e .[dev]

test:
	cd $(PY_SERVICE_DIR) && $(PY_TEST_CMD)

build:
	docker compose build

up:
	docker compose up

up-detached:
	docker compose up -d

down:
	docker compose down

api:
	cd $(PY_SERVICE_DIR) && dw-sim api --host 0.0.0.0 --port 8000 --reload

ui:
	cd services/web-ui && python -m http.server 4173

clean:
	docker compose down -v || true
	rm -rf data/sqlite
	rm -rf $(PY_SERVICE_DIR)/.pytest_cache $(PY_SERVICE_DIR)/.coverage
