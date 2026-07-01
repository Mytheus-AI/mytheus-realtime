.PHONY: install lint typecheck test ci

GW = services/realtime-gateway
TX = services/transcription-service

install:
	cd $(GW) && npm install
	cd $(TX) && uv sync

lint:
	cd $(TX) && uv run ruff check . && uv run black --check .

typecheck:
	cd $(GW) && npm run typecheck
	cd $(TX) && uv run mypy .

test:
	cd $(GW) && npm run build
	cd $(TX) && uv run pytest

ci: lint typecheck test
