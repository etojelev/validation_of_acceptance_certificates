PY_SRCS=src
RADON_MIN_MI=65

.PHONY: help lint fmt type security cc mi hal raw check

help:
	@echo "Доступные цели:"
	@echo " lint - ruff check (с автофиксом)"
	@echo " fmt - ruff format"
	@echo " type - mypy (проверка типов)"
	@echo " security - bandit (скан безопасности)"
	@echo " cc - radon cc (цикломатическая сложность)"
	@echo " mi - radon mi (индекс поддерживаемости)"
	@echo " hal - radon hal (метрика халстеда)"
	@echo " raw - radon raw (SLOC, LLOC, комментарии)"
	@echo " check - быстрый локальный quality gate"

lint:
	uv run ruff check $(PY_SRCS) --fix

fmt:
	uv run ruff format $(PY_SRCS)

type:
	uv run mypy $(PY_SRCS)

security:
	uv run bandit -r src -lll -x .venv,venv,build,dist,migrations

cc:
	@if uv run radon cc -s $(PY_SRCS) | grep -E ' [EF] '; then \
		echo "❌ Radon CC: обнаружены функции со сложностью E/F"; \
		exit 1; \
	else \
		echo "✅ Radon CC: нет функций с E/F"; \
	fi

mi:
	@MI_BAD=$$(uv run radon mi $(PY_SRCS) | awk '{print $$NF}' | awk -F: '{print $$NF}' | awk '$$1+0<$(RADON_MIN_MI){print}'); \
	if [ -n "$$MI_BAD" ]; then \
		echo "❌ Radon MI: найден MI < $(RADON_MIN_MI)"; \
		exit 1; \
	else \
		echo "✅ Radon MI: все файлы с MI >= $(RADON_MIN_MI)"; \
	fi

hal:
	uv run radon hal $(PY_SRCS)

raw:
	uv run radon raw $(PY_SRCS)

check: lint fmt type security cc mi
	@echo "✅ Все проверки пройдены!"
