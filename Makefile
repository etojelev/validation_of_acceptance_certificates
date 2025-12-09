PY_SRCS=src
RADON_MIN_MI=20  # Понижаем для начала

.PHONY: help lint fmt type security cc mi check pre-commit-check

help:
	@echo "Доступные цели:"
	@echo " lint  - ruff check (с автофиксом)"
	@echo " fmt   - ruff format"
	@echo " type  - mypy (проверка типов)"
	@echo " check - локальная проверка (полная)"
	@echo " pre-commit-check - для pre-commit хуков"

lint:
	uv run ruff check $(PY_SRCS) --fix

fmt:
	uv run ruff format $(PY_SRCS)

type:
	uv run mypy $(PY_SRCS)

security:
	uv run bandit -r src -lll -x .venv,venv,build,dist,migrations

cc:
	@echo "Running radon cc (игнорируя буквенные оценки)..."
	@# Проверяем только числовую сложность
	@COMPLEXITY_HIGH=$$(uv run radon cc -s $(PY_SRCS) | awk '$$1 ~ /[0-9]+:/ && $$2 > 10 {print}'); \
	if [ -n "$$COMPLEXITY_HIGH" ]; then \
		echo "❌ Функции с высокой цикломатической сложностью (>10):"; \
		echo "$$COMPLEXITY_HIGH"; \
		exit 1; \
	fi
	@echo "✅ Radon CC: проверка пройдена"

mi:
	@echo "Running radon mi..."
	@# Упрощенная проверка MI
	@MI_CHECK=$$(uv run radon mi $(PY_SRCS) | grep -o '([0-9.]*)' | tr -d '()' | awk '$$1 < $(RADON_MIN_MI) {print "❌ MI слишком низкий:", $$1}'); \
	if [ -n "$$MI_CHECK" ]; then \
		echo "$$MI_CHECK"; \
		exit 1; \
	fi
	@echo "✅ Radon MI: все файлы с MI >= $(RADON_MIN_MI)"

check: lint fmt type security cc mi
	@echo "✅ Все проверки пройдены!"

# Специальная цель для pre-commit (без автофикса)
pre-commit-check:
	@echo "=== Pre-commit проверки ==="
	@echo "1. Ruff check..."
	uv run ruff check $(PY_SRCS)
	@echo "2. Ruff format check..."
	uv run ruff format $(PY_SRCS) --check
	@echo "3. Mypy..."
	uv run mypy $(PY_SRCS)
	@echo "4. Bandit..."
	uv run bandit -r src -lll -x .venv,venv,build,dist,migrations
	@echo "✅ Pre-commit проверки пройдены"