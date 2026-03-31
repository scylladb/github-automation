.PHONY: help test hooks

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

test: ## Run backport unit tests
	@python3 -m pytest .github/tests/ -v

hooks: ## Configure git to use the repo's hooks/ directory
	@git config core.hooksPath hooks
	@echo "Git hooks configured (core.hooksPath = hooks)."
