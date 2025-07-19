# src/pl/plsql/src/nls.mk
CATALOG_NAME     = plsql
GETTEXT_FILES    = pl_comp.c \
                   pl_exec.c \
                   pl_gram.c \
                   pl_funcs.c \
                   pl_handler.c \
                   pl_scanner.c
GETTEXT_TRIGGERS = $(BACKEND_COMMON_GETTEXT_TRIGGERS) yyerror:4 plsql_yyerror:4
GETTEXT_FLAGS    = $(BACKEND_COMMON_GETTEXT_FLAGS)
