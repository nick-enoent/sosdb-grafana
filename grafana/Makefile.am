if BALER
GBALER = models_baler.py
else
GBALER =
endif
grafana_PYTHON = __init__.py \
	urls.py	\
	models_sos.py \
        views.py \
        analysis.py
if BALER
grafana_PYTHON += models_baler.py 
else
endif
grafanadir = $(prefix)/grafana
