sosds_DATA = package.json README.md LICENSE Gruntfile.js
sosdsdir = ${gplugin}
EXTRA_DIST = $(sosds_DATA)

dist_DATA = \
	dist/datasource.js \
	dist/datasource.js.map \
	dist/module.js \
	dist/module.js.map \
	dist/plugin.json \
	dist/query_ctrl.js \
	dist/query_ctrl.js.map
distdir = ${gplugin}/dist
EXTRA_DIST += $(dist_DATA)

cssdist_DATA = dist/css/query-editor.css
cssdistdir = ${gplugin}/dist/css
EXTRA_DIST += $(cssdist_DATA)

imgdist_DATA = dist/img/ogc_logo.png
imgdistdir = ${gplugin}/dist/img
EXTRA_DIST += $(imgdist_DATA)

partialsdist_DATA = \
	dist/partials/annotations.editor.html \
	dist/partials/config.html \
	dist/partials/query.editor.html \
	dist/partials/query.options.html
partialsdistdir = ${gplugin}/dist/partials
EXTRA_DIST += $(partialsdist_DATA)

