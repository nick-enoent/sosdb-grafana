#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT([sosdb-grafana], 4.0.0, nick@ogc.us)
AC_CONFIG_MACRO_DIR([m4])
AM_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])

gplugin=/var/lib/grafana/plugins/sosds
distdir=${PACKAGE_NAME}-${PACKAGE_VERSION}

AC_SUBST(prefix)
AC_SUBST(distdir)
AC_SUBST(gplugin)

AC_ARG_ENABLE([baler],
[  --enable-baler],
[case "${enableval}" in
  yes) baler=true ;;
  no)  baler=false ;;
  *) AC_MSG_ERROR([bad value ${enableval} for --enable-baler]) ;;
esac],[baler=false])
AM_CONDITIONAL([BALER], [test x$baler = xtrue])

AC_CONFIG_FILES([
        Makefile
        sosgui/Makefile
        grafana/Makefile
        sosds/Makefile
        ])
AC_OUTPUT
