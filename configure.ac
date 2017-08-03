#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(sos_web_svcs, 1.2, nick@ogc.us)
AC_CONFIG_MACRO_DIR([m4])
AM_CONFIG_HEADER(config.h)
AM_INIT_AUTOMAKE([foreign])
AM_PATH_PYTHON([2.7])
# Checks for programs.
#AC_PROG_CXX
#AC_PROG_CC
#AC_PROG_INSTALL

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.

gplugin=/var/lib/grafana/plugins/sosds
distdir=${PACKAGE_NAME}
AC_SUBST(prefix)
AC_SUBST(distdir)
AC_SUBST(gplugin)

AC_CONFIG_FILES([
        Makefile
        sosgui/Makefile
        grafana/Makefile
        sosds/Makefile
        sosds/dist/Makefile
        sosds/dist/css/Makefile
        sosds/dist/img/Makefile
        sosds/dist/partials/Makefile
        ])
AC_OUTPUT
