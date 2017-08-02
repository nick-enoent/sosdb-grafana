#!/bin/sh

test -d build || mkdir build
test -d m4 || mkdir m4

autoreconf -v --force --install -I m4
