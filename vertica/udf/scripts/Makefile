
BASEDIR=$(CURDIR)
APP_SRC=$(BASEDIR)/../src
APP_INCLUDE=$(BASEDIR)/../include
VERTICA_INCLUDE=$(BASEDIR)/../vertica_include
SRC = ${SRC}

CC = g++
CFLAGS = -g -Wall -Wno-unused-value -shared -fPIC -std=gnu++0x -O3 -rdynamic
INCLUDES = -I $(VERTICA_INCLUDE) -I HelperLibraries -I $(APP_INCLUDE)
SOURCES = $(APP_SRC)/$(SRC).cpp $(VERTICA_INCLUDE)/Vertica.cpp
EXECUTABLE = $(BASEDIR)/TransformFunctions.so
DEFINES = ${ARGS}

all:
	$(CC) $(INCLUDES) $(CFLAGS) $(DEFINES) -o $(EXECUTABLE) $(SOURCES)
