TARGET=$(PACKAGE).$(LIB_EXTENSION)
SRCS=$(wildcard $(SRCDIR)/*.c)
OBJS=$(SRCS:.c=.o)
GCDAS=$(OBJS:.o=.gcda)
INSTALL?=install
LIBPQ_INCDIR=$(shell pg_config --includedir)
LIBPQ_LIBDIR=$(shell pg_config --libdir)

ifdef LIBPQ_COVERAGE
COVFLAGS=--coverage
endif

.PHONY: all install

all: $(TARGET)

%.o: %.c
	$(CC) $(CFLAGS) $(WARNINGS) $(COVFLAGS) $(CPPFLAGS) -I$(LIBPQ_INCDIR) -o $@ -c $<

$(TARGET): $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS) -L$(LIBPQ_LIBDIR) -lpq $(LIBS) $(PLATFORM_LDFLAGS) $(COVFLAGS)

install:
	$(INSTALL) -d $(INST_LIBDIR)
	$(INSTALL) $(TARGET) $(INST_LIBDIR)
	rm -f $(OBJS) $(TARGET) $(GCDAS)
