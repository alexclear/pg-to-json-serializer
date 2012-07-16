MODULE_big = serializer
OBJS = serializer.o deserializer.o

ifeq ($(OPTION_WITH_DESERIALIZER), 1)
	PG_CPPFLAGS = -DOPTION_WITH_DESERIALIZER
	SHLIB_LINK = -L/usr/local/lib -ljson
endif

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

