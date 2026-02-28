FILES =									\
	dbus-heatpump-control.py			\
	utils.py							\
	s2.py								\

AIOVELIB =								\
	__init__.py							\
	service.py							\
	client.py							\
	localsettings.py					\
	vreglink.py							\
	s2.py								\

all:

install:
	install -d $(DESTDIR)$(bindir)
	install -d $(DESTDIR)$(bindir)/aiovelib
	install -m 0644 $(FILES) $(DESTDIR)$(bindir)
	install -m 0644 $(addprefix ext/aiovelib/aiovelib/,$(AIOVELIB)) \
		$(DESTDIR)$(bindir)/aiovelib
	chmod +x $(DESTDIR)$(bindir)/$(firstword $(FILES))

testinstall:
	$(eval TMP := $(shell mktemp -d))
	$(MAKE) DESTDIR=$(TMP) install
	(cd $(TMP) && python3 dbus-heatpump-control.py --help > /dev/null)
	-rm -rf $(TMP)

test:
	python3 -m unittest discover

clean:

.PHONY: help install testinstall test