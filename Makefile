__ignored__ := $(shell ./setup.sh)

PACKAGES=syndicate syndicate-examples
COLLECTS=syndicate syndicate-examples

all: setup

clean:
	find . -name compiled -type d | xargs rm -rf
	find . -name '*.rkte' | xargs rm -rf

setup:
	raco setup --check-pkg-deps --unused-pkg-deps $(COLLECTS)

link:
	raco pkg install --link $(PACKAGES)

unlink:
	raco pkg remove $(PACKAGES)

test: setup testonly

testonly:
	raco test -p $(PACKAGES)
