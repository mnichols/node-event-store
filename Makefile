ROOT_DIR = .
MAKE = make
# use subshell to pull filelist for testing
TEST_TYPE = spec
TEST_SERVER = $(shell find ${ROOT_DIR}/test -name "*.${TEST_TYPE}.coffee")
# some default opts to keep from choking
TEST_OPTS = "--colors"

all: deploy

test: 
	# to run integraiton tests:
	# make TEST_TYPE=integration test
	# to run specs:
	# make test
	./node_modules/mocha/bin/mocha ${TEST_OPTS} ${TEST_SERVER}

clean:
	#noop

.PHONY: watch test clean 
	



