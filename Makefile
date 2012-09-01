ROOT_DIR = .
BUILD_DIR = ./build/ui
BUILDJS = build.js
MAKE = make
VENDORJS =  ${ROOT_DIR}/public/vendor/js/*
# use subshell to pull filelist for testing
TEST_TYPE = spec
TEST_SERVER = $(shell find ${ROOT_DIR}/test -name "*.${TEST_TYPE}.coffee")
# some default opts to keep from choking
TEST_OPTS = "--colors"

all: deploy

main.js: clean

deploy: main.js package-ui

package-ui: clean main.js
	rm -rf ${ROOT_DIR}/public/package
	cp -rp ${BUILD_DIR}/ ${ROOT_DIR}/public/package
	# TODO consider http://upstart.ubuntu.com/

test: test-server
test-server:
	# to run integraiton tests:
	# make TEST_TYPE=integration test
	# to run specs:
	# make test
	./node_modules/mocha/bin/mocha ${TEST_OPTS} ${TEST_SERVER}

clean:
	rm -rf ${BUILD_DIR}/build	

.PHONY: watch test clean 
	



