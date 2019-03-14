SERVICE = indexrunner
SERVICE_CAPS = IndexRunner
SPEC_FILE = IndexRunner.spec
URL = https://kbase.us/services/indexrunner
DIR = $(shell pwd)
LIB_DIR = lib
SCRIPTS_DIR = scripts
TEST_DIR = test
LBIN_DIR = bin
WORK_DIR = /kb/module/work/tmp
EXECUTABLE_SCRIPT_NAME = run_$(SERVICE_CAPS)_async_job.sh
STARTUP_SCRIPT_NAME = start_server.sh
TEST_SCRIPT_NAME = run_tests.sh

.PHONY: test

default: compile

all: compile build build-startup-script build-executable-script build-test-script

docker:
	docker build -t kbase/indexrunner .

mock:
	docker build -t mock_indexer ./test/mock_indexer

test:
	nosetests -s -x -v --with-coverage --cover-erase --cover-package=IndexRunner --cover-html --cover-html-dir=./test_coverage --nocapture  --nologcapture .


clean:
	rm -rfv $(LBIN_DIR)
