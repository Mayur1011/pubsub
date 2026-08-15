CXX := g++
CXXFLAGS := -std=c++20 -g -Wall -Wextra -Iinclude -fsanitize=thread -pthread
LDFLAGS := -fsanitize=thread -pthread

TARGET := build/server

SRC := $(wildcard src/concurrency/*.cpp) \
       $(wildcard src/net/*.cpp) \
       $(wildcard src/storage/*.cpp)

APP := app/server.cpp

# $(patsubst pattern,replacement,text) -- replace pattern with replacement in text
OBJ := $(patsubst %.cpp,build/%.o,$(SRC)) \
       $(patsubst %.cpp,build/%.o,$(APP))

all: $(TARGET)

$(TARGET): $(OBJ)
	@mkdir -p $(dir $@)
	$(CXX) $(OBJ) -o $@ $(LDFLAGS)

build/%.o: %.cpp
	# create build directory if it doesn't exist. the dir command extract the directory form the currently matched file.
	@mkdir -p $(dir $@)
	# $< means the first prerequisite
	# $@ means the current target
	$(CXX) $(CXXFLAGS) -c $< -o $@

run-server: $(TARGET)
	./$(TARGET)

# ----------------------------------------------------------------------- #
# make run-test TEST=fileName
run-test:
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make run-test TEST=<name>"; \
		echo "Example: make run-test TEST=topicPartition"; \
		exit 1; \
	fi
	@mkdir -p build/tests
	$(CXX) $(CXXFLAGS) test/$(TEST).cpp $(SRC) -o build/tests/$(TEST) $(LDFLAGS)
	./build/tests/$(TEST)

clean:
	rm -rf build

# is to tell make that these targets are not files, but commands to run (who makes such files)
.PHONY: all run-server run-test clean