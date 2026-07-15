CXX := g++
FLAGS := -std=c++20 -Iinclude -Wall -Wextra

server:
	$(CXX) $(FLAGS) src/server.cpp -o server

# Usage:
# make run TEST=test/storage_testing.cpp
run:
	@if [ -z "$(TEST)" ]; then \
		echo "Usage: make run TEST=test/<file>.cpp"; \
		exit 1; \
	fi
	$(CXX) $(FLAGS) $(TEST) $(SRC) -o test_bin
	./test_bin

clean:
	rm -f test_bin
