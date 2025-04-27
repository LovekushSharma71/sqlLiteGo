BINARY_NAME = sqlClone

SRC_FILES = main.go statement.go table.go

all: build

build:
	go build -o $(BINARY_NAME) $(SRC_FILES)

run: build
	./$(BINARY_NAME)

clean:
	rm -f $(BINARY_NAME)

.PHONY: all build run clean
