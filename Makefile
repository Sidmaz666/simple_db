# Compiler
CC = gcc

# Compiler flags
CFLAGS = -Wall -Wextra -g

# Source files
SRC = src/main.c src/server/server.c src/server/routes.c

# Output executable
TARGET = main

# Default rule (clean and build)
all: clean $(TARGET)

# Compile the source files into the executable
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC)

# Clean up the compiled files
clean:
	rm -f $(TARGET)

# Rebuild from scratch
rebuild: clean all

# Run the program (clean, rebuild, and run)
run: all
	clear; ./$(TARGET)

