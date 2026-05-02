# ==============================================================
#  Makefile  –  Mini In-Memory DBMS
#
#  Usage:
#    make          Build the dbms binary (incremental)
#    make clean    Remove all build artifacts and the binary
#    make rebuild  Clean then full rebuild from scratch
#    make run      Build and launch the interactive CLI
#
#  How separate compilation works
#  --------------------------------
#  Instead of recompiling every .c file on every change, each
#  .c file is first compiled to its own .o (object) file inside
#  the build/ directory.  The linker then combines all .o files
#  into the final binary.  If only one .c file changes, only
#  that one .o is recompiled — saving time on large projects.
#
#    src/record.c  ──►  build/record.o  ─┐
#    src/table.c   ──►  build/table.o   ─┤
#    src/hashtable.c►  build/hashtable.o─┤──► dbms
#    src/bptree.c  ──►  build/bptree.o  ─┤
#    src/parser.c  ──►  build/parser.o  ─┤
#    src/executor.c──►  build/executor.o─┤
#    main.c        ──►  build/main.o    ─┘
# ==============================================================

# --- Toolchain ---
CC       = gcc
CFLAGS   = -Wall -Wextra -std=c11   # strict warnings + C11 standard
IFLAGS   = -Iinclude                 # header search path

# --- Targets & Directories ---
TARGET   = dbms
BUILDDIR = build

# --- Source & Object files ---
# Collect every .c under src/ plus main.c
SRCS     = src/record.c    \
           src/table.c     \
           src/hashtable.c \
           src/bptree.c    \
           src/parser.c    \
           src/storage.c      \
           src/optimizer.c    \
           src/transaction.c  \
           src/schema.c       \
           src/executor.c  \
           main.c

# Derive object file paths: src/foo.c -> build/foo.o
# notdir strips the directory prefix, then we prepend build/
OBJS     = $(patsubst %.c, $(BUILDDIR)/%.o, $(notdir $(SRCS)))

# --- Default target: build the binary ---
all: $(BUILDDIR) $(TARGET)

# --- Link all object files into the final binary ---
$(TARGET): $(OBJS)
	@echo "[link]    $^  ->  $@"
	$(CC) $(CFLAGS) -o $@ $^
	@echo ""
	@echo "  Build complete ->  ./$(TARGET)"
	@echo "  Run with:          ./$(TARGET)"
	@echo "  Clean with:        make clean"
	@echo ""

# --- Compile each .c in src/ to build/<name>.o ---
$(BUILDDIR)/%.o: src/%.c
	@echo "[compile] $<"
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

# --- Compile main.c to build/main.o ---
$(BUILDDIR)/main.o: main.c
	@echo "[compile] $<"
	$(CC) $(CFLAGS) $(IFLAGS) -c $< -o $@

# --- Create the build directory if it does not exist ---
$(BUILDDIR):
	mkdir -p $(BUILDDIR)

# --- Remove all generated files ---
clean:
	@echo "[clean]   removing $(BUILDDIR)/ and $(TARGET)"
	rm -rf $(BUILDDIR) $(TARGET)
	@echo "[clean]   done"

# --- Full rebuild from scratch ---
rebuild: clean all

# --- Build and immediately launch the CLI ---
run: all
	./$(TARGET)

# --- Non-file targets (prevents conflicts with files of same name) ---
.PHONY: all clean rebuild run