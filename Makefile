CC = gcc
CFLAGS = -w

# Targets for building
all: assign3 expr

assign3: buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o expr.o record_mgr.o rm_serializer.o test_assign3_1.o
	$(CC) $(CFLAGS) -o test_assign3_1 $^

expr: buffer_mgr.o buffer_mgr_stat.o dberror.o storage_mgr.o expr.o record_mgr.o rm_serializer.o test_expr.o
	$(CC) $(CFLAGS) -o test_expr $^

# Object files
%.o: %.c
	$(CC) $(CFLAGS) -c $<

# Valgrind targets
valgrind_assign3: assign3
	valgrind --leak-check=full --track-origins=yes ./test_assign3_1

valgrind_expr: expr
	valgrind --leak-check=full --track-origins=yes ./test_expr

# Clean target
clean:
	$(RM) test_assign3_1
	$(RM) test_expr
	$(RM) *.o
