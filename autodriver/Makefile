CC = gcc
CFLAGS = -W -Wall -Wextra
LDFLAGS =  -pthread

OBJS = autodriver.o

all: autodriver

autodriver: $(OBJS)
	$(CC) $(LDFLAGS) -o autodriver $(OBJS)
	sudo chown root autodriver
	sudo chmod +s autodriver

autodriver.o: autodriver.c

install:
	cp -p autodriver /usr/bin/autodriver

clean:
	rm -f *.o autodriver

.PHONY: clean install
