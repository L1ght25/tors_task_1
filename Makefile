all: master worker

master: master.c
	gcc -o master master.c -lpthread

worker: worker.c
	gcc -o worker worker.c -lpthread

clean:
	rm -f master worker
