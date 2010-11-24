all: src test
	@make -C src
	@make -C test

clean:
	@make -C src clean
	@make -C test clean
