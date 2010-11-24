bin = simple

all: $(bin)

INC_MG = -I ../src
LIB_MG = -L ../src -lmongodb

%:%.c
	gcc --std=c99 -fPIC -D_GNU_SOURCE -o $@ $< $(INC_MG) $(LIB_MG)

clean:
	@rm -rf $(bin)
