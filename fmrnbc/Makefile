
EXECUTABLE=mrnbc

FC=gfortran
#FCFLAGS=-Wall -c -Wno-tabs -Wextra
FCFLAGS=-c -Wno-tabs
#FCFLAGS=-Wtabs -Wextra
SOURCES=mrnbc.f qmm.f stat.f
CONFIGS=data.inc dimension.inc temp.inc
OBJECTS=$(SOURCES:.f=.o)

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS) 
	$(FC) $(OBJECTS) -o $@

.f.o: 
	$(FC) $(FCFLAGS) $< 



