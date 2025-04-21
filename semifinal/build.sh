rm build -r
mkdir build
cd build
cmake -G "Unix Makefiles" -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=. ../src
make