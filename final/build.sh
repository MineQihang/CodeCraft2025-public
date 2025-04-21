rm build -r
mkdir build
cd build
cmake -G "Unix Makefiles" -DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++ -DCMAKE_RUNTIME_OUTPUT_DIRECTORY=. -DCMAKE_C_FLAGS="-Wno-unused-result" -DCMAKE_CXX_FLAGS="-Wno-unused-result" ../src
make
if [ -f "code_craft" ]; then
    mv code_craft code_craft.exe
fi