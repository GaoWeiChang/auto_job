#!/bin/bash
set -e

# remove existing build directory
if [ -d "build" ]; then
    echo "Cleaning old build directory..."
    rm -rf build
fi

# create build directory
mkdir build
cd build

# run cmake
echo ""
echo "Running CMake..."
cmake ..

# compile
echo ""S
echo "Compiling..."

# install
echo ""
echo "Installing..."
sudo make install

# restart postgresql server
echo ""
echo "Restarting Postgresql..."
systemctl restart postgresql

echo ""
echo "=========================================="
echo "Build Complete"
echo "=========================================="