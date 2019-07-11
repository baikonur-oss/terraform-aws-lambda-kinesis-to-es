#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

echo "[1/5] Activating venv"

if [[ ! -f venv/bin/activate ]]; then
    echo "Could not find venv! Create a venv and try again."
    exit -1
fi

source venv/bin/activate

echo "[2/5] Cleaning cache"
rm -rf build
rm -rf lambda_package.zip

echo "[3/5] Creating local copy"
mkdir -p build
cp lambda/*.py build/
cp lambda/requirements-deploy.txt build/

echo "[4/5] pip install"
cd build
docker run --rm -v $(pwd):/root/p python:3.7 pip3 install -r /root/p/requirements-deploy.txt -t /root/p/ > /dev/null

echo "[5/5] Compiling and making zip package"
python -m compileall .
zip -r9 ../lambda_package.zip ./ -x ".*" > /dev/null
cd ..

echo "Finished!"
