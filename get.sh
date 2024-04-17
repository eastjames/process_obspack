#!/bin/bash
data_link=""
mkdir -p data
cd data
wget ${data_link}
unzip *.nc.zip
