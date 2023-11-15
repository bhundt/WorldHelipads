#!/bin/bash
cp  ../data/export/*.csv .
zip world-helipad-data.zip *.csv README.md
rm *.csv