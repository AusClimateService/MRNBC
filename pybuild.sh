#!/bin/bash
module use /g/data/hh5/public/modules
module load conda/analysis3-unstable

cd fmrnbc
f2py -m module -c mrnbc.f qmm.f stat.f
mv module.cpython-310-x86_64-linux-gnu.so ../