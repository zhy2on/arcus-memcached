#!/bin/sh
find ../ -name "*[cht]" > ./cscope.files
ctags --extras=+q -L ./cscope.files -f ./tags
rm ./cscope.files
