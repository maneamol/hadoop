#!/bin/bash

nl Holmes.txt > nHolmes.txt

tail -1 nHolmes.txt | awk '{print $1}'

typeset -i variable=$(tail -1 nHolmes.txt | awk '{print $1}')

variable=$((variable+1))

 nl -v$variable Ulysses.txt > nUlysses.txt



