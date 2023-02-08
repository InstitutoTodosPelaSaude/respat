#!/bin/bash

input=$1

head -n1 $input | tr "\t" "\n" | cat -n