#!/bin/bash

# Usage:
# ./updateXml.sh base.xml updates.xml
#
# Updates base.xml with text values of respective nodes from updates.xml
# and prints them on standard output.
#

SCRIPT_PATH=../resources/conf/merge_xml.py

python $SCRIPT_PATH $1 $2
