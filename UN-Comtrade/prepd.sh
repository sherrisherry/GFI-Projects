#!/bin/bash
unzip -p tmp/tmp.zip | grep '^\([^,]*,\)\{4\}6,.*' | sed '$a\\'
