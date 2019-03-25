#!/bin/bash
unzip -p tmp/tmp.zip | grep '^\([^,]*,\)\{4\}6,.*' | sed '$a\\'
# not written in the host program because of the '\'s, which cause errors if written in the host program
# pipe stream usually stripes out the final blank line. 'sed' is for adding a blank line to the end output.
# add a blank line to the end output or fread fails:
## Avoidable 73.574 seconds. This file is very unusual: it ends abruptly without a final newline, and also its size is a multiple of 4096 bytes.
# pipe() doesn't seem to fail for lacking ending blank line.
# if execution fails, check EOL; EOL should be LF
# may use Notepad++ to check & convert EOL
# 'sed -i -e 's/\r$//' <filename>' replaces Windows EOL 'CRLF' by 'LF'
