import re
import os
import sys

re_insert = r'^INSERT  IGNORE INTO `(\w+)` VALUES \(\d+,'.encode('utf-8')
re_noid = r'INSERT  IGNORE INTO `\1` VALUES (NULL,'.encode('utf-8')

in_file = os.path.basename(sys.argv[1])
out_file = re.sub(r'\.sql', r'-noid.sql', in_file)

with open(out_file, 'xb') as f_out:
    with open(sys.argv[1], 'rb') as f_in:
        for line in f_in:
            f_out.write(re.sub(re_insert, re_noid, line))
        
