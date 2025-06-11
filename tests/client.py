import os
import sys # add .. to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import rquote


a = rquote.get_price('usIXIC')
print(a)