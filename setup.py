import distutils
from distutils.core import setup

#######
# For later if we need executable scripts
#import glob
#bin_files = glob.glob("bin/*.py") # + glob.glob("bin/*.txt")
#######

# The main call
setup(name='despyDMdb',
      version ='0.1.0',
      license = "GPL",
      description = "common DESDM DB classes/functions",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['despydmdb'],
      package_dir = {'': 'python'},
      #scripts = bin_files,
      #data_files=[('ups',['ups/despyDMdb.table'])], # optional
      )

