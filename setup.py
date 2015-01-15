import distutils
from distutils.core import setup

# The main call
setup(name='despyDMdb',
      version ='0.1.0',
      license = "GPL",
      description = "common DESDM DB classes/functions",
      author = "Michelle Gower",
      author_email = "mgower@illinois.edu",
      packages = ['despydmdb'],
      package_dir = {'': 'python'},
      )

