from distutils.core import setup
import pip

pip.main(['install', 'rethinkdb', 'simplejson', 'cloud'])
pip.main(['install', '--upgrade', 'requests'])

setup(name="synthdb",
      version="1.0",
      py_modules=["synthdb", "preqlerrors"],
      author="Psymphonic",
      description="The python client library for interacting with SynthDB",
      author_email='shawn@psymphonic.com',
      url='http://www.psymphonic.com'
      )
