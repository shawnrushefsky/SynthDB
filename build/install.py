import platform
import subprocess
import os

pwd = os.path.dirname(os.path.realpath(__file__))

print "Preparing to install SynthDB..."

plat, vers, codename = platform.linux_distribution()


def sudo(*args):
    subprocess.call(['sudo']+list(args))


subprocess.call('echo "deb http://downloads.skewed.de/apt/{} {} universe" | sudo tee -a /etc/apt/sources.list'.format(codename, codename), shell=True)
subprocess.call('echo "deb-src http://downloads.skewed.de/apt/{} {} universe" | sudo tee -a /etc/apt/sources.list'.format(codename, codename), shell=True)
subprocess.call('echo "deb http://download.rethinkdb.com/apt {} main" | sudo tee /etc/apt/sources.list.d/rethinkdb.list'.format(codename), shell=True)
subprocess.call("wget -qO- https://download.rethinkdb.com/apt/pubkey.gpg | sudo apt-key add -", shell=True)
subprocess.call("sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test", shell=True)
apt_install = ['apt-get', "install", '-y', '--force-yes']
subprocess.call("apt-key add {}".format(os.path.join(pwd, 'config', 'gt_key.txt')), shell=True)
# subprocess.call("sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test", shell=True)

print "Updating requirements..."

sudo('apt-get', 'update')
sudo('apt-get', 'upgrade')

packs = ['python-pip', 'python-graph-tool', 'rethinkdb']
pips = ['cython', 'simplejson', 'CherryPy', 'rethinkdb', 'futures', 'dill', 'cloud']

sudo(*apt_install+packs)

sudo(*['pip', 'install']+pips)
sudo('pip', 'install', '--upgrade', 'requests')
sudo('python', 'setup.py', 'install')

rethink_conf = '/etc/rethinkdb/instances.d/synthdb.conf'

sudo('cp', '/etc/rethinkdb/default.conf.sample', rethink_conf)
sudo('/etc/init.d/rethinkdb', 'restart')

print "SynthDB uses RethinkDB as a JSON document store. To configure this component, edit {}".format(rethink_conf)
print "Then, run 'sudo /etc/init.d/rethinkdb restart'"
print
print "To configure the SynthDB server, edit {}".format(os.path.join(pwd, 'config', 'server.conf'))