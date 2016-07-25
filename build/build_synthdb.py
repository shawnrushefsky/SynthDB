import sys
from os.path import join, dirname, basename
from os import chdir
from py_compile import compile
import subprocess

if __name__ == "__main__":
    orig_directory = sys.argv[1]
    dest_dir = sys.argv[2]
    subprocess.call("sudo mkdir {}".format(dest_dir), shell=True)
    to_compile = ['server.py', 'install.py', 'secure_it.py']
    for f in to_compile:
        compile(join(orig_directory, f))
    to_move = [join(orig_directory, f+'c') for f in to_compile]+['install.sh', 'setup.py', 'synthdb.py', 'preqlerrors.py', 'check_in.py']
    for f in to_move:
        subprocess.call("cp {} {}".format(f, dest_dir), shell=True)
    subprocess.call("cp -r {} {}".format(join(orig_directory, 'config'), dest_dir), shell=True)
    chdir(dirname(dest_dir))
    subprocess.call("tar -zcvf {}.tar.gz {}".format(basename(dest_dir), basename(dest_dir)), shell=True)
    subprocess.call("sudo rm -r {}".format(dest_dir), shell=True)