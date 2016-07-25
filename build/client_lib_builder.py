import os
import subprocess

path = os.path.dirname(os.path.abspath(__file__))

dest_dir = os.path.join(path, "synthdb_v1_python")
subprocess.call(['sudo', 'mkdir', dest_dir])
to_move = ['setup.py', 'synthdb.py', 'preqlerrors.py']
for f in to_move:
    subprocess.call(['cp', f, dest_dir])
os.chdir(os.path.dirname(dest_dir))
subprocess.call("tar -zcvf {}.tar.gz {}".format(os.path.basename(dest_dir), os.path.basename(dest_dir)), shell=True)
subprocess.call("sudo rm -r {}".format(dest_dir), shell=True)

