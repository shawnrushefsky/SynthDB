#!/bin/bash

sudo apt-get update
sudo apt-get upgrade
sudo apt-get install -y --force-yes software-properties-common build-essential python-dev htop screen git
sudo python gaas_install.py
