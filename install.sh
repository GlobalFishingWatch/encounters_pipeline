#!/bin/bash

echo "Installing dags"
cp ./encounters_dag.py /dags/encounters_dag.py
echo "Installing post_install.sh"
cp ./post_install.sh /dags/post_install.sh
