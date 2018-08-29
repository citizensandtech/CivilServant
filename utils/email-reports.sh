#!/usr/bin/env bash

if [ -z $CS_ENV ]
    then
    echo "trying to source environment variables."
    source ../config/environment_variables.sh
    if [ -z $CS_ENV ]
        then
        echo "couldn't get CS_ENV out of the script, exting"
        exit 1
        else
        echo "Found CS_ENV from script"
    fi
fi

echo "Running with CS_ENV=$CS_ENV"

if [ $CS_ENV = "development" ]
    then
    envpython=$1
    basedir=$2
    else if [ $CS_ENV = "production" ]
    then
    envpython="/home/dmca/dmca/dmca/bin/python"
    basedir="/home/dmca/CivilServant-mmou-twitter/utils"
    fi
fi

$envpython $basedir"/email_db_report.py"
$envpython $basedir"/email_log_report.py"
