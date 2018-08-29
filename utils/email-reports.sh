#!/usr/bin/env bash

if [ -z $1 ]
    then
    envpython="/home/dmca/dmca/dmca/bin/python"
    echo "Using default python of"$envpython
    else
    envpython=$1
fi


if [ -z $2 ]
    then
    basedir="/home/dmca/CivilServant-mmou-twitter"
    echo "trying to source environment variables from"$basedir
    else
    basedir=$2
fi

if [ -z $3 ]
    then
    export CS_ENV=production
    else
    export CS_ENV=$3
fi

echo "Running with CS_ENV=$CS_ENV"

$envpython $basedir"utils/email_db_report.py"
$envpython $basedir"utils/email_log_report.py"
