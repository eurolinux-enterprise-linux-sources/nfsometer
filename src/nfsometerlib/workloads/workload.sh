#!/bin/bash

WORKLOADS_DIR="$(dirname $0)"
WORKLOADS="$(cd $WORKLOADS_DIR && ls *.nfsometer 2>&1 | sed 's/.nfsometer//g')"

usage()
{
	[ -n "$*" ] && echo $* >&2
	echo "usage: $0 <command> [args]" >&2

	echo "XXX" >&2
	exit 1
}

check_rundir() {
	if [ -z "$RUNDIR" -o ! -d "$RUNDIR" ] ; then
		usage "RUNDIR not defined"
	fi
}

check_localdir() {
	if [ -z "$LOCALDIR" -o ! -d "$LOCALDIR" ] ; then
		usage "LOCALDIR not defined"
	fi
}

check_dirs() {
	check_rundir
	check_localdir
}

do_fetch()
{
	_ret=0

	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	cd $LOCALDIR

	if [ -n "$URL" -a -n "$URL_OUT" ] ; then
		if [ ! -f "$URL_OUT" ]; then
			wget -O "$URL_OUT" "$URL"
			_ret=$?

			if [ $_ret -ne 0 ] ; then
				rm -f "$URL_OUT"
			fi
		fi
	fi

	return $_ret
}

do_check()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	cd $LOCALDIR

	workload_check

	return $?
}

do_setup()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	cd $LOCALDIR

	workload_setup

	return $?
}

get_command()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	echo $COMMAND
}

get_description()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	echo $DESCRIPTION
}

get_url()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	echo $URL
}

get_url_out()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	echo $URL_OUT
}

get_name()
{
	W=$1.nfsometer
	source $WORKLOADS_DIR/$W

	if [ -n "$NAME" ]; then
		echo $NAME
	else
		echo $1
	fi
}

need_env()
{
	if [ -z "$(eval "echo \$$(echo $1)")" ] ; then
		echo "env variable '$1' not defined"
	fi
}

need_bin()
{
	if [ -z "$(which $1 2> /dev/null)" ] ; then
		echo "binary '$1' not found"
	fi
}

need_file()
{
	if [ ! -f "$1" ] ; then
		echo "file '$1' not found"
	fi
}

if [ $# -lt 1 ] ; then
	usage
fi

CMD="$1"

if [ "$CMD" = "list" ]; then
	echo $WORKLOADS | sort

elif [ "$CMD" = "check" ]; then
	check_localdir
	if [ $# -ne 2 ] ; then
		usage "check expects one argument <workload>"
	fi
	do_check $2

elif [ "$CMD" = "setup" ]; then
	check_dirs
	if [ $# -ne 2 ] ; then
		usage "setup expects one argument <workload>"
	fi
	do_setup $2

elif [ "$CMD" = "command" ]; then
	#check_dirs
	if [ $# -ne 2 ] ; then
		usage "command expects one argument <workload>"
	fi
	get_command $2

elif [ "$CMD" = "description" ]; then
	#check_dirs
	if [ $# -ne 2 ] ; then
		usage "description expects one argument <workload>"
	fi
	get_description $2

elif [ "$CMD" = "name" ]; then
	#check_dirs
	if [ $# -ne 2 ] ; then
		usage "description expects one argument <workload>"
	fi
	get_name $2

elif [ "$CMD" = "url" ]; then
	check_localdir
	if [ $# -ne 2 ] ; then
		usage "url expects one argument <workload>"
	fi
	get_url $2

elif [ "$CMD" = "url_out" ]; then
	check_localdir
	if [ $# -ne 2 ] ; then
		usage "url_out expects one argument <workload>"
	fi
	get_url_out $2

else
	usage "invalid command: $CMD"
fi

