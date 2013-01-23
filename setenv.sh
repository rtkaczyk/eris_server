if [ -d "conf" ] && [ -d "work" ] && [ -d "logs" ] && [ -d "src" ]
then
	export ERIS_BASEDIR=`pwd`
	export PYTHONPATH=`pwd`/src:${PYTHONPATH}
	alias eris='python -meris'
else
	echo "setenv.sh cannot be called here"
fi
