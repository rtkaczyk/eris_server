if [ -d "conf" ] && [ -d "src" ]
then
	mkdir -p tmp/src
	cp -a conf tmp/
	cp src/*.py tmp/src/
	cp setenv.sh VERSION tmp/
	mkdir tmp/logs
	mkdir tmp/work
	mkdir tmp/work/feed
	cd tmp && tar -czfErisServer.tgz * && cd ..
	mv tmp/ErisServer.tgz . && rm -rf tmp	
else
	echo "distro.sh cannot be called here"
fi
