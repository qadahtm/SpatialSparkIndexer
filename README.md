# Simple File Streaming Server
A simple streaming server that streams lines of text from data files. 

## Deploying a pre-built distribution
From the /dist directory, download one of the archive files and unpack. 
Configure the application.conf file by setting the appropriate values for hostname, port, and the path to the directory containg the data files. 
After that, run the following:
 ```sh
 $ bin/Webserver
 ```
 
 Assuming the following configuration:
 ```
 webserver {
	hostname = "localhost",
	port = 9990,
	data {		
		dir="path/to/data"
	}
}
 ```
 
 This will start the webserver with empty list of streams. In the browser, open `http://localhost:9990/streamadmin.htm`
 
## Build from Souce
### Mac/Linux
The project comes with an sbt launcher script. With this script, you don't need to have sbt installed. First, clone the git repo, using the following command: 
```sh
$ git clone https://github.com/qadahtm/sfsserver
```
Next, build the project using:
```sh
$ ./sbt pack
```
Scripts for launching the webserver will be avaible in `./target/pack/bin` directory.
### Windows
You will need sbt installed. Refer to the following url: http://www.scala-sbt.org/release/tutorial/Installing-sbt-on-Windows.html# SpatialSparkIndexer
