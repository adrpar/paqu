GENERAL NOTE:
-------------

These tools only work with MySQL 5.5 and above. In order to use them 
and compile them, you need the complete source code of the MySQL installation
you are using. If you are using an installation from a Linux distribution
obtain the source code from the distribution. Another option is to compile
MySQL yourself. Instructions (sketchy ones at most) are given below for
ubuntu. If you are using the source of your distribution, you can skip
this part.

PaQu - INTRODUCTION:
--------------------

PaQu is a parallel querying facility that works together with the MySQL Spider
engine. It is built on a similar project using Gearman 
(shard-query: http://code.google.com/p/shard-query/) and has been forked from there
at an early stage. It has been heavily extended and altered to suite our needs.

The general idea of PaQu is to take a query, analyse it and automatically shard/parallelise
it to be run on the different Spider nodes. This is achieved in a map/reduce kind of
fashions. Results are then gathered on the head node and further reduced if aggregation is
needed.

To have an idea how to use PaQu, have a look at paraQuery.php until I find the time to
write a detailed introduction on how to use PaQu. Or use the Daiquiri web framework
(escience.aip.de/daiquiri) to interface to your Spider database from the web using PaQu.


In order to use PaQu you need to compile the following MySQL extensions (see below for instructions
or the README in the respective directories):

- mysql_node_jobMon: a deamon to be installed on the Spider nodes to monitor the head node. This is
                     needed to detect any job kill on the head node, killing the associated job on the
                     Spider node

- mysql_udf: some UDFs needed by PaQu to aggregate STDEV queries

- (optional) direct_paraQuery: this UDF will execute PaQu from within MySQL using libPhpEmbed to call
                               PHP code from within MySQL. Decide for yourself if this is a good idea.
                               To compile, have a look at build.sh and alter accordingly. Also alter the
                               path in udf_paraQuery.cc for finding PaQu. If this is functionality is
                               realy needed, let me know and I will improve the code...

KNOWN LIMITATIONS:
------------------

- Sorting is not necessarily done in parallel - ORDER BYs are pushed to the nodes and executed
  there, however anohter sort is required on the head node. On large tables this is highly
  inefficient. One would need to implement some kind of merge sort in MySQL on the head node
  to achieve the desired behaviour...


HOW TO COMPILE MYSQL FROM SCRATCH:
----------------------------------

Legal prerequisite:
We donot take any responisibilty for wrongly configured and compiled
server versions. IT IS YOUR RESPONIBILITY TO THINK, read the documentation
and decide what you need and how you need it. Only the simplest instructions
are given below and I highly recommend reading a 
book/blog/how-to/tutorial/forum on how to securely compile MySQL.

Software prerequistes:
build-essential
cmake
cmake-gui
libncurses5-dev



Installation instructions:

We will assume, that you already installed MySQL using the package management
system and you want to have your installation besides the official one.

1) Download MySQL source code (Source code - tar.gz version, not RPM)

2) Untar in directory of your convenience:

tar -xf mysql-5.5.28.tar.gz

3) cd mysql-5.5.28

4) mkdir build
   cd build
   cmake-gui ..

5) press configure and walk through wizard (Set generator to Makefile and 
   use native compiler)

6) after configure has finished, adjust the settings in the table. You
   might want to change: CMAKE_INSTALL_PREFIX, MYSQL_DATADIR, 
   WITH_FEDERATED_STORAGE_ENGINE (make sure PARTITIONED is enabled)

7) press configure again and click on generate and close cmake-gui

8) make 
   sudo make install

9) copy package my.cnf file to new location and adjust to new setup.
   especially change port and paths to socket, pid, data, logs to become
   independant of any other mysql installation

   sudo cp /etc/mysql/my.cnf /etc/mysql/my2.cnf
   emacs /etc/mysql/my2.cnf (adjust port, paths and such)

10) Copy and adjust Upstart/ini.d script
    (adjust env HOME if needed, replace my.cnf with my2.cnf, all
     refeneces to the binary files). Specify the new my2.cnf config
     file in the execuptabel (eg:
     
     exec /opt/mysql/bin/mysqld --defaults-file="${HOME}"/my2.cnf

     )

    sudo cp /etc/init/mysql.conf /etc/init/mysql2.conf
    sudo emacs /etc/init/mysql2.conf

11) setup mysql grant table (change paths if needed):
   
    /opt/mysql/scripts/mysql_install_db --basedir=/opt/mysql --datadir=/usr/local/mysql/data

12) sudo start mysql2

if things dont work, look at the log file. check if all directories (especially
the ones for the data and logs) are there. Check the priviledges, these need
to belong to the mysql user and group (in general 

chown -R mysql:mysql /foo/bar

)


COMPILING ADDONS AND PLUGINS FROM PAQU
--------------------------------------

At the moment, the following projects are usable:
mysql_query_queue, mysql_validateSQL

Each of these projects can be compiled and installed in a similar fashion.
The only requirement is, that cmake is available and the source code of
the used MySQL server is present. These tools require heavily on the original
source code.

1) cd into directory

2) mkdir build
   cd build

3) edit the CMakeLists.txt file, to properly refere to the source path of the
   MySQL server

   emacs ../CMakeLists.txt

   set MYSQL_PATH and MYSQL_SOURCES_PATH to the location of the MySQL 
   installation directory and the MySQL sources respectively.

4) cmake ..

5) make
   sudo make install

   (it might be, that mysql messes up the location of where to look for
    the plugins. On ubuntu you might need to copy from where "make install"
    installed the .so file, to /usr/lib/mysql/plugin...

    no clue yet how to change that path...)

6) install UDF/Plugin in MySQL server using install_FOO.sql

   (as root copy commands over into mysql prompt)

