cmake_minimum_required (VERSION 2.6)
project (paqu_jobMon)
###########################################################
#### uncomment this, if you want to specify own mysql path
###########################################################
set(MYSQL_PATH "/usr/local/mariadb")

###########################################################
#### path to the mysql 5.5 sources - download them from
#### www.mysql.com if you don't have them 
###########################################################
set(MYSQL_SOURCES_PATH "/Users/adrian/Downloads/mariadb-5.5.32")

if(MYSQL_PATH)
    set(MYSQL_CONFIG "${MYSQL_PATH}/bin/mysql_config")
else()
    find_file(MYSQL_CONFIG mysql_config)
endif()

execute_process(COMMAND ${MYSQL_CONFIG} --include OUTPUT_VARIABLE MYSQL_INCLUDE_DIR)
execute_process(COMMAND ${MYSQL_CONFIG} --libs OUTPUT_VARIABLE MYSQL_LIBRARIES)
execute_process(COMMAND ${MYSQL_CONFIG} --plugindir OUTPUT_VARIABLE MYSQL_PLUGIN_DIR)

STRING(REGEX REPLACE "\n" "" MYSQL_INCLUDE_DIR ${MYSQL_INCLUDE_DIR})
STRING(REGEX REPLACE "\n" "" MYSQL_LIBRARIES ${MYSQL_LIBRARIES})
STRING(REGEX REPLACE "\n" "" MYSQL_PLUGIN_DIR ${MYSQL_PLUGIN_DIR})

set(CMAKE_BUILD_TYPE Debug)
SET(BUILD_SHARED_LIBS ON)

add_definitions(-DMYSQL_DYNAMIC_PLUGIN)

#read define parameters with which mysql has been compiled. for this we parse mysqlbug
file(READ ${MYSQL_PATH}/bin/mysqlbug mysqlbugContent)
STRING(REGEX REPLACE ".*COMP_RUN_INFO=.*CXXFLAGS='([^']*).*" "\\1" mysqlConfig ${mysqlbugContent})
STRING(REGEX MATCHALL "-D([^ ]*)" defineList "${mysqlConfig}")
message("Found the following defines in your mysqld configuration: ${defineList}")

add_definitions(${defineList})
add_definitions(-DDISABLE_DTRACE)

#add_definitions(-D__JOBMON_DEBUG__)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MYSQL_INCLUDE_DIR} -fPIC -fno-exceptions -fno-rtti")

include_directories ("${PROJECT_SOURCE_DIR}" "${MYSQL_SOURCES_PATH}/include" "${MYSQL_SOURCES_PATH}/mysys" "${MYSQL_SOURCES_PATH}/regex" "${MYSQL_SOURCES_PATH}/sql" "${MYSQL_SOURCES_PATH}")

file(GLOB FILES_SRC "${PROJECT_SOURCE_DIR}/src/*.h" "${PROJECT_SOURCE_DIR}/src/*.cpp" "${PROJECT_SOURCE_DIR}/src/*.cc")

add_library (paqu_jobMon ${FILES_SRC})

#get rid of the lib infront of the target file name
set_target_properties(paqu_jobMon PROPERTIES PREFIX "")

INSTALL(TARGETS paqu_jobMon DESTINATION "${MYSQL_PLUGIN_DIR}")

message("\nFURTHER INSTALLATION INSTRUCTIONS")
message("---------------------------------\n")
message("After executing make - make install, you need to setup the")
message("approperiate mysql system tables and functions. As mysql root")
message("execute the install_qqueue.sql script...\n\n")
