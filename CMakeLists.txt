cmake_minimum_required(VERSION 3.10)
project(auto_job)

# ==========================================
# Find postgesql file path
# ==========================================

# server header files (postgres.h, fmgr.h, etc.)
execute_process(
    COMMAND pg_config --includedir-server
    OUTPUT_VARIABLE PG_INCLUDEDIR_SERVER
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# client header files
execute_process(
    COMMAND pg_config --includedir
    OUTPUT_VARIABLE PG_INCLUDEDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# .so file
execute_process(
    COMMAND pg_config --pkglibdir
    OUTPUT_VARIABLE PG_PKGLIBDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

# .sql and .control file
execute_process(
    COMMAND pg_config --sharedir
    OUTPUT_VARIABLE PG_SHAREDIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)


# ==========================================
# Setting up compiler
# ==========================================

# warning flag
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")

# debug
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -g -O0 -fno-omit-frame-pointer")


# ===============================================
# Source Files
# ===============================================

set(SOURCES
    src/init.c
    src/launcher.c
    src/job.c
)


# ===============================================
# create shared Library (.so)
# ===============================================

add_library(auto_job MODULE ${SOURCES})
target_include_directories(auto_job PRIVATE 
    ${PG_INCLUDEDIR_SERVER}
    ${PG_INCLUDEDIR}
)

set_target_properties(auto_job PROPERTIES PREFIX "") # remove prefix (eg. libauto_job.so -> auto_job.so)

# ===============================================
# Install Targets
# ===============================================

# install .so file
install(TARGETS auto_job
        LIBRARY DESTINATION ${PG_PKGLIBDIR})

# install .control file
install(FILES auto_job.control
        DESTINATION ${PG_SHAREDIR}/extension)

# install .sql files
install(FILES 
        sql/auto_job--1.0.0.sql
        DESTINATION ${PG_SHAREDIR}/extension)