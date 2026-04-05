#pragma once

#include <postgres.h>


// launcher background worker entry point 
extern void launcher_main(Datum main_arg);