function(fusedebug _varname)
	if(FUSE_DEBUG)
		message("${_varname} = ${${_varname}}")
	endif(FUSE_DEBUG)
endfunction(fusedebug)

find_package(PkgConfig)

pkg_check_modules(Libfuse REQUIRED fuse)

if (Libfuse_FOUND)
	fusedebug(Libfuse_LIBRARIES)
	fusedebug(Libfuse_LIBRARY_DIRS)
	fusedebug(Libfuse_LDFLAGS)
	fusedebug(Libfuse_LDFLAGS_OTHER)
	fusedebug(Libfuse_INCLUDE_DIRS)
	fusedebug(Libfuse_CFLAGS)
	fusedebug(Libfuse_CFLAGS_OTHER)
endif(Libfuse_FOUND)


find_PATH(LIBFUSE_ROOT
	NAMES include/fuse.h
)

find_PATH(Libfuse_INCLUDE_DIR
	NAME fuse.h
	HINTS ${LIBFUSE_ROOT}/include
)

find_library(Libfuse_LIBRARY
	NAMES fuse libfuse 
	HINTS ${HINT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LIBFUSE DEFAULT_MSG
	Libfuse_INCLUDE_DIR
	Libfuse_LIBRARY
)

set(Libfuse_DEFINITIONS ${Libfuse_CFLAGS_OTHER})

mark_as_advanced(LIBFUSE_ROOT
	Libfuse_INCLUDE_DIR
	Libfuse_LIBRARY
)
