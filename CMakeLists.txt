cmake_minimum_required(VERSION 3.11...3.16)

project(icf VERSION 1.0 LANGUAGES CXX)
option(PYTHON_SETUP "If invoked by setup.py" OFF)
set(LIBTARGET ${PROJECT_NAME})
set(PYTARGET ${PROJECT_NAME}_py)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(EXISTS $ENV{CONDA_PREFIX})
  set(CMAKE_INSTALL_PREFIX $ENV{CONDA_PREFIX})

endif()


if((APPLE) OR (EXISTS $ENV{CONDA_PREFIX}))
  # The following settings were copied from
  # https://cmake.org/Wiki/CMake_RPATH_handling
  # to avoid the rpath issue (issue #11998) that appears on OS X El Capitan
  # https://forge.in2p3.fr/issues/11998

  # use, i.e. don't skip the full RPATH for the build tree
  set(CMAKE_SKIP_BUILD_RPATH  FALSE)

  # when building, don't use the install RPATH already
  # (but later on when installing)
  if(PYTHON_SETUP)
    set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
  else()
    set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
  endif()

  set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")

  # add the automatically determined parts of the RPATH
  # which point to directories outside the build tree to the install RPATH
  set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
endif()

find_package(Git QUIET)
# find_package(Python3 COMPONENTS Interpreter)
find_package(pybind11 QUIET)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
include(CTest)
include(ExternalProject)

# Save executables to bin directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Dependencies
include(FetchContent)

if(NOT pybind11_FOUND)
  FetchContent_Declare(
      pybind11
      GIT_REPOSITORY https://github.com/pybind/pybind11
      GIT_TAG        v2.4.3
      GIT_SHALLOW TRUE
  )

  FetchContent_GetProperties(pybind11)
  if(NOT pybind11_POPULATED)
      FetchContent_Populate(pybind11)
      add_subdirectory(${pybind11_SOURCE_DIR} ${pybind11_BINARY_DIR})
  endif()
endif()

if(NOT PYTHON_SETUP)
  message(STATUS "${Cyan}doctest${ColourReset}")
  FetchContent_Declare(
      doctest
      GIT_REPOSITORY https://github.com/onqtam/doctest
      GIT_TAG        2.3.6
      GIT_SHALLOW TRUE
  )
  FetchContent_GetProperties(doctest)
  if(NOT doctest_POPULATED)
      FetchContent_Populate(doctest)
      set(DOCTEST_WITH_TESTS OFF CACHE BOOL "No tests of doctests")
      set(DOCTEST_NO_INSTALL ON CACHE BOOL "No doctest install")
      add_subdirectory(${doctest_SOURCE_DIR} ${doctest_BINARY_DIR})
  endif()
  set(DOCTEST_INCLUDE_DIR ${doctest_SOURCE_DIR}/doctest CACHE INTERNAL "Path to include folder for doctest")
endif()


# src
add_library(${LIBTARGET} SHARED src/icfFile.cc)
target_include_directories(${LIBTARGET} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>
    $<INSTALL_INTERFACE:include/>
    )
target_compile_features(${LIBTARGET} PUBLIC cxx_std_11)

if(PYTHON_SETUP AND NOT EXISTS $ENV{CONDA_PREFIX})
  set_target_properties(${LIBTARGET}
      PROPERTIES
        PREFIX ""
        LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/icf/_ext/"
  )
else()
  install (TARGETS ${LIBTARGET} EXPORT icf-file-targets LIBRARY DESTINATION lib)
  install (DIRECTORY ${PROJECT_SOURCE_DIR}/include/ DESTINATION include)
endif()
# Makes it easier for IDEs to find all headers
source_group(TREE "${PROJECT_SOURCE_DIR}/include" PREFIX "Header Files" FILES ${HEADER_LIST})

#Get git version number
execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=7
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                OUTPUT_VARIABLE VERSION
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)
set(ICF_VERSION_LONG ${VERSION})
#parse the version information into pieces.
string(REGEX REPLACE "^v([0-9]+)\\..*" "\\1" VERSION_MAJOR "${VERSION}")
string(REGEX REPLACE "^v[0-9]+\\.([0-9]+).*" "\\1" VERSION_MINOR "${VERSION}")
string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.([0-9]+).*" "\\1" VERSION_PATCH "${VERSION}")
string(REGEX REPLACE "^v[0-9]+\\.[0-9]+\\.[0-9]+(.*)" "\\1" VERSION_SHA1 "${VERSION}")
set(ICF_VERSION "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")

configure_file( version_config.h.in ${CMAKE_BINARY_DIR}/generated/version_config.h )
include_directories( ${CMAKE_BINARY_DIR}/generated/)


# pybind
# set(PỲTARGET_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/icf/_ext/")
# message(${PYTARGET_OUTPUT_DIR})
pybind11_add_module(${PYTARGET} pybind/module.cc pybind/icfFile.cc)
target_link_libraries(${PYTARGET} PRIVATE ${LIBTARGET})
set_target_properties(${PYTARGET}
    PROPERTIES
      PREFIX ""
      LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/icf/_ext/"
)
if(NOT PYTHON_SETUP)
# Python stuff
# Creating a symlink to the python package
add_custom_command(TARGET ${PYTARGET} POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E create_symlink "${PROJECT_SOURCE_DIR}/icf/" "${CMAKE_CURRENT_BINARY_DIR}/icf/_icf")
# Python stuff
# Creating a symlink to the python tests
add_custom_command(TARGET ${PYTARGET} POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E create_symlink "${PROJECT_SOURCE_DIR}/pytest/" "${CMAKE_CURRENT_BINARY_DIR}/pytest")


if(Python3_Interpreter_FOUND)
  add_test(NAME PythonTests COMMAND python3 -m pytest -r a -v WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/pytest")
endif()


  configure_file(setup.py.in "${PROJECT_BINARY_DIR}/setup.py")


# Place the initialization file in the output directory for the Python
# bindings. This will simplify the installation.
CONFIGURE_FILE(__init__.py
    ${CMAKE_CURRENT_BINARY_DIR}/icf/__init__.py
)


add_executable(test_icf ctests/test_icf.cc)# $<TARGET_OBJECTS:test_main>)
target_link_libraries(test_icf ${LIBTARGET})


# ctests
add_library(test_main OBJECT ctests/test_main.cc)
target_include_directories(test_main PUBLIC  ${DOCTEST_INCLUDE_DIR})

add_executable(test_ICFFile ctests/ICFFile.cc $<TARGET_OBJECTS:test_main> )
add_test(NAME test_ICFFile COMMAND test_ICFFile)
target_link_libraries(test_ICFFile ${LIBTARGET})
target_include_directories(test_ICFFile PUBLIC ctests ${DOCTEST_INCLUDE_DIR})
target_compile_features(test_ICFFile PRIVATE cxx_std_11)

endif()