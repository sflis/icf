cmake_minimum_required(VERSION 3.11...3.16)

project(icf VERSION 1.0 LANGUAGES CXX)
set(LIBTARGET ${PROJECT_NAME})
set(PYTARGET ${PROJECT_NAME}_py)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Git QUIET)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Save executables to bin directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Dependencies
find_package(pybind11 REQUIRED)

include(CTest)

include(ExternalProject)
ExternalProject_Add(
    doctest
    PREFIX ${CMAKE_BINARY_DIR}/doctest
    GIT_REPOSITORY https://github.com/onqtam/doctest.git
    TIMEOUT 10
    UPDATE_COMMAND ${GIT_EXECUTABLE} pull
    GIT_SHALLOW 1
    CONFIGURE_COMMAND ""
    BUILD_COMMAND ""
    INSTALL_COMMAND ""
    LOG_DOWNLOAD ON
)

# Expose required variable (DOCTEST_INCLUDE_DIR) to parent scope
ExternalProject_Get_Property(doctest source_dir)
set(DOCTEST_INCLUDE_DIR ${source_dir}/doctest CACHE INTERNAL "Path to include folder for doctest")


# src
add_library(${LIBTARGET} SHARED src/icfFile.cc)
target_include_directories(${LIBTARGET} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>
    $<INSTALL_INTERFACE:include/>
    )
target_compile_features(${LIBTARGET} PUBLIC cxx_std_11)
install (TARGETS ${LIBTARGET} EXPORT icf-file-targets LIBRARY DESTINATION lib)

# Makes it easier for IDEs to find all headers
source_group(TREE "${PROJECT_SOURCE_DIR}/include" PREFIX "Header Files" FILES ${HEADER_LIST})

#Get git version number
execute_process(COMMAND ${GIT_EXECUTABLE} describe --tags --abbrev=7
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
                OUTPUT_VARIABLE ICF_VERSION_LIB
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)
configure_file( version_config.h.in ${CMAKE_BINARY_DIR}/generated/version_config.h )
include_directories( ${CMAKE_BINARY_DIR}/generated/)


# pybind
pybind11_add_module(${PYTARGET} pybind/module.cc pybind/icfFile.cc)
target_link_libraries(${PYTARGET} PRIVATE ${LIBTARGET})
set_target_properties(${PYTARGET}
    PROPERTIES
      PREFIX ""
      LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/icf/_ext/"
)

# Python stuff
# Creating a symlink to the python package
add_custom_command(TARGET ${PYTARGET} POST_BUILD
                  COMMAND ${CMAKE_COMMAND} -E create_symlink "${PROJECT_SOURCE_DIR}/icf/" "${CMAKE_CURRENT_BINARY_DIR}/icf/_icf")
# Generating the correct setup.py file
file(GENERATE OUTPUT setup.py.in INPUT setup.py.in.in)
configure_file(render-variables.cmake.in render-variables.cmake @ONLY)
add_custom_command(
    OUTPUT "${PROJECT_BINARY_DIR}/setup.py"
    COMMAND "${CMAKE_COMMAND}"
        -DPROJECT_BINARY_DIR="${PROJECT_BINARY_DIR}"
        -DPROJECT_NAME="${PROJECT_NAME}"
        -DICF_VERSION_LIB="${ICF_VERSION_LIB}"

        -P "${PROJECT_BINARY_DIR}/render-variables.cmake"
    MAIN_DEPENDENCY "${PROJECT_BINARY_DIR}/setup.py.in"
    DEPENDS "${PROJECT_BINARY_DIR}/render-variables.cmake"
)
add_custom_target(
    make-setupy.py ALL
    DEPENDS "${PROJECT_BINARY_DIR}/setup.py"
)


configure_file(
        ${CMAKE_SOURCE_DIR}/env_shell.sh.in
        ${CMAKE_BINARY_DIR}/env_shell.sh
        @ONLY
    )

# Place the initialization file in the output directory for the Python
# bindings. This will simplify the installation.
CONFIGURE_FILE(__init__.py
    ${CMAKE_CURRENT_BINARY_DIR}/icf/__init__.py
)


add_executable(test_icf ctests/test_icf.cc)# $<TARGET_OBJECTS:test_main>)
target_link_libraries(test_icf ${LIBTARGET})



# # ctests
# ctests
add_library(test_main OBJECT ctests/test_main.cc)
target_include_directories(test_main PUBLIC  ${DOCTEST_INCLUDE_DIR})
add_executable(test_ICFFile ctests/ICFFile.cc $<TARGET_OBJECTS:test_main> )
add_test(NAME test_ICFFile COMMAND test_ICFFile)
target_link_libraries(test_ICFFile ${LIBTARGET})
target_include_directories(test_ICFFile PUBLIC ctests ${DOCTEST_INCLUDE_DIR})
target_compile_features(test_ICFFile PRIVATE cxx_std_11)

# add_library(test_main OBJECT ctests/test_main.cc)



# add_executable(test_WaveformDataPacket ctests/WaveformDataPacket.cc $<TARGET_OBJECTS:test_main>)
# add_test(NAME test_WaveformDataPacket COMMAND test_WaveformDataPacket)
# target_link_libraries(test_WaveformDataPacket ${LIBTARGET})
# target_include_directories(test_WaveformDataPacket PUBLIC ctests)
# target_compile_features(test_WaveformDataPacket PRIVATE cxx_std_11)

# add_executable(test_Waveform ctests/Waveform.cc $<TARGET_OBJECTS:test_main>)
# add_test(NAME test_Waveform COMMAND test_Waveform)
# target_link_libraries(test_Waveform ${LIBTARGET})
# target_include_directories(test_Waveform PUBLIC ctests)
# target_compile_features(test_Waveform PRIVATE cxx_std_11)

# # data files
# file(GLOB DATA_FILES "${CMAKE_CURRENT_SOURCE_DIR}/share/sstcam/interfaces/*")
# file(COPY ${DATA_FILES} DESTINATION ${CMAKE_BINARY_DIR}/share/sstcam/interfaces/)
# install(DIRECTORY "${CMAKE_BINARY_DIR}/share" DESTINATION .)

# # Compilation options
# target_compile_options(${LIBTARGET} PUBLIC -O2 -Wall -pedantic -Werror -Wextra)


# # Python stuff
# # Creating a symlink to the python package
# add_custom_command(TARGET ${PYTARGET} POST_BUILD
#                   COMMAND ${CMAKE_COMMAND} -E create_symlink "${PROJECT_SOURCE_DIR}/python/" "${PYTHON_PACKAGE_PATH}/interfaces")
# # Creating a symlink to the python extension in the main python package directory tree
# add_custom_command(TARGET ${PYTARGET} POST_BUILD COMMAND ${CMAKE_COMMAND} -E create_symlink "$<TARGET_FILE:${PYTARGET}>" "${PYTHON_EXTENSIONS_PATH}/${PYTARGET}")

