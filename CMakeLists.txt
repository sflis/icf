cmake_minimum_required(VERSION 3.11...3.16)

project(icf VERSION 1.0 LANGUAGES CXX)
set(LIBTARGET ${PROJECT_NAME})
set(PYTARGET ${PROJECT_NAME}_py)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)

# Save executables to bin directory
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Dependencies
find_package(pybind11 REQUIRED)
# find_package(Doxygen)

# if(Doxygen_FOUND)
#     add_subdirectory(docs)
# else()
#     message(STATUS "Doxygen not found, not building docs")
# endif()

include(CTest)

# src
# set(HEADER_LIST include/sstcam/interfaces/WaveformDataPacket.h include/sstcam/interfaces/Waveform.h)
add_library(${LIBTARGET} SHARED src/icfFile.cc)
target_include_directories(${LIBTARGET} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/>
    $<INSTALL_INTERFACE:include/>
    )
target_compile_features(${LIBTARGET} PUBLIC cxx_std_11)
install (TARGETS ${LIBTARGET} EXPORT icf-file-targets LIBRARY DESTINATION lib)

# Makes it easier for IDEs to find all headers
source_group(TREE "${PROJECT_SOURCE_DIR}/include" PREFIX "Header Files" FILES ${HEADER_LIST})

# pybind
pybind11_add_module(${PYTARGET} pybind/module.cc pybind/icfFile.cc)
target_link_libraries(${PYTARGET} PRIVATE ${LIBTARGET})
install(TARGETS ${PYTARGET} LIBRARY DESTINATION ${PYTHON_SITE_PACKAGES})


add_executable(test_icf ctests/test_icf.cc)# $<TARGET_OBJECTS:test_main>)
target_link_libraries(test_icf ${LIBTARGET})

# # ctests
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

configure_file(
        ${CMAKE_SOURCE_DIR}/env_shell.sh.in
        ${CMAKE_BINARY_DIR}/env_shell.sh
        @ONLY
    )