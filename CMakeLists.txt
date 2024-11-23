cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME wvlet)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

# Define wvlet library details
set(WVLET_LIB_URL "https://github.com/quackmagic/wvlet-lib/releases/download/v2024.9.12/libwvlet_linux-x64.so")
set(WVLET_LIB_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third-party/libwvlet_linux-x64.so")

# Download wvlet library if it doesn't exist
if(NOT EXISTS ${WVLET_LIB_PATH})
    message(STATUS "Downloading wvlet library from ${WVLET_LIB_URL}")
    # Create third-party directory if it doesn't exist
    file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third-party")
    # Download the file
    file(DOWNLOAD
        ${WVLET_LIB_URL}
        ${WVLET_LIB_PATH}
        SHOW_PROGRESS
        STATUS DOWNLOAD_STATUS
        TLS_VERIFY ON)

    list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
    if(NOT STATUS_CODE EQUAL 0)
        message(FATAL_ERROR "Failed to download wvlet library")
    endif()

endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/wvlet_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${WVLET_LIB_PATH})
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto ${WVLET_LIB_PATH})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
