cmake_minimum_required(VERSION 3.5)
# Set extension name here
set(TARGET_NAME wvlet)
find_package(OpenSSL REQUIRED)

# Define wvlet library details for both static and dynamic
set(WVLET_STATIC_URL "https://github.com/quackmagic/wvlet-lib/releases/download/nightly/libwvlet.a")
set(WVLET_DYNAMIC_URL "https://github.com/quackmagic/wvlet-lib/releases/download/nightly/libwvlet.so")

set(WVLET_STATIC_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third-party/libwvlet.a")
set(WVLET_DYNAMIC_PATH "${CMAKE_CURRENT_SOURCE_DIR}/third-party/libwvlet.so")

# Function to download library if it doesn't exist
function(download_library url path)
    if(NOT EXISTS ${path})
        message(STATUS "Downloading wvlet library from ${url}")
        file(MAKE_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third-party")
        file(DOWNLOAD
            ${url}
            ${path}
            SHOW_PROGRESS
            STATUS DOWNLOAD_STATUS
            TLS_VERIFY ON)
        list(GET DOWNLOAD_STATUS 0 STATUS_CODE)
        if(NOT STATUS_CODE EQUAL 0)
            message(FATAL_ERROR "Failed to download wvlet library from ${url}")
        endif()
    endif()
endfunction()

# Download both libraries
download_library(${WVLET_STATIC_URL} ${WVLET_STATIC_PATH})
download_library(${WVLET_DYNAMIC_URL} ${WVLET_DYNAMIC_PATH})

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
project(${TARGET_NAME})

include_directories(src/include)
set(EXTENSION_SOURCES src/wvlet_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Create list of libraries in correct order for static linking
set(STATIC_LIBS
    ${WVLET_STATIC_PATH}
    -Wl,--whole-archive
    OpenSSL::Crypto
    OpenSSL::SSL
    -Wl,--no-whole-archive
    -ldl
    -pthread
)

# For static extension, use static library with correct link order
target_link_libraries(${EXTENSION_NAME} 
#    ${WVLET_DYNAMIC_PATH}
    ${STATIC_LIBS}
)

# For loadable extension, use dynamic library
target_link_libraries(${LOADABLE_EXTENSION_NAME} 
    OpenSSL::SSL 
    OpenSSL::Crypto 
    ${WVLET_DYNAMIC_PATH}
)

# Set link flags to ensure proper symbol resolution
set_target_properties(${EXTENSION_NAME} PROPERTIES
    LINK_FLAGS "-Wl,--no-as-needed"
)

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
