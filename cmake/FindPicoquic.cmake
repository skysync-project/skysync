# # Enable fetching content
# include(FetchContent)

# # Fetch the picoquic library, which brings in picotls with it
# FetchContent_Declare(
#     picoquic
#     GIT_REPOSITORY  https://gitee.com/zhihaogit/picoquic
#     GIT_TAG         master
# )

# # Set the option to force picoquic to fetch the picotls
# set(PICOQUIC_FETCH_PTLS ON)

# # Make dependencies available
# FetchContent_MakeAvailable(picoquic)

find_path(Picoquic_INCLUDE_DIR
    NAMES picoquic.h
    HINTS ${CMAKE_SOURCE_DIR}/../picoquic/picoquic
          ${CMAKE_BINARY_DIR}/../picoquic/picoquic
          ../picotls/picoquic/ )

find_path(Picoquic_BINLOG_DIR
    NAMES autoqlog.h
    HINTS ${CMAKE_SOURCE_DIR}/../picoquic/loglib
          ${CMAKE_BINARY_DIR}/../picoquic/loglib
          ../picotls/picoquic/ )

set(Picoquic_HINTS ${CMAKE_BINARY_DIR}/../picoquic ../picoquic)

find_library(Picoquic_CORE_LIBRARY picoquic-core HINTS ${Picoquic_HINTS})
find_library(Picoquic_LOG_LIBRARY picoquic-log HINTS ${Picoquic_HINTS})

include(FindPackageHandleStandardArgs)
# handle the QUIETLY and REQUIRED arguments and set Picoquic_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args(Picoquic REQUIRED_VARS
    Picoquic_CORE_LIBRARY
    Picoquic_INCLUDE_DIR)

if(Picoquic_FOUND)
    set(Picoquic_LIBRARIES
        ${Picoquic_CORE_LIBRARY}
        ${Picoquic_LOG_LIBRARY})
    set(Picoquic_INCLUDE_DIRS
        ${Picoquic_INCLUDE_DIR}
        ${Picoquic_BINLOG_DIR})
endif()

mark_as_advanced(Picoquic_LIBRARIES Picoquic_INCLUDE_DIRS)