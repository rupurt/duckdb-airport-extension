cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME airport)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(Arrow REQUIRED)
find_package(ArrowFlight REQUIRED)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES src/airport_extension.cpp src/airport_flight_stream.cpp)


build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

#target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

target_link_libraries(${EXTENSION_NAME}
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  gRPC::grpc++
)

#target_link_libraries(${LOADABLE_TARGET_NAME} PRIVATE Arrow::arrow_shared ArrowFlight::arrow_flight_shared)

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${LOADABLE_EXTENSION_NAME}
  Arrow::arrow_static
  ArrowFlight::arrow_flight_static
  gRPC::grpc++
)


install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
