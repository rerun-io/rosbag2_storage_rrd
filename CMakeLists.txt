cmake_minimum_required(VERSION 3.16)
project(rosbag2_storage_rrd)

if(NOT CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
  set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(ament_cmake_python REQUIRED)
find_package(pluginlib REQUIRED)
find_package(rcutils REQUIRED)
find_package(rosbag2_storage REQUIRED)

# RoboStack's rcutils exports `-latomic` unconditionally; macOS clang has no separate
# libatomic (the intrinsics are built in), so linking fails unless a conda stub happens
# to be on the search path. Strip it from the imported interface instead.
if(APPLE AND TARGET rcutils::rcutils)
  get_target_property(_rcutils_link_libs rcutils::rcutils INTERFACE_LINK_LIBRARIES)
  if(_rcutils_link_libs)
    list(REMOVE_ITEM _rcutils_link_libs atomic)
    set_target_properties(rcutils::rcutils PROPERTIES
      INTERFACE_LINK_LIBRARIES "${_rcutils_link_libs}")
  endif()
endif()

# --- Rust core ---------------------------------------------------------------
#
# Everything that touches CDR, Arrow or Rerun lives in a Rust staticlib linked into the
# plugin below. A staticlib rather than a cdylib means there is no second artifact to
# install and no macOS rpath to fix up, and it keeps the Rerun SDK to a single linkage.

find_program(CARGO_EXECUTABLE cargo REQUIRED)

set(RRD_CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo")
set(RRD_FFI_LIB "${RRD_CARGO_TARGET_DIR}/release/librosbag2_storage_rrd_ffi.a")

# Rebuild whenever any source or the resolved dependency set changes. Listing files by
# hand is what left the previous plugin silently stale after edits.
file(GLOB_RECURSE RRD_RUST_SOURCES CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/crates/*/src/*.rs")

add_custom_command(
  OUTPUT ${RRD_FFI_LIB}
  COMMAND ${CMAKE_COMMAND} -E env "CARGO_TARGET_DIR=${RRD_CARGO_TARGET_DIR}"
          ${CARGO_EXECUTABLE} build --release --package rosbag2_storage_rrd_ffi
          --manifest-path "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
  DEPENDS ${RRD_RUST_SOURCES}
          "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
          "${CMAKE_CURRENT_SOURCE_DIR}/crates/rosbag2_storage_rrd_ffi/Cargo.toml"
          "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.lock"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building the rrd storage plugin's Rust core"
  VERBATIM
)
add_custom_target(rrd_ffi_build DEPENDS ${RRD_FFI_LIB})

add_library(rrd_ffi STATIC IMPORTED GLOBAL)
set_target_properties(rrd_ffi PROPERTIES IMPORTED_LOCATION ${RRD_FFI_LIB})
add_dependencies(rrd_ffi rrd_ffi_build)

# What the Rust standard library and the Rerun SDK need from the host.
if(APPLE)
  set_property(TARGET rrd_ffi PROPERTY INTERFACE_LINK_LIBRARIES
    "-framework CoreFoundation" "-framework IOKit" "-framework Security")
elseif(UNIX)
  set_property(TARGET rrd_ffi PROPERTY INTERFACE_LINK_LIBRARIES m dl pthread)
endif()

# --- Plugin ------------------------------------------------------------------

add_library(${PROJECT_NAME} SHARED cpp/rrd_storage.cpp)

# rosbag2_storage 0.33 (lyrical) added the pure-virtual `write_message` /
# `write_messages` pair to the write interface; older distros do not have them.
if(rosbag2_storage_VERSION VERSION_GREATER_EQUAL "0.33")
  target_compile_definitions(${PROJECT_NAME} PRIVATE RRD_HAS_WRITE_MESSAGES=1)
endif()

target_include_directories(${PROJECT_NAME} PRIVATE
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

target_link_libraries(${PROJECT_NAME} PRIVATE
  pluginlib::pluginlib
  rcutils::rcutils
  rosbag2_storage::rosbag2_storage
  rrd_ffi
)

pluginlib_export_plugin_description_file(rosbag2_storage plugin_description.xml)

install(
  TARGETS ${PROJECT_NAME}
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)

# Makes `--storage-preset-profile` discoverable from `ros2 bag record -s rrd --help`.
ament_python_install_package(ros2bag_rrd_cli)

if(BUILD_TESTING)
  find_package(ament_cmake_gtest REQUIRED)
  # The round-trip tests build real serialized messages, which needs rclcpp and a message
  # package. The plugin itself needs neither.
  find_package(rclcpp REQUIRED)
  find_package(std_msgs REQUIRED)
  # RoboStack's rclcpp export drags in the `*__rosidl_generator_py` libraries, which
  # expect libpython symbols from the host process; provide them to the test binary.
  find_package(Python3 REQUIRED COMPONENTS Development)

  ament_add_gtest(test_rrd_storage test/test_rrd_storage.cpp)
  if(TARGET test_rrd_storage)
    target_link_libraries(test_rrd_storage
      pluginlib::pluginlib
      rclcpp::rclcpp
      rosbag2_storage::rosbag2_storage
      std_msgs::std_msgs__rosidl_typesupport_cpp
      Python3::Python
    )
  endif()
endif()

ament_package()
