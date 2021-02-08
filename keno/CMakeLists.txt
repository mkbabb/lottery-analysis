project(keno)
cmake_minimum_required(VERSION 3.2)

set(CMAKE_CXX_STANDARD 20)
add_compile_options(-Wall -Wextra)

set(CMAKE_BUILD_TYPE "Debug")

include(${CMAKE_CURRENT_SOURCE_DIR}/conanbuildinfo.cmake)

conan_basic_setup()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(${PROJECT_NAME} include/tmp.cpp)
target_link_libraries(${PROJECT_NAME} ${CONAN_LIBS})




