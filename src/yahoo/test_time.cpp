#include <chrono>
#include <cstdint>
#include <iostream>

long int timeSinceEpochMillisec() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

int main() {
  std::cout << timeSinceEpochMillisec() << std::endl;
  return 0;
}