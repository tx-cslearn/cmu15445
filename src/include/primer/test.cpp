#include <iostream>
#include <ostream>
#include <string>
#include "p0_trie.h"

auto main() -> int {
  bustub::Trie trie;
  trie.Insert("abc", "d");
  bool success = true;
  auto val = trie.GetValue<std::string>("abc", &success);
  std::cout << "=============" << std::endl;
  if (success) {
    std::cout << "true" << std::endl;
  }
  if (val == "d") {
    std::cout << "d" << std::endl;
  }
  return 0;
}