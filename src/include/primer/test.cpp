#include "p0_trie.h"

auto main() -> int {
  auto t = bustub::TrieNode('a');
  auto child_node = t.InsertChildNode('b', std::make_unique<bustub::TrieNode>('b'));

  printf("%d\n", static_cast<int>(t.HasChildren()));
  printf("%c", child_node->get()->GetKeyChar());
  return 0;
}