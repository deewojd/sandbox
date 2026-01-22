# Binary Search Tree Exploration: Iterative Depth-First Search
"""
Time Complexity:
- O(log n) average case (balanced BST)

Space Complexity:
- O(1) average and worst case (iterative, no recursion stack)

Purpose:
Exploring an iterative BST search to avoid recursion stack overhead
while maintaining the same time complexity as the recursive approach.
Note: Iterative BST search along a single path works only when
unnecessary branches can be ignored using BST properties.
For problems requiring all paths or visiting all nodes,
we need full DFS (recursive or iterative with a stack) to explore the tree.
"""

# Create Objects
class Node:
    """Binary Search Tree Node Object"""
    def __init__(self, val):
        self.val = val
        self.left = None
        self.right = None

def build_balanced_bst(sorted_vals):
    """Build a balanced BST from a sorted list to avoid possibility 
    of O(n) time complexity from skewed BST
    Note: List must be sorted for this method to work as intended."""
    if not sorted_vals:
        return None

    mid = len(sorted_vals) // 2
    root = Node(sorted_vals[mid])
    root.left = build_balanced_bst(sorted_vals[:mid])
    root.right = build_balanced_bst(sorted_vals[mid+1:])
    return root

def search_bst_iter(root, target):
    """Iterative BST search along the path from root to target"""
    while root:
        if target == root.val:
            return root
        elif target < root.val:
            root = root.left
        else:
            root = root.right
    return None

# Build BST
values = [50, 30, 70, 20, 40, 60, 80]
values.sort()  # Make list sorted before building a balanced BST
root = build_balanced_bst(values)

# Test iterative BST search
result = search_bst_iter(root, 60)
print(result.val if result else "Not found")


