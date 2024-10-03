package main

import (
	"fmt"
	"os"

	"github.com/pkg/errors"
)

// TODO: Have a external buffer system, where any new page read is placed and then, when all operation for that page are completed, that pgae is updated to the disk
// This ^ will allow to, not have to re-read the same page from the disk, just for the sake for an updated copy in the memory, which we currently are doing a lot.

// SEARCH OPERATION

func Search(key uint32, root_id uint32, file_header *FileHeaderPage, file *os.File) ([]byte, bool, error) {

	if root_id == 0 {
		return nil, false, nil
	}

	pt, _, np, _, err := ReadPage(file, root_id)
	if err != nil {
		return nil, false, err
	}
	if pt != Page_type_ids["Node"] {
		return nil, false, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", root_id, pt))
	}

	// Binary Search to find the position in which this key should be kept.
	ind, inArr := binary_index_node(np.Blocks[:np.Block_size], 0, int(np.Block_size), key)
	if !inArr || ind >= int(np.Block_size) {
		if ind < int(np.Block_size)+1 {
			return Search(key, np.Children[ind], file_header, file)
		} else {
			return nil, false, errors.New(fmt.Sprintf("got index of key in nodepage to be %v which is more than even the number of children of the node %v", ind, np.Block_size+1))
		}
	}

	data, found, err := Read_from_NodePage(root_id, key, file_header, file)
	if err != nil {
		// TODO: This is where the error is occuring, data is literally not there where the offset leads to
		return nil, false, errors.Wrap(err, fmt.Sprintf("error while trying to retrieve key %v data from the nodepage %v", key, np.Data_page_id))
	}
	if !found {
		return nil, false, errors.Wrap(err, fmt.Sprintf("error while trying to retrieve key %v data from the datapage %v", key, np.Data_page_id))
	}

	return data, true, nil
}

// INSERT OPERATION

func split(node_id uint32, file_header *FileHeaderPage, file *os.File) (uint32, []byte, uint32, error) {
	/*
		INPUT:
			1. node_id: page id of the node which is needs to be split (IMP: This node's Block_size == MAX_DEGREE)
			2. file_header
			3. file
		OUTPUT:
			1. uint32 [push_to_top_key]  => the key of the data which needs to be pushed up
			2. []byte [push_to_top_data] =>, associated data
			3. uint32 [new_node_id] 	 => page id of the new NodePage created in the file
			4. error
	*/

	pt, _, node, _, err := ReadPage(file, node_id)
	if err != nil {
		return 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}

	if node.Block_size < uint16(MAX_DEGREE) {
		return 0, nil, 0, errors.New(fmt.Sprintf("read nodepage %v isn't full (block_size = %v), and so doesn't need splitting", node_id, node.Block_size))
	}

	var mid uint32
	var new_node *NodePage
	var new_node_id uint32
	var i uint32 = 0

	// Make a new NodePage
	new_node_id, err = MakeNewPage(Page_type_ids["Node"], file_header, file)
	if err != nil {
		return 0, nil, 0, err
	}
	// Read the new NodePage
	pt, _, _, _, err = ReadPage(file, new_node_id)
	if err != nil {
		return 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", new_node_id, pt))
	}

	// Save the mid value which will be pushed to the top layers
	mid = uint32(MAX_DEGREE) / 2
	push_to_top_key := node.Blocks[mid].Key
	push_to_top_data, data_found, err := Read_from_NodePage(node_id, push_to_top_key, file_header, file)
	if err != nil {
		return 0, nil, 0, errors.Wrap(err, fmt.Sprintf("couldn't read the key %v data from the nodepage %v", push_to_top_key, node_id))
	}
	if !data_found {
		return 0, nil, 0, errors.New(fmt.Sprintf("read nodepage %v didn't have the key %v", node_id, push_to_top_key))
	}

	// Move the later half of node to the new_node [BLOCKS]
	for i = mid + 1; i < uint32(MAX_DEGREE); i++ {
		data, foundKey, err := Read_from_NodePage(node_id, node.Blocks[i].Key, file_header, file)
		if err != nil {
			return 0, nil, 0, errors.Wrap(err, fmt.Sprintf("couldn't read the data of key %v from the nodepage %v", node.Blocks[i].Key, node_id))
		}
		if !foundKey {
			return 0, nil, 0, errors.Wrap(err, fmt.Sprintf("couldn't find the key %v from the nodepage %v", node.Blocks[i].Key, node_id))
		}
		err = Put_in_NodePage(new_node_id, node.Blocks[i].Key, data, node.Children[i+1], false, file_header, file)
		if err != nil {
			return 0, nil, 0, errors.Wrap(err, fmt.Sprintf("couldn't put the key %v into the nodepage %v", node.Blocks[i].Key, new_node_id))
		}
		err = Delete_in_NodePage(node_id, node.Blocks[i].Key, false, file_header, file)
		if err != nil {
			return 0, nil, 0, errors.Wrap(err, fmt.Sprintf("couldn't delete the key %v into the nodepage %v", node.Blocks[i].Key, new_node_id))
		}
	}

	// Moving the right-most child from node to new_node
	pt, _, node, _, err = ReadPage(file, node_id)
	if err != nil {
		return 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}
	pt, _, new_node, _, err = ReadPage(file, new_node_id)
	if err != nil {
		return 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", new_node_id, pt))
	}
	new_node.Children[0] = node.Children[mid+1]
	node.Children[mid+1] = 0

	// Save the node and the new_node
	err = SavePage(node_id, Data_to_Bytes(node), file_header, file)
	if err != nil {
		return 0, nil, 0, err
	}
	err = SavePage(new_node_id, Data_to_Bytes(new_node), file_header, file)
	if err != nil {
		return 0, nil, 0, err
	}

	// Delete the `push_to_top_key` key from the node
	err = Delete_in_NodePage(node_id, push_to_top_key, false, file_header, file)
	if err != nil {
		return 0, nil, 0, err
	}

	// err = Defragment_Node(node_id, file_header, file)
	// if err != nil {
	// 	return push_to_top_key, push_to_top_data, new_node_id, err
	// }

	return push_to_top_key, push_to_top_data, new_node_id, nil
}

func insert_helper(node_id uint32, key uint32, data []byte, file_header *FileHeaderPage, file *os.File) (uint32, bool, uint32, []byte, uint32, error) {
	/*
		INPUT:
			1. Current root of the B-Tree
			2. `key` integer to insert in the B-Tree
			3. `data` associated data with the key
		OUTPUT:
			1. uint32 [new_root_id] => The new root of the B-Tree
			2. bool [is_overflow] => True, if overflow occured while inserting in the B-Tree, (signalling splitting has occured)
			3. uint32 [pushed_from_bottom_key] => The integer which the current needs to accomodate since the lower layers are full.
			4. []byte [pushed_from_bottom_data] => Associated with the data
			5. uint32 [new_node_id] => New right node created from splitting at bottom level (This needs to be adjusted in the current node)
			6. error
	*/

	pt, _, node, _, err := ReadPage(file, node_id)
	if err != nil {
		return 0, false, 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, false, 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}

	// Check if there are any children
	if node.Children[0] == 0 { // In a B-Tree there will either be children for all blocks or for none, since a B-Tree is always balanced
		// This is the leaf node
		_, inArr := binary_index_node(node.Blocks[:node.Block_size], 0, int(node.Block_size), key)
		if inArr {
			return node_id, false, 0, nil, 0, nil
		}

		err = Put_in_NodePage(node_id, key, data, 0, false, file_header, file) // Will also update the `node *NodePage`
		if err != nil {
			return 0, false, 0, nil, 0, err
		}

		// Read the node again, since its updated now...
		pt, _, node, _, err = ReadPage(file, node_id)
		if err != nil {
			return 0, false, 0, nil, 0, err
		}
		if pt != Page_type_ids["Node"] {
			return 0, false, 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
		}

		if int(node.Block_size) == MAX_DEGREE {
			// Overflow has occured in the node, so the node needs to be split
			push_to_top_key, push_to_top_data, new_node_id, err := split(node_id, file_header, file)
			if err != nil {
				return node_id, true, push_to_top_key, push_to_top_data, new_node_id, errors.Wrap(err, fmt.Sprintf("error while trying to split the nodepage %v", node_id))
			}
			return node_id, true, push_to_top_key, push_to_top_data, new_node_id, nil
		}

		return node_id, false, 0, nil, 0, nil
	}

	// Now, we need to find the correct child to do recursion on
	ind, inArr := binary_index_node(node.Blocks[:node.Block_size], 0, int(node.Block_size), key)
	if inArr {
		return node_id, false, 0, nil, 0, nil
	}

	_, is_overflow, pushed_from_bottom_key, pushed_from_bottom_data, new_node_id, err := insert_helper(node.Children[ind], key, data, file_header, file)
	if err != nil {
		return node_id, false, 0, nil, 0, errors.Wrap(err, fmt.Sprintf("error while trying to find the child node to put key %v in", key))
	}

	if !is_overflow {
		return node_id, false, 0, nil, 0, nil
	}

	err = Put_in_NodePage(node_id, pushed_from_bottom_key, pushed_from_bottom_data, new_node_id, false, file_header, file) // Will also update the `node *NodePage`
	if err != nil {
		return 0, false, 0, nil, 0, err
	}

	// Read the node again, since its updated now...
	pt, _, node, _, err = ReadPage(file, node_id)
	if err != nil {
		return 0, false, 0, nil, 0, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, false, 0, nil, 0, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}

	if int(node.Block_size) == MAX_DEGREE {
		// Overflow has occured in the node, so the node needs to be split
		push_to_top_key, push_to_top_data, new_node_id, err := split(node_id, file_header, file)
		if err != nil {
			return node_id, true, push_to_top_key, push_to_top_data, new_node_id, errors.Wrap(err, fmt.Sprintf("error while trying to split the nodepage %v", node_id))
		}
		return node_id, true, push_to_top_key, push_to_top_data, new_node_id, nil
	}

	return node_id, false, 0, nil, 0, nil
}

func Insert(key uint32, data []byte, file_header *FileHeaderPage, file *os.File) error {

	earlier_value := file_header.Total_data_size

	var err error

	if file_header.Root_node_id == 0 {
		file_header.Root_node_id, err = MakeNewPage(Page_type_ids["Node"], file_header, file)
		if err != nil {
			return err
		}
	}

	_, is_overflow, pushed_from_bottom_key, pushed_from_bottom_data, new_node_id, err := insert_helper(file_header.Root_node_id, key, data, file_header, file)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to find the child node to put key %v in", key))
	}

	if !is_overflow {
		file_header.Total_data_size = earlier_value + uint64(len(data))
		return nil
	}

	// There is an overflow for the exsting root node of the B-Tree, so make a new NodePage which will our new root of the B-tree
	new_root_id, err := MakeNewPage(Page_type_ids["Node"], file_header, file)
	if err != nil {
		return err
	}

	// Putting the current root as the first child of the new root
	err = Put_in_NodePage(new_root_id, pushed_from_bottom_key, pushed_from_bottom_data, file_header.Root_node_id, true, file_header, file)
	if err != nil {
		return err
	}

	// Now, we also need to put the new_node NodePage as one of the children of the new_root
	pt, _, new_root, _, err := ReadPage(file, new_root_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", new_root_id, pt))
	}

	new_root.Children[1] = new_node_id
	file_header.Root_node_id = new_root_id

	err = SavePage(new_root_id, Data_to_Bytes(new_root), file_header, file)
	if err != nil {
		return err
	}

	file_header.Total_data_size = earlier_value + uint64(len(data))
	return nil
}

// DELETE OPERATION

func merge_helper(left_node_id uint32, right_node_id uint32, file_header *FileHeaderPage, file *os.File) error {

	pt, _, left_node, _, err := ReadPage(file, left_node_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", left_node_id, pt))
	}
	pt, _, right_node, _, err := ReadPage(file, right_node_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", right_node_id, pt))
	}

	var right_key uint32
	var right_data []byte
	var found_data bool

	i := int(left_node.Block_size)
	j := 0
	for i < MAX_DEGREE-1 && j < int(right_node.Block_size) {
		right_key = right_node.Blocks[j].Key
		right_data, found_data, err = Read_from_NodePage(right_node_id, right_key, file_header, file)
		if err != nil {
			return err
		}
		if !found_data {
			return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", right_key, right_node_id))
		}
		if j == 0 {
			err = Put_in_NodePage(left_node_id, right_key, right_data, right_node.Children[j], false, file_header, file)
		} else {
			err = Put_in_NodePage(left_node_id, right_key, right_data, right_node.Children[j], true, file_header, file)
		}
		// file_header.Total_data_size -= uint64(len(right_data))
		if err != nil {
			return err
		}
		i++
		j++
	}
	pt, _, left_node, _, err = ReadPage(file, left_node_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", left_node_id, pt))
	}
	left_node.Children[left_node.Block_size] = right_node.Children[right_node.Block_size]

	err = DeletePage(right_node_id, file_header, file)
	if err != nil {
		return err
	}

	err = SavePage(left_node_id, Data_to_Bytes(left_node), file_header, file)
	if err != nil {
		return err
	}

	return nil
}

func merge(node_id uint32, ind int, file_header *FileHeaderPage, file *os.File) error {
	/*
		4 Possible Cases:
			1. The child node has more than `min_block_size` elements.
			2. The sibling node on right has more than `min_block_size` elems.
			3. The sibling node on right does not have more than `min_block_size` elems. But, the left does.
			4. Neither have more than `min_block_size` elems.
				4.1. Left Sibling exists
				4.2. Right Sibling exists
		[Other sibling nodes except for just right and just left are not useful (in this scenario)]
	*/

	// Read node
	pt, _, node, _, err := ReadPage(file, node_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}
	// Read Child node
	pt, _, child_of_node_1, _, err := ReadPage(file, node.Children[ind])
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node.Children[ind], pt))
	}

	// Case 1
	if child_of_node_1.Block_size >= uint16(min_block_size) {
		return nil
	}

	// Case 2
	if ind+1 < int(node.Block_size)+1 {
		pt, _, child_of_node_2, _, err := ReadPage(file, node.Children[ind+1])
		if err != nil {
			return err
		}
		if pt != Page_type_ids["Node"] {
			return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node.Children[ind], pt))
		}
		if child_of_node_2.Block_size > uint16(min_block_size) {
			// Right sibling exists and has more than `min_block_size` elements
			right_child_id := node.Children[ind+1]
			left_child_id := node.Children[ind]
			right_child_key := child_of_node_2.Blocks[0].Key
			right_child_left_child := child_of_node_2.Children[0]
			right_child_data, found_data, err := Read_from_NodePage(right_child_id, right_child_key, file_header, file)
			if err != nil {
				return err
			}
			if !found_data {
				return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", right_child_key, right_child_id))
			}

			// Take the 0th element from the right child
			err = Delete_in_NodePage(right_child_id, right_child_key, true, file_header, file)
			if err != nil {
				return err
			}

			// Put the right child 0th element in the node at `ind` position
			node_key := node.Blocks[ind].Key
			node_data, found_data, err := Read_from_NodePage(node_id, node_key, file_header, file)
			if err != nil {
				return err
			}
			if !found_data {
				return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", node_key, node_id))
			}
			err = Delete_in_NodePage(node_id, node_key, false, file_header, file)
			if err != nil {
				return err
			}
			err = Put_in_NodePage(node_id, right_child_key, right_child_data, right_child_id, false, file_header, file)
			if err != nil {
				return err
			}

			// Put the `ind` element of node into `ind` child
			err = Put_in_NodePage(left_child_id, node_key, node_data, right_child_left_child, false, file_header, file)
			if err != nil {
				return nil
			}

			return nil
		}
	}

	// Case 3
	if ind-1 > -1 {
		pt, _, child_of_node_2, _, err := ReadPage(file, node.Children[ind-1])
		if err != nil {
			return err
		}
		if pt != Page_type_ids["Node"] {
			return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node.Children[ind], pt))
		}
		if child_of_node_2.Block_size > uint16(min_block_size) {
			// Left sibling exists and has more than `min_block_size` elements
			left_child_id := node.Children[ind-1]
			right_child_id := node.Children[ind]
			left_child_key := child_of_node_2.Blocks[child_of_node_2.Block_size-1].Key
			left_child_right_child := child_of_node_2.Children[child_of_node_2.Block_size]
			left_child_data, found_data, err := Read_from_NodePage(left_child_id, left_child_key, file_header, file)
			if err != nil {
				return err
			}
			if !found_data {
				return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", left_child_key, left_child_id))
			}

			// Take the last element from the left child
			err = Delete_in_NodePage(left_child_id, left_child_key, false, file_header, file)
			if err != nil {
				return err
			}

			// Put the left child's last element in the node at `ind` position
			node_key := node.Blocks[ind-1].Key
			node_data, found_data, err := Read_from_NodePage(node_id, node_key, file_header, file)
			if err != nil {
				return err
			}
			if !found_data {
				return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", node_key, node_id))
			}
			err = Delete_in_NodePage(node_id, node_key, true, file_header, file)
			if err != nil {
				return err
			}
			err = Put_in_NodePage(node_id, left_child_key, left_child_data, left_child_id, true, file_header, file)
			if err != nil {
				return err
			}

			// Put the `ind` element of node into `ind` child
			err = Put_in_NodePage(right_child_id, node_key, node_data, left_child_right_child, true, file_header, file)
			if err != nil {
				return nil
			}

			return nil
		}
	}

	// Case 4 [Neither left nor right sibling have extra elems to spare. So, we possibly merge them (if they exist)]
	// Case 4.1 [Left Sibling exist]
	if ind-1 > -1 {
		// Left Sibling exists
		focus_child_id := node.Children[ind]
		left_child_id := node.Children[ind-1]

		// Insert block `ind-1` of node in the focus child node
		node_key := node.Blocks[ind-1].Key
		node_data, found_data, err := Read_from_NodePage(node_id, node_key, file_header, file)
		if err != nil {
			return err
		}
		if !found_data {
			return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", node_id, node_key))
		}
		err = Put_in_NodePage(focus_child_id, node_key, node_data, 0, true, file_header, file)
		if err != nil {
			return err
		}

		// Merge focus child and left child
		err = merge_helper(left_child_id, focus_child_id, file_header, file)
		if err != nil {
			return err
		}

		// Delete `ind-1` block from the node
		err = Delete_in_NodePage(node_id, node_key, false, file_header, file)
		if err != nil {
			return err
		}

		return nil
	}
	// Case 4.2 [Right Sibling exist]
	if ind+1 < int(node.Block_size)+1 {
		// Right Sibling exists
		focus_child_id := node.Children[ind]
		right_child_id := node.Children[ind+1]

		// Insert block `ind` of node in the focus child node
		node_key := node.Blocks[ind].Key
		node_data, found_data, err := Read_from_NodePage(node_id, node_key, file_header, file)
		if err != nil {
			return err
		}
		if !found_data {
			return errors.New(fmt.Sprintf("coudn't find key %v in the nodepage %v", node_id, node_key))
		}
		err = Put_in_NodePage(right_child_id, node_key, node_data, 0, true, file_header, file)
		if err != nil {
			return err
		}

		// Merge focus child and left child
		err = merge_helper(focus_child_id, right_child_id, file_header, file)
		if err != nil {
			return err
		}

		// Delete `ind-1` block from the node
		err = Delete_in_NodePage(node_id, node_key, false, file_header, file)
		if err != nil {
			return err
		}

		return nil
	}

	return nil
}

func find_leftmost(node_id uint32, file_header *FileHeaderPage, file *os.File) (uint32, []byte, error) {

	if node_id == 0 {
		return 0, nil, nil
	}

	pt, _, node, _, err := ReadPage(file, node_id)
	if err != nil {
		return 0, nil, err
	}
	if pt != Page_type_ids["Node"] {
		return 0, nil, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}

	if node.Children[0] == 0 { // This is a leaf node
		data, found, err := Read_from_NodePage(node_id, node.Blocks[0].Key, file_header, file)
		if err != nil {
			return 0, nil, err
		}
		if !found {
			return 0, nil, err
		}
		return node.Blocks[0].Key, data, nil
	}
	return find_leftmost(node.Children[0], file_header, file)
}

func delete_helper(node_id uint32, key uint32, file_header *FileHeaderPage, file *os.File) (int, error) {
	/*
		INPUT:
			1. The root of the B-Tree in which the element could be present
			2. The element which needs to be deleted from the B-Tree
		OUTPUT:
			1. An integer which represents a code, which is used for internal function use to tell what case are we working with.
				-> -1 = The element was not found
				->  0 = The element was found and deleted wih no problems
				->  1 = The deletion has occured but now the node below has less number of elems and thus needs to be merged with sibling.
	*/
	if node_id == 0 {
		return -1, nil
	}

	pt, _, node, _, err := ReadPage(file, node_id)
	if err != nil {
		return -1, err
	}
	if pt != Page_type_ids["Node"] {
		return -1, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
	}

	ind, inArr := binary_index_node(node.Blocks[:], 0, int(node.Block_size), key)

	if inArr { // This node contains the element we want to delete
		if node.Children[0] == 0 {
			// This is a leaf node
			err = Delete_in_NodePage(node_id, key, false, file_header, file)
			if err != nil {
				return 1, errors.Wrap(err, fmt.Sprintf("error while trying to delete key %v from the nodepage %v", key, node_id))
			}
			if node.Block_size-1 < uint16(min_block_size) {
				return 1, nil // This leaf node has less elements than we need and so, merging will be required
			}
			return 0, nil
		}

		// This is an internal node
		replace_key, replace_data, err := find_leftmost(node.Children[ind+1], file_header, file)
		if err != nil {
			return -1, errors.Wrap(err, fmt.Sprintf("error while trying to find the leftmost element in the right subtree of the nodepage %v, with child index %v", node_id, ind+1))
		}

		right_subtree := node.Children[ind+1]
		err = Delete_in_NodePage(node_id, node.Blocks[ind].Key, false, file_header, file)
		if err != nil {
			return -1, err
		}
		err = Put_in_NodePage(node_id, replace_key, replace_data, right_subtree, false, file_header, file)
		if err != nil {
			return -1, err
		}

		code, err := delete_helper(node.Children[ind+1], replace_key, file_header, file)
		if err != nil || code == -1 {
			return -1, errors.Wrap(err, fmt.Sprintf("error while trying to delete the leftmost element of the right subtree of the nodepage %v at child index %v", node_id, ind+1))
		}
		if code == 1 {
			err = merge(node_id, ind+1, file_header, file)
			if err != nil {
				return -1, err
			}
			pt, _, node, _, err := ReadPage(file, node_id)
			if err != nil {
				return -1, err
			}
			if pt != Page_type_ids["Node"] {
				return -1, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
			}
			if int(node.Block_size) < min_block_size {
				return 3, nil
			}
		}
		return 0, nil
	}

	// The element maybe in one of the leaf nodes of the current node
	code, err := delete_helper(node.Children[ind], key, file_header, file)
	if err != nil {
		return -1, err
	}

	if code == -1 {
		// Element was not found in the children
		return -1, nil
	}
	if code == 0 {
		// The element was found and deleted wih no problems
		return 0, nil
	}
	if code == 1 {
		// Element WAS in the leaf node (which is just below this node)
		err = merge(node_id, ind, file_header, file)
		if err != nil {
			return -1, err
		}
		pt, _, node, _, err := ReadPage(file, node_id)
		if err != nil {
			return -1, err
		}
		if pt != Page_type_ids["Node"] {
			return -1, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
		}
		if int(node.Block_size) < min_block_size {
			return 3, nil
		}
		return 0, nil
	}

	return 0, nil
}

func Delete(key uint32, file_header *FileHeaderPage, file *os.File) error {

	data, found, err := Search(key, file_header.Root_node_id, file_header, file)
	if err != nil {
		return err
	}
	if !found {
		return errors.New(fmt.Sprintf("error while trying to find data for the key %v in the b-tree", key))
	}
	earlier_value := file_header.Total_data_size

	code, err := delete_helper(file_header.Root_node_id, key, file_header, file)
	if err != nil {
		return err
	}
	if code == -1 {
		return errors.New(fmt.Sprintf("coudn't find key %v in the b-tree with root id %v", key, file_header.Root_node_id))
	}

	if code == 1 {
		pt, _, root_node, _, err := ReadPage(file, file_header.Root_node_id)
		if err != nil {
			return err
		}
		if pt != Page_type_ids["Node"] {
			return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", file_header.Root_node_id, pt))
		}

		if root_node.Block_size == 0 {
			err = DeletePage(file_header.Root_node_id, file_header, file)
			if err != nil {
				return err
			}
			file_header.Root_node_id = root_node.Children[0]
		}
		file_header.Total_data_size = earlier_value - uint64(len(data))
		return nil
	}

	file_header.Total_data_size = earlier_value - uint64(len(data))
	return nil
}

// TESTING

// func postorder(node_id uint32, want_expanded_output bool, file_header *FileHeaderPage, file *os.File) error {
// 	var err error
// 	if node_id != 0 {
// 		err = Visualize_Page(node_id, file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 		pt, _, node, _, err := ReadPage(file, node_id)
// 		if err != nil {
// 			return err
// 		}
// 		if pt != Page_type_ids["Node"] {
// 			return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
// 		}
// 		if want_expanded_output {
// 			err = Visualize_Page(node.Data_page_id, file_header, file)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		for i := 0; i < int(node.Block_size)+1; i++ {
// 			err = postorder(node.Children[i], want_expanded_output, file_header, file)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		fmt.Println()
// 	}
// 	return nil
// }

// func shuffleSlice(slice []int) {
// 	rand.Shuffle(len(slice), func(i, j int) {
// 		slice[i], slice[j] = slice[j], slice[i]
// 	})
// }

// func test(db_name string) error {

// 	// Make a new DB
// 	file, file_header, err := Create_and_ConnectDB(db_name)
// 	if err != nil {
// 		return err
// 	}
// 	DisconnectDB(file, file_header)

// 	// Work with the newly created db file
// 	file, file_header, err = ConnectDB(db_name)
// 	if err != nil {
// 		return err
// 	}

// 	// Try putting data in the B-tree
// 	num_data := 300
// 	data_size := 500
// 	randomness_in_data_size := 100
// 	rand_num := 0
// 	data_arr := make(map[int]string)
// 	var data string
// 	for i := 0; i < num_data; i++ {
// 		data = ""
// 		rand_num = rand.Intn(randomness_in_data_size)
// 		for j := 0; j < data_size+rand_num; j++ {
// 			data += strconv.Itoa(j)
// 		}
// 		data_arr[i] = data
// 		err = Insert(uint32(i), []byte(data), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = VisualizeDB(file)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println()
// 	fmt.Println("Total_pages =", file_header.Total_pages)
// 	fmt.Println("Free_space_table =", file_header.Free_space_table[:max(10, file_header.Space_table_size)])
// 	fmt.Println("Space_table_size =", file_header.Space_table_size)
// 	fmt.Println("Total_data_size =", file_header.Total_data_size)
// 	fmt.Println("Root_node_id =", file_header.Root_node_id)
// 	fmt.Println()

// 	err = postorder(file_header.Root_node_id, false, file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	total_data_size := 0
// 	for key, value := range data_arr {
// 		data, found, err := Search(uint32(key), file_header.Root_node_id, file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 		if !found {
// 			return errors.New(fmt.Sprintf("error while trying to search for key %v in the b-tree", key))
// 		}
// 		if value != string(data) {
// 			return errors.New(fmt.Sprintf("data saved (%v) doesn't equal, data asked to save (%v) for the key %v", data, value, key))
// 		}
// 		total_data_size += len(data)
// 	}
// 	fmt.Printf("\nALL THE DATA IS CORRECTLY SAVED IN THE DB\n\n")

// 	if file_header.Total_data_size != uint64(total_data_size) {
// 		return errors.New(fmt.Sprintf("total_data_size %v in file header is displayed wrong, expected %v", file_header.Total_data_size, total_data_size))
// 	}
// 	fmt.Printf("\nfile_header.Total_data_size %v == %v [Correct]\n\n", file_header.Total_data_size, total_data_size)

// 	fmt.Println()
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Println()

// 	// Try deleting some data from the B-tree
// 	nums := make([]int, num_data)
// 	for i := 0; i < num_data; i++ {
// 		nums[i] = i
// 	}
// 	shuffleSlice(nums)
// 	fmt.Printf("%v \n\n", nums)
// 	for _, num := range nums[:(num_data / 2)] {
// 		delete(data_arr, num)
// 		err = Delete(uint32(num), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	err = VisualizeDB(file)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println()
// 	fmt.Println("Total_pages =", file_header.Total_pages)
// 	fmt.Println("Free_space_table =", file_header.Free_space_table[:max(10, file_header.Space_table_size)])
// 	fmt.Println("Space_table_size =", file_header.Space_table_size)
// 	fmt.Println("Total_data_size =", file_header.Total_data_size)
// 	fmt.Println("Root_node_id =", file_header.Root_node_id)
// 	fmt.Println()

// 	err = postorder(file_header.Root_node_id, false, file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	// Check if all the inserted data is correct and there
// 	total_data_size = 0
// 	for key, value := range data_arr {
// 		data, found, err := Search(uint32(key), file_header.Root_node_id, file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 		if !found {
// 			return errors.New(fmt.Sprintf("error while trying to search for key %v in the b-tree", key))
// 		}
// 		if value != string(data) {
// 			return errors.New(fmt.Sprintf("data saved (%v) doesn't equal, data asked to save (%v) for the key %v", data, value, key))
// 		}
// 		total_data_size += len(data)
// 	}
// 	fmt.Printf("\nALL THE DATA IS CORRECTLY SAVED IN THE DB\n\n")

// 	if file_header.Total_data_size != uint64(total_data_size) {
// 		return errors.New(fmt.Sprintf("total_data_size %v in file header is displayed wrong, expected %v", file_header.Total_data_size, total_data_size))
// 	}
// 	fmt.Printf("\nfile_header.Total_data_size %v == %v [Correct]\n\n", file_header.Total_data_size, total_data_size)

// 	DisconnectDB(file, file_header)
// 	err = os.Remove("../databases/" + db_name + ".db")
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func main() {
// 	for i := 0; i < 1; i++ {
// 		err := test("aswd")
// 		if err != nil {
// 			fmt.Printf("%+v\n", err)
// 			return
// 		}
// 	}
// }
