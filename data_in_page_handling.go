package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/pkg/errors"
)

const DATA_HEADER uint16 = 0x1E7F
const max_data_size = PAGESIZE - 512 // DO NOT CHANGE // maximum data that can be put inside the DataPage, including the header for the data

// Data Handling in Data Pages

func overlap_intervals_bytes(a_offset, b_offset uint16, a_size, b_size uint16) bool {
	// a and b should be in sorted order
	if a_offset < b_offset {
		return a_offset+a_size == b_offset
	} else if a_offset > b_offset {
		return b_offset+b_size == a_size
	}
	return true
}

func DataPage_table_fixer(dp *DataPage) {

	// Removing empty elements
	i := uint16(0)
	j := uint16(0)
	for i < dp.Space_table_size && j < dp.Space_table_size {
		for j < dp.Space_table_size && dp.Unallocated_space_table[j].Size == 0 {
			j++
		}
		if j >= dp.Space_table_size {
			break
		}
		if i != j {
			dp.Unallocated_space_table[i] = dp.Unallocated_space_table[j]
			dp.Unallocated_space_table[j] = data_page_unallocated_space_table_row{0, 0}
		}
		i++
		j++
	}
	dp.Space_table_size = i

	// Sorting elements
	sort.Slice(dp.Unallocated_space_table[:dp.Space_table_size], func(i, j int) bool {
		a := dp.Unallocated_space_table[i]
		b := dp.Unallocated_space_table[j]
		return a.Offset > b.Offset
	})

	// Merging overlapping elements
	i = dp.Space_table_size - 1
	for i < dp.Space_table_size {
		j = i - 1
		for j < dp.Space_table_size && overlap_intervals_bytes(dp.Unallocated_space_table[i].Offset, dp.Unallocated_space_table[j].Offset, dp.Unallocated_space_table[i].Size, dp.Unallocated_space_table[j].Size) {
			dp.Unallocated_space_table[i].Size += dp.Unallocated_space_table[j].Size
			dp.Unallocated_space_table[j] = data_page_unallocated_space_table_row{0, 0}
			j--
		}
		i = j
	}

	// Removing empty elements
	i = 0
	j = 0
	for i < dp.Space_table_size && j < dp.Space_table_size {
		for j < dp.Space_table_size && dp.Unallocated_space_table[j].Size == 0 {
			j++
		}
		if j >= dp.Space_table_size {
			break
		}
		if i != j {
			dp.Unallocated_space_table[i] = dp.Unallocated_space_table[j]
			dp.Unallocated_space_table[j] = data_page_unallocated_space_table_row{0, 0}
		}
		i++
		j++
	}
	dp.Space_table_size = i
}

func appendHeader(b []byte) []byte {
	// Create a new byte slice to store the final result
	newSlice := make([]byte, len(b)+6) // 6 bytes for one uint16 (2B) header value and one uint32 (4B) length value

	// Add the head value at the beginning
	if NativeEndian == binary.BigEndian {
		binary.BigEndian.PutUint16(newSlice[:2], DATA_HEADER)
	} else {
		binary.LittleEndian.PutUint16(newSlice[:2], DATA_HEADER)
	}

	// Add the tail value at the end
	if NativeEndian == binary.BigEndian {
		binary.BigEndian.PutUint32(newSlice[2:6], uint32(len(b)))
	} else {
		binary.LittleEndian.PutUint32(newSlice[2:6], uint32(len(b)))
	}

	// Copy the original slice
	copy(newSlice[6:], b)

	return newSlice
}

func defragment_datapage(datapage_id uint32, file_header *FileHeaderPage, file *os.File) (map[uint32]uint32, error) {

	// We will eventually need to get all the datapages anyways, so why not do it in the beginning?
	// Then make an array of these datapages... This will make working with them much easier!
	var dp_array []*DataPage
	var dp_id_array []uint32
	// Working with heap memory is easier right now and also faster (in this case) since it is unlikely to be that more than size of 4 (==16kB)
	// If we instead just bring in the Datapages we have the need for the moment, it will create a massive delay since smae Datapages will have to be
	// loaded repeatedly... Also, saving the updated Datapages will become more hassling...
	cur_page_id := datapage_id
	for cur_page_id != 0 {
		pt, _, _, temp, err := ReadPage(file, cur_page_id)
		if err != nil {
			return nil, err
		}
		if pt != Page_type_ids["Data"] {
			return nil, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page of type %v", cur_page_id, pt))
		}
		dp_id_array = append(dp_id_array, cur_page_id)
		cur_page_id = temp.Next_data_page
		dp_array = append(dp_array, temp)
	}

	type index struct {
		dp   int
		data int
	}
	j := index{0, 0} // Index of the byte from which we are start going to fill from
	k := index{0, 0} // Index which will look for data and ignore blanks (by incrementing when encountering them)

	changes_record := make(map[uint32]uint32)

	var header_buffer [6]byte

	// Loop through all the bytes in the data field of datapage (including all the next datapages)
	for k.dp < len(dp_array) && j.dp < len(dp_array) { // Iterating through all the datapages
		for u := 0; u < 6; u++ {
			if k.data >= max_data_size {
				k.data = 0
				k.dp++
			}
			if k.dp >= len(dp_array) {
				break
			}
			header_buffer[u] = dp_array[k.dp].Data[k.data]
			k.data++
		}
		if k.dp >= len(dp_array) {
			break
		}
		for u := 0; u < 6; u++ {
			if k.data == 0 {
				k.data = max_data_size - 1
				k.dp--
				continue
			}
			k.data--
		}
		is_start_of_data, length_of_data := checkHeader(header_buffer[:], 0)

		// If there is data in our current i_th position then fill it in j_th position and increment it
		if is_start_of_data {
			if j == k {
				k.data += int(length_of_data)
				j.data += int(length_of_data)
				if k.data >= max_data_size {
					k.data = k.data - max_data_size
					k.dp++
					j.data = j.data - max_data_size
					j.dp++
				}
				continue
			}
			if max_data_size-j.data <= 6 {
				j.data = 0
				j.dp++
			}
			changes_record[uint32(max_data_size*k.dp+k.data)] = uint32(max_data_size*j.dp + j.data)
			for t := 0; t < int(length_of_data); t++ {
				dp_array[j.dp].Data[j.data] = dp_array[k.dp].Data[k.data]
				dp_array[k.dp].Data[k.data] = 0
				k.data++
				j.data++
				if k.data == max_data_size {
					k.data = 0
					k.dp++
				}
				if j.data == max_data_size {
					j.data = 0
					j.dp++
				}
			}
			continue
		}
		// else just increment i_th position and skip all the blanks
		k.data++
		if k.data >= max_data_size {
			k.data = 0
			k.dp++
		}
		if k.dp >= len(dp_array) {
			break
		}
		for k.dp < len(dp_array) && dp_array[k.dp].Data[k.data] == 0 {
			k.data++
			if k.data == max_data_size {
				k.data = 0
				k.dp++
			}
		}
	}

	// Fix the headers of all the DataPages
	delete_from := -1
	for i := 0; i < len(dp_array); i++ {
		count := 0
		for j := max_data_size - 1; j >= 0; j-- {
			if dp_array[i].Data[j] == 0 {
				count++
			} else {
				break
			}
		}
		dp_array[i].Data_held = uint16(max_data_size - count)
		if count == max_data_size {
			for k := i + 1; k < len(dp_array); k++ {
				dp_array[k].Data_held = 0
			}
			if i != 0 {
				dp_array[i-1].Next_data_page = 0
				delete_from = i
			} else {
				delete_from = i + 1
			}
			break
		}
		for t := 0; t < int(dp_array[i].Space_table_size); t++ {
			dp_array[i].Unallocated_space_table[t] = data_page_unallocated_space_table_row{0, 0}
		}
		dp_array[i].Space_table_size = 1
		dp_array[i].Unallocated_space_table[0] = data_page_unallocated_space_table_row{Offset: uint16(max_data_size - count), Size: uint16(count)}
	}

	// Save all the changes made to the DataPages
	for i := 0; i < len(dp_array); i++ {
		err := WriteChunk(file, dp_id_array[i], Data_to_Bytes(dp_array[i]))
		if err != nil {
			return changes_record, errors.Wrap(err, fmt.Sprintf("error while trying to overwrite unnecessary datapage %v", dp_id_array[i]))
		}
	}

	// Delete unnecessary Datapages
	if delete_from > 0 {
		for i := delete_from; i < len(dp_array); i++ {
			err := DeletePage(dp_id_array[i], file_header, file)
			if err != nil {
				return changes_record, errors.Wrap(err, fmt.Sprintf("error while trying to delete unnecessary datapage %v", dp_id_array[i]))
			}
		}
	}

	return changes_record, nil
}

func Defragment_Node(page_id uint32, file_header *FileHeaderPage, file *os.File) error {

	pt, _, np, _, err := ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", page_id, pt))
	}

	if np.Data_page_id == 0 {
		return errors.New(fmt.Sprintf("tried to defragment a nodepage %v with no associated datapages", page_id))
	}

	// defragment Datapage
	changes_made, err := defragment_datapage(np.Data_page_id, file_header, file)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to defragment the datapage %v belonging to the nodepage %v", np.Data_page_id, page_id))
	}

	// Relay changes to the offsets near the key
	for i := 0; i < int(np.Block_size); i++ {
		if val, ok := changes_made[np.Blocks[i].Offset]; ok {
			np.Blocks[i].Offset = val
		}
	}

	// Save the changes made to the nodepage
	err = WriteChunk(file, page_id, Data_to_Bytes(np))
	if err != nil {
		return err
	}

	return nil
}

func rec_put_in_datapage_helper(page_id uint32, data []byte, file_header *FileHeaderPage, file *os.File) (uint32, error) { // This function is only to be used after Datapage has been defragmented
	/*
		ONLY TO BE USED AFTER DEFRAGMENTATION
	*/

	pt, _, _, dp, err := ReadPage(file, page_id)
	if err != nil {
		return 0, err
	}
	if pt != Page_type_ids["Data"] {
		return 0, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page of type %v", page_id, pt))
	}

	// Data must already be appended with headers

	// Go the last datapage available and start adding data from there
	if dp.Next_data_page != 0 {
		off, err := rec_put_in_datapage_helper(dp.Next_data_page, data, file_header, file)
		return max_data_size + off, err
	}

	// This is the last page, start adding data from here

	// Calculate how many DataPages more might be needed to fit the data
	datapages_needed := 0
	if len(data) > int(dp.Unallocated_space_table[0].Size) {
		datapages_needed = 1 + ((len(data) - int(dp.Unallocated_space_table[0].Size)) / max_data_size)
	}
	// Make the required number of datapages
	cur_dp := dp
	cur_id := page_id
	var new_dp_id uint32
	for i := 0; i < datapages_needed; i++ {
		// Make a new datapage for data put into
		new_dp_id, err = MakeNewPage(Page_type_ids["Data"], file_header, file)
		if err != nil {
			return 0, errors.Wrap(err, "needed new datapage for data to fit but coudnt make a new datapage")
		}
		// Connect new datapage to current datapage
		cur_dp.Next_data_page = new_dp_id
		if cur_id == page_id {
			dp.Next_data_page = new_dp_id
		}
		// Add parent node id to the new datapage
		err = put_nodeId_to_data_page(dp.Parent_node_page, new_dp_id, file)
		if err != nil {
			return 0, errors.Wrap(err, fmt.Sprintf("error while trying to put node id %v in the datapages", dp.Parent_node_page))
		}
		// Save the current datapage
		err = WriteChunk(file, cur_id, Data_to_Bytes(cur_dp))
		if err != nil {
			return 0, err
		}
		// Update the current datapage
		pt, _, _, cur_dp, err = ReadPage(file, cur_dp.Next_data_page)
		cur_id = new_dp_id
		if err != nil {
			return 0, err
		}
		if pt != Page_type_ids["Data"] {
			return 0, errors.New(fmt.Sprintf("tried to read a datapage (%v) at page id %v but instead got page of type %v", Page_type_ids["Data"], cur_dp.Next_data_page, pt))
		}
	}

	// Putting the data in
	off := dp.Unallocated_space_table[0].Offset
	type index struct {
		dp      *DataPage
		dp_id   uint32
		data_id uint16
	}
	j := index{dp, page_id, dp.Unallocated_space_table[0].Offset}
	i := 0
	for i < len(data) {
		if j.data_id >= max_data_size {
			DataPage_table_fixer(j.dp)
			// Save the current datapage
			err = WriteChunk(file, j.dp_id, Data_to_Bytes(j.dp))
			if err != nil {
				return uint32(dp.Unallocated_space_table[0].Offset), err
			}

			j.data_id = 0
			j.dp_id = j.dp.Next_data_page

			// Update the current datapage
			pt, _, _, j.dp, err = ReadPage(file, j.dp_id)
			if err != nil {
				return uint32(dp.Unallocated_space_table[0].Offset), err
			}
			if pt != Page_type_ids["Data"] {
				return uint32(dp.Unallocated_space_table[0].Offset), errors.New(fmt.Sprintf("error, read page %v isn't a datapage, found page_id = %v", j.dp_id, pt))
			}
		}
		j.dp.Data[j.data_id] = data[i]
		i++
		j.data_id++
		j.dp.Data_held += 1
		j.dp.Unallocated_space_table[0].Size -= 1
		j.dp.Unallocated_space_table[0].Offset += 1
	}
	file_header.Total_data_size += uint64(len(data))
	// Save the current datapage
	err = WriteChunk(file, j.dp_id, Data_to_Bytes(j.dp))
	if err != nil {
		return uint32(dp.Unallocated_space_table[0].Offset), err
	}

	return uint32(off), nil
}

func Put_in_DataPage(page_id uint32, data []byte, file_header *FileHeaderPage, file *os.File) (uint32, error) {

	pt, _, _, dp, err := ReadPage(file, page_id)
	if err != nil {
		return 0, err
	}
	if pt != Page_type_ids["Data"] {
		return 0, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page of type %v", page_id, pt))
	}

	data = appendHeader(data)

	// 2 casses possible,
	// 		1. Data can fit within an existing fragment -> Put it there
	//		2. It cannot fit in THIS datapage
	//			2.1. There are more pages there -> Check if it can fit any fragments in the next pages
	//			2.2. There aren't anymore pages there -> Defragment Datapage, then put it at the end

	type index struct {
		dp    *DataPage
		dp_id uint32
		ind   uint32
	}
	cur_dp := index{dp, page_id, 0}

	for cur_dp.dp_id != 0 {
		// Checking if Data can fit or not
		chosen := -1
		for i := 0; i < int(cur_dp.dp.Space_table_size); i++ {
			// BEST FIT alogorithm to find where to put the data
			if int(cur_dp.dp.Unallocated_space_table[i].Size) >= len(data) {
				if chosen != -1 && cur_dp.dp.Unallocated_space_table[i].Size <= cur_dp.dp.Unallocated_space_table[chosen].Size {
					chosen = i
				} else if chosen == -1 {
					chosen = i
				}
			}
		}

		if chosen == -1 {
			cur_dp.ind++
			cur_dp.dp_id = cur_dp.dp.Next_data_page
			if cur_dp.dp_id == 0 {
				cur_dp.dp = nil
				break
			}
			pt, _, _, cur_dp.dp, err = ReadPage(file, cur_dp.dp_id)
			if err != nil {
				return 0, err
			}
			if pt != Page_type_ids["Data"] {
				return 0, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page of type %v", cur_dp.dp_id, pt))
			}
			continue
		}

		// Data can be fitted inside this datapage with no problem
		for i := 0; i < len(data); i++ {
			cur_dp.dp.Data[cur_dp.dp.Unallocated_space_table[chosen].Offset+uint16(i)] = data[i]
		}

		// Update the headers
		data_loc := cur_dp.dp.Unallocated_space_table[chosen].Offset
		cur_dp.dp.Data_held += uint16(len(data))
		file_header.Total_data_size += uint64(len(data))
		// Update the Unallocated Space Table
		cur_dp.dp.Unallocated_space_table[chosen].Size -= uint16(len(data))
		cur_dp.dp.Unallocated_space_table[chosen].Offset += uint16(len(data))
		DataPage_table_fixer(cur_dp.dp)

		// Save the changes made
		err = WriteChunk(file, cur_dp.dp_id, Data_to_Bytes(cur_dp.dp))
		if err != nil {
			return 0, err
		}

		return (cur_dp.ind * max_data_size) + uint32(data_loc), nil
	}

	// No Space in any of the DataPages for the new Data, so defragment and then put the data

	// Defragment the Datapages
	if dp.Parent_node_page == 0 {
		_, err = defragment_datapage(page_id, file_header, file)
		if err != nil {
			return 0, errors.Wrap(err, fmt.Sprintf("error while trying to defragment the nodepage %v", page_id))
		}
	} else {
		err = Defragment_Node(dp.Parent_node_page, file_header, file)
		if err != nil {
			return 0, errors.Wrap(err, fmt.Sprintf("error while trying to defragment the nodepage %v", dp.Parent_node_page))
		}
	}

	// Now, just put data in the last place in the datapage
	off, err := rec_put_in_datapage_helper(page_id, data, file_header, file)
	if err != nil {
		return 0, err
	}

	return off, nil
}

func checkHeader(b []byte, offset uint16) (bool, uint32) {
	// Ensure there is enough space in the slice for the header and the length
	if len(b) < int(offset)+6 {
		return false, 0
	}

	// Check the head (at offset)
	var head uint16
	if NativeEndian == binary.BigEndian {
		head = binary.BigEndian.Uint16(b[offset:])
	} else {
		head = binary.LittleEndian.Uint16(b[offset:])
	}
	if head != DATA_HEADER {
		return false, 0
	}

	// Check the length (at offset + 2)
	var length uint32
	if NativeEndian == binary.BigEndian {
		length = binary.BigEndian.Uint32(b[offset+2:])
	} else {
		length = binary.LittleEndian.Uint32(b[offset+2:])
	}

	return true, length + 6
}

func Delete_in_DataPage(page_id uint32, offset uint32, file_header *FileHeaderPage, file *os.File) error {

	pt, _, _, dp, err := ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Data"] {
		return errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", page_id, pt))
	}

	if offset >= max_data_size {
		if dp.Next_data_page == 0 {
			return errors.New(fmt.Sprintf("recieved input for offset %v which is over the length of the data field size for this datapage and no next datapage exists", offset))
		}
		err := Delete_in_DataPage(dp.Next_data_page, offset-max_data_size, file_header, file)
		if err != nil {
			return err
		}
		return nil
	}

	// Deleting the data
	type index struct {
		dp      *DataPage
		dp_id   uint32
		data_id uint16
	}
	var overflow bool = false
	j := index{dp, page_id, uint16(offset)}
	k := j
	var header_buffer [6]byte
	for u := 0; u < 6; u++ {
		if k.data_id == max_data_size {
			k.data_id = 0
			k.dp_id = k.dp.Next_data_page
			pt, _, _, k.dp, err = ReadPage(file, k.dp_id)
			if err != nil {
				return err
			}
			if pt != Page_type_ids["Data"] {
				return errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", k.dp_id, pt))
			}
		}
		header_buffer[u] = k.dp.Data[k.data_id]
		k.data_id++
	}
	isValid, length := checkHeader(header_buffer[:], 0)
	if !isValid {
		return errors.New(fmt.Sprintf("error: data at offset %v is not valid data", offset))
	}

	if j.dp.Space_table_size+1 > data_page_space_table_num_entries {
		overflow = true
	}
	if !overflow {
		j.dp.Space_table_size++
		j.dp.Unallocated_space_table[j.dp.Space_table_size-1].Offset = uint16(offset)
	}
	for i := 0; i < int(length); i++ {
		if j.data_id >= max_data_size {
			// Fix the Free Space Table
			if !overflow {
				DataPage_table_fixer(j.dp)
			}
			// Save the DataPage
			err = WriteChunk(file, j.dp_id, Data_to_Bytes(j.dp))
			if err != nil {
				return err
			}
			// Update the j_th index
			j.dp_id = j.dp.Next_data_page
			j.data_id = 0
			pt, _, _, j.dp, err = ReadPage(file, j.dp_id)
			if err != nil {
				return err
			}
			if pt != Page_type_ids["Data"] {
				return errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", page_id, pt))
			}
			if !overflow {
				if j.dp.Space_table_size+1 > data_page_space_table_num_entries {
					overflow = true
				}
			}
			if !overflow {
				j.dp.Space_table_size++
				j.dp.Unallocated_space_table[j.dp.Space_table_size-1].Offset = 0
			}
		}
		j.dp.Data[j.data_id] = 0
		j.dp.Data_held--
		j.data_id++
		if !overflow {
			j.dp.Unallocated_space_table[j.dp.Space_table_size-1].Size++
		}
	}

	// Save the j_th DataPage
	DataPage_table_fixer(j.dp)
	err = WriteChunk(file, j.dp_id, Data_to_Bytes(j.dp))
	if err != nil {
		return err
	}

	// Checking if Free Space Table is full or not, if full then defragment the Datapages
	if overflow {
		if dp.Parent_node_page != 0 {
			err = Defragment_Node(dp.Parent_node_page, file_header, file)
			if err != nil {
				return err
			}

		} else {
			_, err = defragment_datapage(page_id, file_header, file)
			if err != nil {
				return err
			}
		}
	}

	// Update the file_header
	file_header.Total_data_size -= uint64(length)
	return nil
}

func Read_from_DataPage(page_id uint32, offset uint32, file_header *FileHeaderPage, file *os.File) ([]byte, error) {

	pt, _, _, dp, err := ReadPage(file, page_id)
	if err != nil {
		return nil, err
	}
	if pt != Page_type_ids["Data"] {
		return nil, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", page_id, pt))
	}

	if offset >= max_data_size {
		if dp.Next_data_page == 0 {
			return nil, errors.New(fmt.Sprintf("recieved input for offset %v which is over the length of the data field size for this datapage and no next datapage exists", offset))
		}
		data, err := Read_from_DataPage(dp.Next_data_page, offset-max_data_size, file_header, file)
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	type index struct {
		dp      *DataPage
		dp_id   uint32
		data_id uint16
	}
	j := index{dp, page_id, uint16(offset)}
	k := j
	var header_buffer [6]byte
	for u := 0; u < 6; u++ {
		if k.data_id == max_data_size {
			k.data_id = 0
			k.dp_id = k.dp.Next_data_page
			pt, _, _, k.dp, err = ReadPage(file, k.dp_id)
			if err != nil {
				return nil, err
			}
			if pt != Page_type_ids["Data"] {
				return nil, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", k.dp_id, pt))
			}
		}
		header_buffer[u] = k.dp.Data[k.data_id]
		k.data_id++
	}
	isValid, length := checkHeader(header_buffer[:], 0)
	if !isValid {
		return nil, errors.New(fmt.Sprintf("data at offset %v is not valid data", offset))
	}

	// Reading the Data
	j = index{dp, page_id, uint16(offset)}
	var data []byte
	for i := 0; i < int(length); i++ {
		if j.data_id >= max_data_size {
			if j.dp.Next_data_page == 0 {
				return nil, errors.New(fmt.Sprintf("recieved input for offset %v which is over the length of the data field size for this datapage and no next datapage exists", offset))
			}
			j.data_id = 0
			j.dp_id = j.dp.Next_data_page
			pt, _, _, j.dp, err = ReadPage(file, j.dp_id)
			if err != nil {
				return nil, err
			}
			if pt != Page_type_ids["Data"] {
				return nil, errors.New(fmt.Sprintf("read page %v isn't a datapage. read page type to be %v", page_id, pt))
			}
		}

		data = append(data, j.dp.Data[j.data_id])
		j.data_id++
	}

	// Removing the headers (6 Bytes) from the data
	return data[6:], nil
}

// Data Handling in Node Pages

func binary_index_node(arr []node_page_cell_offet, low, high int, key uint32) (int, bool) {
	l, r := low, high
	mid := 0
	for l < r {
		mid = (l + r) / 2
		if arr[mid].Key == key {
			return mid, true
		} else if arr[mid].Key > key {
			r = mid
		} else {
			l = mid + 1
		}
	}
	return l, false
}

func Put_in_NodePage(page_id uint32, key uint32, data []byte, new_node uint32, put_child_on_left_of_new_node bool, file_header *FileHeaderPage, file *os.File) error {

	pt, _, np, _, err := ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", page_id, pt))
	}

	// Make a new DataPage if it doesn't exist
	if np.Data_page_id == 0 {
		data_page_id, err := MakeNewPage(Page_type_ids["Data"], file_header, file)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while trying to make a new DataPage for the NodePage %v", page_id))
		}
		np.Data_page_id = data_page_id
		err = put_nodeId_to_data_page(page_id, data_page_id, file)
		if err != nil {
			return errors.Wrap(err, "error while trying to put node id in the newly created datapages")
		}
	}

	// Check if node is full
	if np.Block_size == uint16(MAX_DEGREE) {
		return errors.New(fmt.Sprintf("the nodepage %v is full and cannot accept any further key-data", page_id))
	}

	// Put the data in the DataPage and get where it is saved
	off, err := Put_in_DataPage(np.Data_page_id, data, file_header, file) // This can update the NodePage we are working in (just the offsets)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to put data in datapage %v for the nodepage %v", np.Data_page_id, page_id))
	}

	// Read the possibly updated NodePage
	pt, _, np, _, err = ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", page_id, pt))
	}

	// Binary Search to find the position in which this key should be kept.
	ind, inArr := binary_index_node(np.Blocks[:np.Block_size], 0, int(np.Block_size), key)
	if inArr {
		return errors.New(fmt.Sprintf("the key %v already exists in the nodepage %v", key, page_id))
	}

	// Putting the key and offset in the NodePage
	var i int
	for i = int(np.Block_size); i > ind; i-- {
		np.Blocks[i] = np.Blocks[i-1]
	}
	np.Blocks[ind] = node_page_cell_offet{Key: key, Offset: off}
	limit := ind + 1
	if put_child_on_left_of_new_node {
		limit = ind
	}
	for i = int(np.Block_size) + 1; i > limit; i-- {
		np.Children[i] = np.Children[i-1]
	}
	np.Children[limit] = new_node
	np.Block_size += 1

	// Save the NodePage
	err = WriteChunk(file, page_id, Data_to_Bytes(np))
	if err != nil {
		return err
	}

	return nil
}

func Delete_in_NodePage(page_id uint32, key uint32, delete_left_child_of_key bool, file_header *FileHeaderPage, file *os.File) error {

	pt, _, np, _, err := ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt != Page_type_ids["Node"] {
		return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page type to be %v", page_id, pt))
	}

	// Binary Search to find the position in which this key should be kept.
	ind, inArr := binary_index_node(np.Blocks[:np.Block_size], 0, int(np.Block_size), key)
	if !inArr {
		return errors.New(fmt.Sprintf("the key %v doesn't exist in the nodepage %v", key, page_id))
	}
	if ind >= int(np.Block_size) {
		return errors.New(fmt.Sprintf("the key %v doesn't exist in the nodepage %v", key, page_id))
	}

	// Remove the key from the node
	temp := np.Blocks[ind]
	var j int
	for j = ind; j < min(MAX_DEGREE-1, int(np.Block_size)); j++ {
		np.Blocks[j] = np.Blocks[j+1]
	}
	j = ind + 1
	if delete_left_child_of_key {
		j = ind
	}
	for j < min(MAX_DEGREE, int(np.Block_size+1)) {
		np.Children[j] = np.Children[j+1]
		j++
	}
	np.Children[j] = 0
	if np.Block_size == uint16(MAX_DEGREE) {
		np.Blocks[MAX_DEGREE-1] = node_page_cell_offet{0, 0}
		np.Children[MAX_DEGREE] = 0
	}
	np.Block_size -= 1

	// Save the NodePage
	err = WriteChunk(file, page_id, Data_to_Bytes(np))
	if err != nil {
		return err
	}

	// Save the associated key data from the DataPage
	err = Delete_in_DataPage(np.Data_page_id, temp.Offset, file_header, file)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to delete the data (at offset %v) for the associated key %v", np.Blocks[ind].Offset, key))
	}

	return nil
}

func Read_from_NodePage(page_id uint32, key uint32, file_header *FileHeaderPage, file *os.File) ([]byte, bool, error) {
	pt, _, np, _, err := ReadPage(file, page_id)
	if err != nil {
		return nil, false, err
	}
	if pt != Page_type_ids["Node"] {
		return nil, false, errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", page_id, pt))
	}

	// Binary Search to find the position in which this key should be kept.
	ind, inArr := binary_index_node(np.Blocks[:np.Block_size], 0, int(np.Block_size), key)
	if !inArr || ind >= int(np.Block_size) {
		return nil, false, nil
	}

	data, err := Read_from_DataPage(np.Data_page_id, np.Blocks[ind].Offset, file_header, file)
	if err != nil {
		return nil, false, errors.Wrap(err, fmt.Sprintf("error while trying to retrieve key %v data from the datapage %v stored at offset %v", key, np.Data_page_id, np.Blocks[ind].Offset))
	}

	return data, true, nil
}

// Visualization of Pages

func Visualize_NodePage(page_id uint32, np *NodePage) {
	fmt.Println()
	fmt.Printf("NodePage ID: %v\n", page_id)
	fmt.Printf("\t-> Data_page_id: %v\n", np.Data_page_id)
	fmt.Printf("\t-> Block_size: %v\n", np.Block_size)
	fmt.Printf("\t-> Blocks: %v\n", np.Blocks[:max(np.Block_size, 10)])
	fmt.Printf("\t-> Children: %v\n", np.Children[:max(np.Block_size, 10)+1])
}

func Visualize_DataPage(page_id uint32, dp *DataPage) {

	fmt.Println()
	fmt.Printf("DataPage ID: %v\n", page_id)
	fmt.Printf("\t-> Data_held: %v\n", dp.Data_held)
	fmt.Printf("\t-> Next_data_page: %v\n", dp.Next_data_page)
	fmt.Printf("\t-> Parent_node_page: %v\n", dp.Parent_node_page)
	fmt.Printf("\t-> Space_table_size: %v\n", dp.Space_table_size)
	fmt.Printf("\t-> Unallocated_space_table: %v\n", dp.Unallocated_space_table[:max(dp.Space_table_size, 10)])
	fmt.Printf("\t-> Data: ")

	var is_data bool
	var length uint32
	var i uint16 = 0
	var j int
	for i < max_data_size {

		if i == 0 && dp.Data[i] != 0 {
			is_data, _ = checkHeader(dp.Data[:], i)
			for !is_data && i < max_data_size && dp.Data[i] != 0 {
				fmt.Printf("%c", dp.Data[i])
				i++
				is_data, _ = checkHeader(dp.Data[:], i)
			}
			fmt.Printf(", ")
		}

		is_data, length = checkHeader(dp.Data[:], i)
		if !is_data {
			fmt.Printf("---, ")
		}
		for !is_data && i < max_data_size {
			i++
			is_data, length = checkHeader(dp.Data[:], i)
		}
		if i >= max_data_size {
			break
		}
		i += 6
		for j = 0; j < int(length-6) && i < max_data_size; j++ {
			fmt.Printf("%c", dp.Data[i])
			i++
		}
		if j < int(length-6) {
			fmt.Printf("...{continued in %v}", dp.Next_data_page)
		}
		fmt.Printf(", ")
	}
	fmt.Println()
}

func Visualize_Page(page_id uint32, file_header *FileHeaderPage, file *os.File) error {
	pt, fp, np, dp, err := ReadPage(file, page_id)
	if err != nil {
		return err
	}
	if pt == Page_type_ids["Node"] {
		Visualize_NodePage(page_id, np)
		return nil
	}
	if pt == Page_type_ids["Data"] {
		Visualize_DataPage(page_id, dp)
		return nil
	}
	if pt == Page_type_ids["FileHeader"] {
		fmt.Println()
		fmt.Printf("FileHeader ID: %v\n", page_id)
		fmt.Println("\t-> Total_pages =", fp.Total_pages)
		fmt.Println("\t-> Space_table_size =", fp.Space_table_size)
		fmt.Println("\t-> Total_data_size =", fp.Total_data_size)
		fmt.Println("\t-> Free_space_table =", fp.Free_space_table[:max(10, fp.Space_table_size)])
		return nil
	}
	fmt.Println()
	fmt.Printf("FreePage ID: %v\n", page_id)
	fmt.Println("\t-> This is a free page")
	return nil
}

// TESTING the 3 abstract layers till now

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
// 	defer DisconnectDB(file, file_header)

// 	// Add Bunch of Data and Node pages
// 	for i := 0; i < 2; i++ {
// 		_, err = MakeNewPage(Page_type_ids["Data"], file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	for i := 0; i < 2; i++ {
// 		_, err = MakeNewPage(Page_type_ids["Node"], file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	for i := 0; i < 2; i++ {
// 		_, err = MakeNewPage(Page_type_ids["Data"], file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	for i := 0; i < 2; i++ {
// 		_, err = MakeNewPage(Page_type_ids["Node"], file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	for i := 0; i < 2; i++ {
// 		_, err = MakeNewPage(Page_type_ids["Data"], file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	// Delete a bunch of Pages
// 	pages_to_del := []int{14, 10, 4, 1, 13}
// 	for i := 0; i < len(pages_to_del); i++ {
// 		err = DeletePage(uint32(pages_to_del[i]), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}

// 	err = VisualizeDB(file)
// 	if err != nil {
// 		return err
// 	}

// 	// Try putting data in the NodePages and DataPages
// 	pages_to_put_data_in := []int{6}
// 	// pages_to_put_data_in := []int{2, 5, 11, 8}
// 	var data string
// 	num_data := 335
// 	per_data_size := 5
// 	randmoness_to_data := 10
// 	for i := 0; i < len(pages_to_put_data_in); i++ {
// 		for j := 0; j < num_data; j++ {
// 			data = ""
// 			// for k := 0; k < 100; k++ {
// 			// 	data += strconv.Itoa(k)
// 			// }
// 			for k := 0; k < per_data_size+rand.Intn(randmoness_to_data); k++ {
// 				data += strconv.Itoa(k)
// 			}
// 			err = Put_in_NodePage(uint32(pages_to_put_data_in[i]), uint32(j), []byte(data), 0, false, file_header, file)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		err = Visualize_Page(uint32(pages_to_put_data_in[i]), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = Visualize_Page(5, file_header, file)
// 	if err != nil {
// 		return err
// 	}
// 	err = Visualize_Page(1, file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Println()
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Println()

// 	// Try deleting data from DataPages
// 	for i := 0; i < len(pages_to_put_data_in); i++ {
// 		for j := 0; j < num_data; j += 2 {
// 			err = Delete_in_NodePage(uint32(pages_to_put_data_in[i]), uint32(j), false, file_header, file)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		err = Visualize_Page(uint32(pages_to_put_data_in[i]), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = Visualize_Page(5, file_header, file)
// 	if err != nil {
// 		return err
// 	}
// 	err = Visualize_Page(1, file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	s, _, err := Read_from_NodePage(6, 3, file_header, file)
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Printf("\nkey: 3, data: %v\n", s)

// 	fmt.Println()
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Printf("####################################################################################################################################\n")
// 	fmt.Println()

// 	for i := 0; i < len(pages_to_put_data_in); i++ {
// 		for j := num_data; j < (3*num_data)/2; j++ {
// 			data = ""
// 			// for k := 0; k < 100; k++ {
// 			// 	data += strconv.Itoa(k)
// 			// }
// 			for k := 0; k < per_data_size+rand.Intn(randmoness_to_data); k++ {
// 				data += strconv.Itoa(k)
// 			}
// 			err = Put_in_NodePage(uint32(pages_to_put_data_in[i]), uint32(j), []byte(data), 0, false, file_header, file)
// 			if err != nil {
// 				return err
// 			}
// 		}
// 		err = Visualize_Page(uint32(pages_to_put_data_in[i]), file_header, file)
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	err = Visualize_Page(5, file_header, file)
// 	if err != nil {
// 		return err
// 	}
// 	err = Visualize_Page(1, file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	err = defragment_db_file(file_header, file)
// 	if err != nil {
// 		return err
// 	}

// 	err = VisualizeDB(file)
// 	if err != nil {
// 		return err
// 	}

// 	fmt.Println()
// 	fmt.Println("Total_pages =", file_header.Total_pages)
// 	fmt.Println("Free_space_table =", file_header.Free_space_table[:10])
// 	fmt.Println("Space_table_size =", file_header.Space_table_size)
// 	fmt.Println("Total_data_size =", file_header.Total_data_size)
// 	fmt.Println()

// 	err = os.Remove("../databases/" + db_name + ".db")
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

// func main() {
// 	var err error
// 	for i := 0; i < 1; i++ {
// 		err = test("awsd")
// 		if err != nil {
// 			panic(err)
// 		}
// 	}
// }
