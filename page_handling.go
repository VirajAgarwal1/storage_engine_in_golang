package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"sort"

	"github.com/pkg/errors"
)

// Easy Visual Way to see what is there in the DB
func VisualizeDB(file *os.File) error {

	file_stats, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "coudnt get the file stats")
	}
	if file_stats.Size()%PAGESIZE != 0 {
		return errors.New(fmt.Sprintf("DB size does not satisfy the implicit assumption of being multiple of 4096 bytes. This could mean that one of the pages being saved in the DB exceed or subceed the PAGESIZE (=%dB). This is should be fixed right away, as without this the DB will fail catastrophically", PAGESIZE))
	}
	num_pages_in_db := uint32(file_stats.Size() / PAGESIZE)

	var buf []byte
	var buf_reader *bytes.Reader
	var read_page Page
	var fp FileHeaderPage
	var np NodePage
	var dp DataPage
	var i uint32

	fmt.Printf("\nThe file is %d B = %d * 4kB long\n[REF] %v * %v = %v\n", file_stats.Size(), file_stats.Size()/PAGESIZE, file_stats.Size()/PAGESIZE, PAGESIZE, (file_stats.Size()/PAGESIZE)*PAGESIZE)
	for i = 0; i < num_pages_in_db; i++ {
		buf, err = ReadChunk(file, i)
		if err != nil {
			return errors.Wrap(err, "error in reading page while trying to visualize the db")
		}
		buf_reader = bytes.NewReader(buf)
		binary.Read(buf_reader, NativeEndian, &read_page)

		if read_page.Identification_num != PAGE_IDENTITY_NUM {
			fmt.Printf("%d. Free\n", i)
		} else {
			buf_reader.Reset(buf)

			if read_page.Page_type == Page_type_ids["FileHeader"] {
				binary.Read(buf_reader, NativeEndian, &fp)
				fmt.Printf("%d. File Header\n", i)

			} else if read_page.Page_type == Page_type_ids["Node"] {
				binary.Read(buf_reader, NativeEndian, &np)
				fmt.Printf("%d. Node -> [data] %v\n", i, np.Data_page_id)

			} else if read_page.Page_type == Page_type_ids["Data"] {
				binary.Read(buf_reader, NativeEndian, &dp)
				fmt.Printf("%d. Data [%v] -> [next] %v\n", i, dp.Parent_node_page, dp.Next_data_page)

			} else {
				fmt.Printf("%d. Free\n", i)
			}
		}
	}
	return nil
}

func Create_and_ConnectDB(db_name string) (*os.File, *FileHeaderPage, error) {

	file, err := os.OpenFile("./databases/"+db_name+".db", os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error while creating the database file")
	}

	file_header := FileHeaderPage{
		Identification_num: PAGE_IDENTITY_NUM,
		Page_type:          Page_type_ids["FileHeader"],
		Total_pages:        1,
	}
	buf := Data_to_Bytes(file_header)
	WriteChunk(file, 0, buf)

	return file, &file_header, nil
}

func ConnectDB(db_name string) (*os.File, *FileHeaderPage, error) {

	file, err := os.OpenFile("./databases/"+db_name+".db", os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error while creating the database file")
	}

	buf, err := ReadChunk(file, 0)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error while reading file header")
	}
	buf_reader := bytes.NewReader(buf)
	var file_header FileHeaderPage
	binary.Read(buf_reader, NativeEndian, &file_header)
	if file_header.Identification_num != PAGE_IDENTITY_NUM {
		return nil, nil, errors.New(fmt.Sprintf("the file header which was read doesn't have page identification number right, the page_ident_num in the age found = %d", file_header.Identification_num))
	}
	if file_header.Page_type != Page_type_ids["FileHeader"] {
		return nil, nil, errors.New(fmt.Sprintf("page read doesnt have a valid type id, found id = %d, expected to be %d (FileHeader)", file_header.Page_type, Page_type_ids["FileHeader"]))
	}

	return file, &file_header, nil
}

func ReadPage(file *os.File, page_id uint32) (uint8, *FileHeaderPage, *NodePage, *DataPage, error) {

	buf, err := ReadChunk(file, page_id)
	if err != nil {
		return 0, nil, nil, nil, errors.Wrap(err, fmt.Sprintf("error while trying to read %v page from file", page_id))
	}
	buf_reader := bytes.NewReader(buf)
	var temp Page
	binary.Read(buf_reader, NativeEndian, &temp)

	if temp.Identification_num != PAGE_IDENTITY_NUM {
		return Page_type_ids["Free"], nil, nil, nil, nil
		// return 0, nil, nil, nil, errors.New(fmt.Sprintf("read random bytes instead of the page while trying to read page_id = %v. read ident_num = 0x%X", page_id, temp.Identification_num))
	}

	buf_reader.Reset(buf)

	if temp.Page_type == Page_type_ids["FileHeader"] {
		var temp2 FileHeaderPage
		binary.Read(buf_reader, NativeEndian, &temp2)
		return Page_type_ids["FileHeader"], &temp2, nil, nil, nil

	} else if temp.Page_type == Page_type_ids["Node"] {
		var temp2 NodePage
		binary.Read(buf_reader, NativeEndian, &temp2)
		return Page_type_ids["Node"], nil, &temp2, nil, nil

	} else if temp.Page_type == Page_type_ids["Data"] {
		var temp2 DataPage
		binary.Read(buf_reader, NativeEndian, &temp2)
		return Page_type_ids["Data"], nil, nil, &temp2, nil

	}
	return 0, nil, nil, nil, nil
}

func DisconnectDB(file *os.File, file_header *FileHeaderPage) {

	err := WriteChunk(file, 0, Data_to_Bytes(file_header))
	if err != nil {
		fmt.Printf("error while trying to save file_header to the db file: \n%+v\n", err)
		return
	}
	err = file.Close()
	if err != nil {
		fmt.Printf("error while trying to close the db file:\n%+v\n", err)
	}
}

func overlap_intervals_pages(a, b free_space_table_row) bool {
	// a and b should be in sorted order
	if a.Page_id < b.Page_id {
		return a.Page_id+uint32(a.Num_pages) == b.Page_id
	} else if a.Page_id > b.Page_id {
		return b.Page_id+uint32(b.Num_pages) == a.Page_id
	}
	return true
}

func SpaceTableFixer(file_header *FileHeaderPage) {

	i := uint16(0)
	j := uint16(0)
	for i < file_header.Space_table_size && j < file_header.Space_table_size {
		for j < file_header.Space_table_size && file_header.Free_space_table[j].Page_id == 0 {
			j++
		}
		if j >= file_header.Space_table_size {
			break
		}
		if i != j {
			file_header.Free_space_table[i].Page_id = file_header.Free_space_table[j].Page_id
			file_header.Free_space_table[i].Num_pages = file_header.Free_space_table[j].Num_pages

			file_header.Free_space_table[j].Page_id = 0
			file_header.Free_space_table[j].Num_pages = 0
		}
		i++
		j++
	}
	file_header.Space_table_size = i

	// {9,2} {4,4} {11,1} {2,2} {1,1}
	sort.Slice(file_header.Free_space_table[:file_header.Space_table_size], func(i, j int) bool {
		a := file_header.Free_space_table[i]
		b := file_header.Free_space_table[j]
		return a.Page_id > b.Page_id
	})
	// {11,1} {9,2} {4,4} {2,2} {1,1}

	i = file_header.Space_table_size - 1
	j = 0
	for i < file_header.Space_table_size {
		j = i - 1
		for j < file_header.Space_table_size && overlap_intervals_pages(file_header.Free_space_table[i], file_header.Free_space_table[j]) {
			file_header.Free_space_table[i].Num_pages += file_header.Free_space_table[j].Num_pages
			file_header.Free_space_table[j].Num_pages = 0
			file_header.Free_space_table[j].Page_id = 0
			j--
		}
		i = j
	}

	i = 0
	j = 0
	for i < file_header.Space_table_size && j < file_header.Space_table_size {
		for j < file_header.Space_table_size && file_header.Free_space_table[j].Page_id == 0 {
			j++
		}
		if j >= file_header.Space_table_size {
			break
		}
		if i != j {
			file_header.Free_space_table[i].Page_id = file_header.Free_space_table[j].Page_id
			file_header.Free_space_table[i].Num_pages = file_header.Free_space_table[j].Num_pages

			file_header.Free_space_table[j].Page_id = 0
			file_header.Free_space_table[j].Num_pages = 0
		}
		i++
		j++
	}
	file_header.Space_table_size = i
}

func Trim_db_file(file_header *FileHeaderPage, file *os.File) error {

	file_stats, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "coudnt get the file stats")
	}
	if file_stats.Size()%PAGESIZE != 0 {
		return errors.New(fmt.Sprintf("DB size does not satisfy the implicit assumption of being multiple of 4096 bytes. This could mean that one of the pages being saved in the DB exceed or subceed the PAGESIZE (=%dB). This is should be fixed right away, as without this the DB will fail catastrophically", PAGESIZE))
	}
	num_pages_in_db := uint32(file_stats.Size() / PAGESIZE)

	var buf []byte
	var buf_reader *bytes.Reader
	var read_page Page
	var i uint32
	var j uint32

	for i = 0; i < num_pages_in_db; i++ {
		buf, err = ReadChunk(file, num_pages_in_db-i-1)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error in reading page %v while trying to visualize the db", num_pages_in_db-i-1))
		}
		buf_reader = bytes.NewReader(buf)
		binary.Read(buf_reader, NativeEndian, &read_page)

		if read_page.Identification_num != PAGE_IDENTITY_NUM {
			j++
		} else if read_page.Page_type == Page_type_ids["FileHeader"] {
			break
		} else if read_page.Page_type == Page_type_ids["Node"] {
			break
		} else if read_page.Page_type == Page_type_ids["Data"] {
			break
		} else {
			j++
		}
	}

	err = file.Truncate(int64(num_pages_in_db-j) * PAGESIZE)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to truncate the file to size %v from %v", int64(num_pages_in_db-j)*PAGESIZE, file_stats.Size()))
	}

	page_id_to_find := num_pages_in_db - uint32(j)

	// file_header.Free_space_table should most probably be sorted in decreasing order before the execution of this code
	// And hence this for loop should break at 0th index whenever that is true
	var changed int = -1
	for i := 0; i < int(file_header.Space_table_size); i++ {
		p, k := file_header.Free_space_table[i].Page_id, file_header.Free_space_table[i].Num_pages
		if page_id_to_find >= p && page_id_to_find < p+uint32(k) {
			file_header.Free_space_table[i].Num_pages -= uint16(j)
			changed = i
			break
		}
	}

	if changed != -1 {
		if file_header.Free_space_table[changed].Num_pages == 0 {
			file_header.Free_space_table[changed].Page_id = 0
			// Shift all the cells to the left
			for t := changed; t < int(file_header.Space_table_size)-1; t++ {
				// file_header.Free_space_table[t].Page_id, file_header.Free_space_table[t].Num_pages = file_header.Free_space_table[t+1].Page_id, file_header.Free_space_table[t+1].Num_pages
				file_header.Free_space_table[t] = file_header.Free_space_table[t+1]
			}
			file_header.Free_space_table[file_header.Space_table_size-1] = free_space_table_row{0, 0}
			file_header.Space_table_size -= 1
		}
	}

	return nil
}

func DeletePage(page_id uint32, file_header *FileHeaderPage, file *os.File) error {

	if page_id == 0 {
		return errors.New("cannot delete the file header page of the db without deleting the db")
	}

	pg_type, _, np, dp, err := ReadPage(file, page_id)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to read page id = %v", page_id))
	}
	if pg_type == Page_type_ids["Free"] {
		return nil
		// return errors.New(fmt.Sprintf("read random bytes instead of the page while trying to read page_id = %v", page_id))
	}

	empty_buffer := Data_to_Bytes(Page{})

	if pg_type == Page_type_ids["Data"] {
		// Should I allow deletion of data pages with data inside it? ---- Yes, atleast now, since this function will only be run by approved code
		// if dp.Data_held != 0 {
		// 	return errors.New(fmt.Sprintf("cannot delete a data page %v with still data inside of it", page_id))
		// }
		if dp.Next_data_page != 0 {
			err = DeletePage(dp.Next_data_page, file_header, file)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error while trying to delete page_id = %v which is the next_data_page of data page id = %v", dp.Next_data_page, page_id))
			}
		}

		file_header.Total_data_size -= uint64(dp.Data_held)
		err = WriteChunk(file, page_id, empty_buffer)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while trying to overwrite the page index = %v", page_id))
		}

	} else if pg_type == Page_type_ids["Node"] {
		// Should I allow deletion of node pages with data inside it? ---- Yes, atleast now, since this function will only be run by approved code
		// if np.Block_size != 0 {
		// 	return errors.New(fmt.Sprintf("cannot delete a node page %v with still data inside of it", page_id))
		// }
		err = DeletePage(np.Data_page_id, file_header, file)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while trying to delete the data page (%v) associated with the node page at page_id = %v (node)", np.Data_page_id, page_id))
		}

		err = WriteChunk(file, page_id, empty_buffer)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while trying to overwrite the page index = %v", page_id))
		}

	} else if pg_type == Page_type_ids["FileHeader"] {
		return errors.New(fmt.Sprintf("unexpectedly found file header page deletion request at page_id = %v", page_id))

	} else {
		return nil
	}

	file_header.Total_pages -= 1

	if file_header.Space_table_size == num_free_space_entries_file_header {
		// the Free Space table is full -> So, defragment the whole db_file to make a fresh start
		defragment_db_file(file_header, file)

	} else if file_header.Space_table_size != 0 {
		i := file_header.Space_table_size
		file_header.Free_space_table[i].Page_id = page_id
		file_header.Free_space_table[i].Num_pages = 1
		file_header.Space_table_size += 1
		SpaceTableFixer(file_header)

	} else {
		file_header.Space_table_size = 1
		file_header.Free_space_table[0].Page_id = page_id
		file_header.Free_space_table[0].Num_pages = 1
	}

	return nil
}

func put_nodeId_to_data_page(node_page_id uint32, data_page_id uint32, file *os.File) error {

	pt, _, _, dp, err := ReadPage(file, data_page_id)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to read the data page %v", data_page_id))
	}
	if pt != Page_type_ids["Data"] {
		return errors.New(fmt.Sprintf("expected to find data page but didn't find data at page_id = %v. found page type = %v", data_page_id, pt))
	}

	dp.Parent_node_page = node_page_id
	err = WriteChunk(file, data_page_id, Data_to_Bytes(dp))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("error while trying to uodate the data page id %v", data_page_id))
	}

	if dp.Next_data_page != uint32(0) {
		err = put_nodeId_to_data_page(node_page_id, dp.Next_data_page, file)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error while trying to put the parent node id in next data page %v", dp.Next_data_page))
		}
	}

	return nil
}

func MakeNewPage(page_type uint8, file_header *FileHeaderPage, file *os.File) (uint32, error) {

	if page_type == Page_type_ids["FileHeader"] {
		return 0, errors.New("invalid request! cannot make another file header inside an existing db file")
	}

	var buf []byte
	var data_page_id uint32
	var err error

	if page_type == Page_type_ids["Node"] {
		data_page_id, err = MakeNewPage(Page_type_ids["Data"], file_header, file)
		if err != nil {
			return 0, errors.Wrap(err, "could not make the data page for the node page")
		}
		temp := NodePage{
			Identification_num: PAGE_IDENTITY_NUM,
			Page_type:          Page_type_ids["Node"],
			Data_page_id:       data_page_id,
		}
		buf = Data_to_Bytes(temp)

	} else if page_type == Page_type_ids["Data"] {
		temp := DataPage{
			Identification_num: PAGE_IDENTITY_NUM,
			Page_type:          Page_type_ids["Data"],
			Space_table_size:   1,
		}
		temp.Unallocated_space_table[0] = data_page_unallocated_space_table_row{
			Offset: 0,
			Size:   uint16(len(temp.Data)),
		}
		buf = Data_to_Bytes(temp)

	} else {
		return 0, errors.New(fmt.Sprintf("invalid input! recieved request to make a page of unknown type = %v", page_type))
	}

	var page_id uint32

	if file_header.Space_table_size != 0 {

		i := file_header.Space_table_size - 1
		page_id = file_header.Free_space_table[i].Page_id
		file_header.Free_space_table[i].Num_pages -= 1
		file_header.Free_space_table[i].Page_id += 1

		if file_header.Free_space_table[i].Num_pages == 0 {
			file_header.Free_space_table[i].Page_id = 0
			file_header.Space_table_size -= 1
		}

	} else {
		page_id = file_header.Total_pages
	}

	err = WriteChunk(file, page_id, buf)
	if err != nil {
		if page_type == Page_type_ids["Node"] {
			err = DeletePage(data_page_id, file_header, file)
			if err != nil {
				return 0, errors.Wrap(err, fmt.Sprintf("error while writing a node to page index = %v and deleting the extra data page %v -> this page needs to be deleted", page_id, data_page_id))
			}
		}
		return 0, errors.Wrap(err, fmt.Sprintf("error while writing to page index = %v", page_id))
	}
	file_header.Total_pages += 1

	if page_type == Page_type_ids["Node"] {
		// The node page has the data page id but the data page id doesn't
		// So, we recursively put the node id in all of those data pages
		err = put_nodeId_to_data_page(page_id, data_page_id, file)
		if err != nil {
			return 0, errors.Wrap(err, fmt.Sprintf("error while trying to put the node id (%v) into the associated data pages", page_id))
		}
	}

	return page_id, nil
}

func defragment_db_file(file_header *FileHeaderPage, file *os.File) error {

	changes_record := make(map[uint32]uint32)

	file_stats, err := file.Stat()
	if err != nil {
		return err
	}
	num_pages := file_stats.Size() / PAGESIZE

	empty_page := Page{}

	// Defragment- Move the pages from non-empty to empty and concentrate them
	j := 1
	k := 1
	for j < int(num_pages) && k < int(num_pages) {
		pt, fp, np, dp, err := ReadPage(file, uint32(k))
		if err != nil {
			return err
		}

		if pt == Page_type_ids["Node"] || pt == Page_type_ids["Data"] || pt == Page_type_ids["FileHeader"] {
			// Filled Page needs to be moved to j_th position (Free Page)
			if j == k {
				j++
				k++
				continue
			}

			changes_record[uint32(k)] = uint32(j)
			if pt == Page_type_ids["Node"] {
				err = WriteChunk(file, uint32(j), Data_to_Bytes(np))
				if err != nil {
					return err
				}
			}
			if pt == Page_type_ids["Data"] {
				err = WriteChunk(file, uint32(j), Data_to_Bytes(dp))
				if err != nil {
					return err
				}
			}
			if pt == Page_type_ids["FileHeader"] {
				err = WriteChunk(file, uint32(j), Data_to_Bytes(fp))
				if err != nil {
					return err
				}
			}
			err = WriteChunk(file, uint32(k), Data_to_Bytes(empty_page))
			if err != nil {
				return err
			}
			j++
			k++
			continue
		}
		// This is a free page, increment k for all such free pages
		k++
	}

	// Change the file_header
	if val, ok := changes_record[file_header.Root_node_id]; ok {
		file_header.Root_node_id = val
	}
	for i := 0; i < int(file_header.Space_table_size); i++ {
		file_header.Free_space_table[i] = free_space_table_row{0, 0}
	}
	file_header.Space_table_size = 0

	// Relay the changes in the page ids to every individual page
	for i := 1; i < int(num_pages); i++ {
		pt, _, np, dp, err := ReadPage(file, uint32(i))
		if err != nil {
			return err
		}

		if pt == Page_type_ids["Node"] {
			if val, ok := changes_record[np.Data_page_id]; ok {
				np.Data_page_id = val
			}
			for j := 0; j < int(np.Block_size)+1; j++ {
				if val, ok := changes_record[np.Children[j]]; ok {
					np.Children[j] = val
				}
			}
			err = WriteChunk(file, uint32(i), Data_to_Bytes(np))
			if err != nil {
				return err
			}
		}
		if pt == Page_type_ids["Data"] {
			if val, ok := changes_record[dp.Next_data_page]; ok {
				dp.Next_data_page = val
			}
			if val, ok := changes_record[dp.Parent_node_page]; ok {
				dp.Parent_node_page = val
			}
			err = WriteChunk(file, uint32(i), Data_to_Bytes(dp))
			if err != nil {
				return err
			}
		}
	}

	// Fix the database file
	SpaceTableFixer(file_header)
	err = Trim_db_file(file_header, file)
	if err != nil {
		return err
	}

	return Trim_db_file(file_header, file)
}

func SavePage(page_id uint32, page_data []byte, file_header *FileHeaderPage, file *os.File) error {

	err := WriteChunk(file, page_id, page_data)
	if err != nil {
		return err
	}
	return nil
}
