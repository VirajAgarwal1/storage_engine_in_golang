/*
	BIG ENDIAN is followed for saving data.

	Each DB file will be implicitly partitioned into 4kB (4 kilo Byte) slabs. This is strict and should not be overriden

	Each 4kB slab is reffered to as a page. Since they can be accessed in one disk access. Run
	`grep -ir pagesize /proc/self/smaps` to check appropriate page size on any UNIX like system. This code however
	works only with 4kB page sizes and wil require considerable changes to work for other page sizes including rethinking
	a maority of design decisions made.

	EVERY page starts with a 4B random number (common number for all pages) which helps in identifying that what we are
	reading is indeed a page and not a random sequence of bytes.

	The first page of the file is will always be the File Header page.

	Page Types allowed along wit there ids:
		PAGE_TYPE			TYPE_ID
		FileHeader				21
		Node					33
		Data					45
*/

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
)

var NativeEndian = binary.BigEndian // Can be changed as its system independant

const MAX_DEGREE int = 336 // [DO NOT CHANGE] MAX_DEGREE = 4 means that in one node at most 3 elements can be there and at most 4 children of that node
const min_block_size int = (MAX_DEGREE / 2) - 1

const PAGESIZE = 4096                          // 4KB // DO NOT CHANGE
const PAGE_IDENTITY_NUM uint32 = 0x6EBC061F    // 4B Random Number used to identify if read memory is actually a page or not. First 4B of EVERY page is this number
const num_free_space_entries_file_header = 200 // Max Value of 200
const data_page_space_table_num_entries = 123  // Max value of 123
const node_page_header_size = 60               // [DO NOT CHANGE]
var Page_type_ids map[string]uint8 = map[string]uint8{"FileHeader": 21, "Node": 33, "Data": 45, "Free": 0}

// General structure of a page
type Page struct {
	Identification_num uint32
	Page_type          uint8
	_                  [PAGESIZE - (4 + 1)]byte
}

// Structure of the File Header
type free_space_table_row struct {
	Page_id   uint32
	Num_pages uint16
}
type FileHeaderPage struct {
	Identification_num uint32
	Page_type          uint8
	Total_pages        uint32 // Includes count of all Pages in the DB (Data, Node and even this FileHeader page as well) // This is important for writing to the DB
	Total_data_size    uint64
	Root_node_id       uint32
	Space_table_size   uint16
	Free_space_table   [num_free_space_entries_file_header]free_space_table_row
	_                  [PAGESIZE - (4 + 1 + 4 + 8 + 4 + 2 + (num_free_space_entries_file_header * (4 + 2)))]byte
}

// Structure of the DataPage
type data_page_unallocated_space_table_row struct {
	Offset uint16
	Size   uint16
}
type DataPage struct {
	// Header Start
	Identification_num      uint32
	Page_type               uint8
	Data_held               uint16
	Next_data_page          uint32
	Parent_node_page        uint32
	Space_table_size        uint16
	Unallocated_space_table [data_page_space_table_num_entries]data_page_unallocated_space_table_row
	_                       [512 - (4 + 1 + 2 + 4 + 4 + 2 + (data_page_space_table_num_entries * (2 + 2)))]byte
	// Header End
	Data [PAGESIZE - 512]byte
}

// Structure of the NodePage
type node_page_cell_offet struct {
	Key    uint32
	Offset uint32
}
type NodePage struct {
	// Header Start
	Identification_num uint32
	Page_type          uint8
	Data_page_id       uint32
	Block_size         uint16
	_                  [node_page_header_size - (4 + 1 + 4 + 2)]byte
	// Header End
	Blocks   [MAX_DEGREE]node_page_cell_offet // 8*MAX_DEGREE = 2688 Bytes
	Children [MAX_DEGREE + 1]uint32
}

// Read and Write to a file in pages
func ReadChunk(file *os.File, pageIndex uint32) ([]byte, error) {
	// Calculate the byte offset for the specified chunk
	offset := int64(pageIndex) * PAGESIZE

	file_stats, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error while trying to get stats of the file")
	}
	file_size := file_stats.Size()
	if offset >= file_size {
		return nil, errors.New(fmt.Sprintf("Specified pageIndex = %v is out of the scope of the file", pageIndex))
	}

	// Seek to the specified offset
	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error seeking to offset %d", offset))
	}

	// Create a buffer to hold the chunk data
	buffer := make([]byte, PAGESIZE)

	// Read the chunk into the buffer
	bytesRead, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, fmt.Sprintf("error reading chunk at index %d", pageIndex))
	}

	// If fewer bytes were read, adjust the buffer size
	return buffer[:bytesRead], nil
}
func WriteChunk(file *os.File, pageIndex uint32, data []byte) error {
	// Calculate the byte offset for the specified chunk
	offset := int64(pageIndex) * PAGESIZE

	// Get the current file size
	fileInfo, err := file.Stat()
	if err != nil {
		return errors.Wrap(err, "error getting file info")
	}
	currentSize := fileInfo.Size()

	// Determine where to write: at the end of the file or at the specified offset
	if offset >= currentSize {
		// If the offset is beyond the file size, write at the end of the file
		_, err := file.Seek(0, io.SeekEnd)
		if err != nil {
			return errors.Wrap(err, "error seeking to end of file")
		}
	} else {
		// Otherwise, seek to the specified offset
		_, err := file.Seek(offset, io.SeekStart)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error seeking to offset %d", offset))
		}
	}

	// Ensure the data is exactly 4KB
	if len(data) != PAGESIZE {
		return errors.New(fmt.Sprintf("data must be exactly %d bytes", PAGESIZE))
	}

	// Write the data to the file
	_, err = file.Write(data)
	if err != nil {
		return errors.Wrap(err, "error writing to chunk")
	}

	return nil
}

// Conversion between data and array of bytes
func Data_to_Bytes(data any) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, NativeEndian, data)
	return buf.Bytes()
}

/*Bytes_to_Data
can be done with the code
	buf := bytes.NewReader(ip)
	binary.Read(buf, NativeEndian, op)
*/
