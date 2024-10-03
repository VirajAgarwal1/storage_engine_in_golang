package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"

	"github.com/pkg/errors"
)

func postorder(node_id uint32, want_expanded_output bool, file_header *FileHeaderPage, file *os.File) error {
	var err error
	if node_id != 0 {
		err = Visualize_Page(node_id, file_header, file)
		if err != nil {
			return err
		}
		pt, _, node, _, err := ReadPage(file, node_id)
		if err != nil {
			return err
		}
		if pt != Page_type_ids["Node"] {
			return errors.New(fmt.Sprintf("read page %v isn't a nodepage. read page of type %v", node_id, pt))
		}
		if want_expanded_output {
			err = Visualize_Page(node.Data_page_id, file_header, file)
			if err != nil {
				return err
			}
		}
		for i := 0; i < int(node.Block_size)+1; i++ {
			err = postorder(node.Children[i], want_expanded_output, file_header, file)
			if err != nil {
				return err
			}
		}
		fmt.Println()
	}
	return nil
}

func shuffleSlice(slice []int) {
	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})
}

func test(db_name string) error {

	// Make a new DB
	file, file_header, err := Create_and_ConnectDB(db_name)
	if err != nil {
		return err
	}
	DisconnectDB(file, file_header)

	// Work with the newly created db file
	file, file_header, err = ConnectDB(db_name)
	if err != nil {
		return err
	}

	// Try putting data in the B-tree
	num_data := 500
	data_size := 120 // The Storage engine can handle this upto 180 till which equates to about 450 Bytes of data
	randomness_in_data_size := 10
	rand_num := 0
	data_arr := make(map[int]string)
	var data string
	for i := 0; i < num_data; i++ {
		data = ""
		rand_num = rand.Intn(randomness_in_data_size)
		for j := 0; j < data_size+rand_num; j++ {
			data += strconv.Itoa(j)
		}
		data_arr[i] = data
		err = Insert(uint32(i), []byte(data), file_header, file)
		if err != nil {
			return err
		}
	}
	err = VisualizeDB(file)
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println("Total_pages =", file_header.Total_pages)
	fmt.Println("Free_space_table =", file_header.Free_space_table[:max(10, file_header.Space_table_size)])
	fmt.Println("Space_table_size =", file_header.Space_table_size)
	fmt.Println("Total_data_size =", file_header.Total_data_size)
	fmt.Println("Root_node_id =", file_header.Root_node_id)
	fmt.Println()

	err = postorder(file_header.Root_node_id, false, file_header, file)
	if err != nil {
		return err
	}

	total_data_size := 0
	for key, value := range data_arr {
		data, found, err := Search(uint32(key), file_header.Root_node_id, file_header, file)
		if err != nil {
			return err
		}
		if !found {
			return errors.New(fmt.Sprintf("error while trying to search for key %v in the b-tree", key))
		}
		if value != string(data) {
			return errors.New(fmt.Sprintf("data saved (%v) doesn't equal, data asked to save (%v) for the key %v", data, value, key))
		}
		total_data_size += len(data)
	}
	fmt.Printf("\nALL THE DATA IS CORRECTLY SAVED IN THE DB\n\n")

	if file_header.Total_data_size != uint64(total_data_size) {
		return errors.New(fmt.Sprintf("total_data_size %v in file header is displayed wrong, expected %v", file_header.Total_data_size, total_data_size))
	}
	fmt.Printf("\nfile_header.Total_data_size %v == %v [Correct]\n\n", file_header.Total_data_size, total_data_size)

	fmt.Println()
	fmt.Printf("####################################################################################################################################\n")
	fmt.Printf("####################################################################################################################################\n")
	fmt.Printf("####################################################################################################################################\n")
	fmt.Println()

	// Try deleting some data from the B-tree
	nums := make([]int, num_data)
	for i := 0; i < num_data; i++ {
		nums[i] = i
	}
	shuffleSlice(nums)
	fmt.Printf("%v \n\n", nums)
	for _, num := range nums[:(num_data / 2)] {
		delete(data_arr, num)
		err = Delete(uint32(num), file_header, file)
		if err != nil {
			return err
		}
	}

	err = VisualizeDB(file)
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println("Total_pages =", file_header.Total_pages)
	fmt.Println("Free_space_table =", file_header.Free_space_table[:max(10, file_header.Space_table_size)])
	fmt.Println("Space_table_size =", file_header.Space_table_size)
	fmt.Println("Total_data_size =", file_header.Total_data_size)
	fmt.Println("Root_node_id =", file_header.Root_node_id)
	fmt.Println()

	err = postorder(file_header.Root_node_id, false, file_header, file)
	if err != nil {
		return err
	}

	// Check if all the inserted data is correct and there
	total_data_size = 0
	for key, value := range data_arr {
		data, found, err := Search(uint32(key), file_header.Root_node_id, file_header, file)
		if err != nil {
			return err
		}
		if !found {
			return errors.New(fmt.Sprintf("error while trying to search for key %v in the b-tree", key))
		}
		if value != string(data) {
			return errors.New(fmt.Sprintf("data saved (%v) doesn't equal, data asked to save (%v) for the key %v", data, value, key))
		}
		total_data_size += len(data)
	}
	fmt.Printf("\nALL THE DATA IS CORRECTLY SAVED IN THE DB\n\n")

	if file_header.Total_data_size != uint64(total_data_size) {
		return errors.New(fmt.Sprintf("total_data_size %v in file header is displayed wrong, expected %v", file_header.Total_data_size, total_data_size))
	}
	fmt.Printf("\nfile_header.Total_data_size %v == %v [Correct]\n\n", file_header.Total_data_size, total_data_size)

	DisconnectDB(file, file_header)
	err = os.Remove("./databases/" + db_name + ".db")
	if err != nil {
		return err
	}

	return nil
}

func main() {
	for i := 0; i < 1; i++ {
		err := test("aswd")
		if err != nil {
			fmt.Printf("%+v\n", err)
			panic(err)
		}
	}
}
