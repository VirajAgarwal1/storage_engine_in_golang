# Storage Engine with B-Trees
---

#### Overview

This project was made for learning about B-Trees and their role in the internals of the Database (DB). In this project, I have tried to make a DB from scratch, but due to sheer amount of functions needed to make a fully functional DB, I stopped the project at a storage engine. The difference is that this is purely for savely data in an orderly fashion and doesn't actually provide the ACID (Atomicity, Consistency, Isolation and Durability) properties of good databases. The `notes.xopp` and the `notes.pdf` files, are my handwritten notes made in Xournall++, through which I did all of the planning for the project and made all the decisions.

---
#### How to run this project

###### 1. If you have golang installed on your device: 
1.  Go to the directory where the project is saved `cd /project/directory/`
2.  Run the golang code `go run .` (the dot is important, since it selects everything in the directory)

###### 2. If you don't have golang installed:
Even if you don't have golang installed, I have already pre-build the code for Linux and Windows machines. So you if you are on Windows try running `b_tree_disk.exe` file. And if you are on Linux please try to run `b_tree_disk` file.

The output of the code will be displayed in the terminal itself. Currently, a set number of inserts, searches and deletes are being operated upon the storage engine in the `test` function in the `main.go` file. 

---

### Planning of the project

The coolest part of this project was to make pages in which data will saved and B-Tree nodes will be placed. These pages are directly placed upon the disk on chunks of `4 KiloBytes` And all the pages are placed in the database to which I have given a unique extension of `.db`. 

The project is built in 4 layers, with the following functions: (written in fashion of closest to hardware to furthest)

1. `structs.go` -> This layer handles all the pages and working with raw database file and handling how read and write data to the file. It also contains the structure for every page, which were also made from scratch by me.
2. `page_handling.go` -> This layer contains all the function you might need to interact with the logical pages, i.e. Add pages, Delete pages, Save pages and also importantly Defragment pages. Defragmentation is required since deleted pages can create a lot of free spaces in between which can cause problems.
3. `data_in_page_handling.go` -> This layer helps in achieving all the function one would need to work with data inside the logical pages. Eg, insert data, read data, delete data and defragment data. Since, we are using VARIABLE SIZED data inside our Storage engine, deletion will cause serious amounts of fragmentation and so it needs to be dealt with.
4. `b_tree.go` -> This layer handles the B-tree logic and integrates it with the 3rd layer.

I developed this project in the same order above, and so it could be helpful for someone to know this if he/she wants to try understand it.

---

### Remarks

I personally learned a lot by undertaking this challenge. This project increased my appreciation of Databases exponentially, beacuse I understand the humongous efforts needed to make one. Although, I did a lot to try make the performance better, it still is not upto what I would have liked. There are many optimisations which can be done to make it faster but, for now I will give this project a rest and work on something else. I am glad I undertook this project.
