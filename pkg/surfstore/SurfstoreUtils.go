package surfstore

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	reflect "reflect"
)

func getFilelist(baseDir string) []string {

	files, err := ioutil.ReadDir(baseDir)
	if err != nil {
		log.Fatal(err)
	}
	var fileList []string
	// Get file
	for _, file := range files {
		if file.Name() != DEFAULT_META_FILENAME {
			fileList = append(fileList, file.Name())
		}
	}
	return fileList
}

//
func synchroLocalMeta(localFileMetaMap map[string]*FileMetaData, fileList []string, client RPCClient) map[string]*FileMetaData {
	for _, fileName := range fileList {
		//File in basedir and in localMAP , not new need compare and refresh
		if _, ok := localFileMetaMap[fileName]; ok {
			fileBlockHashList := transFileToHashString(fileName, client)

			if len(fileBlockHashList) != len(localFileMetaMap[fileName].BlockHashList) {
				localFileMetaMap[fileName].BlockHashList = fileBlockHashList
				localFileMetaMap[fileName].Version++
			} else {
				if len(localFileMetaMap[fileName].BlockHashList) == 0 {
					var fileFMD FileMetaData
					fileFMD.Filename = fileName
					fileFMD.Version = 1
					fileFMD.BlockHashList = transFileToHashString(fileName, client)
					localFileMetaMap[fileName] = &fileFMD
				} else if localFileMetaMap[fileName].BlockHashList[0] == "0" {
					// var fileFMD FileMetaData
					// fileFMD.Filename = fileName
					// fileFMD.Version = 1
					// fileFMD.BlockHashList = transFileToHashString(fileName, client)
					// localFileMetaMap[fileName] = &fileFMD
					for i, _ := range fileBlockHashList {
						if fileBlockHashList[i] != localFileMetaMap[fileName].BlockHashList[i] {
							localFileMetaMap[fileName].BlockHashList = fileBlockHashList
							localFileMetaMap[fileName].Version++
							break
						}
					}

				} else if localFileMetaMap[fileName].BlockHashList[0] != "0" {

					for i, _ := range fileBlockHashList {
						if fileBlockHashList[i] != localFileMetaMap[fileName].BlockHashList[i] {
							localFileMetaMap[fileName].BlockHashList = fileBlockHashList
							localFileMetaMap[fileName].Version++
							break
						}
					}

				}
			}
		} else { //File in basedir but not in localMAP :new file
			var fileFMD FileMetaData
			fileFMD.Filename = fileName
			fileFMD.Version = 1
			fileFMD.BlockHashList = transFileToHashString(fileName, client)
			localFileMetaMap[fileName] = &fileFMD
		}
	}

	//File in localMAP , but not in dir: Delete file local v++
	log.Println(localFileMetaMap)
	for key, _ := range localFileMetaMap {
		In := false
		for _, fileName := range fileList {
			if key == fileName {
				In = true
				break
			}
		}
		if !In && len(localFileMetaMap[key].BlockHashList) == 0 {
			localFileMetaMap[key].BlockHashList = make([]string, 1)
			localFileMetaMap[key].BlockHashList[0] = "0"
			localFileMetaMap[key].Version++
		} else if !In && localFileMetaMap[key].BlockHashList[0] != "0" {
			// delete(localFileMetaMap, key)
			localFileMetaMap[key].BlockHashList = make([]string, 1)
			localFileMetaMap[key].BlockHashList[0] = "0"
			localFileMetaMap[key].Version++
			log.Print("deleted========")
		}
		// else if !In {
		// 	localFileMetaMap[key].BlockHashList = make([]string, 1)
		// 	localFileMetaMap[key].BlockHashList[0] = "0"
		// 	localFileMetaMap[key].Version++
		// }
	}

	log.Println(localFileMetaMap)
	return localFileMetaMap
}

//
func transFileToHashString(fileName string, client RPCClient) []string {
	var fileBlockHashList []string
	filePath := ConcatPath(client.BaseDir, fileName)
	fobj, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}

	defer fobj.Close()
	reader := bufio.NewReader(fobj)

	for {
		buf := make([]byte, client.BlockSize)
		size, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		singleBlock := buf[:size]
		singleBlockHash := GetBlockHashString(singleBlock)
		fileBlockHashList = append(fileBlockHashList, singleBlockHash)
	}
	return fileBlockHashList
}

//
func syncLocalRemote(localFileMetaMap *(map[string]*FileMetaData), serverFileMetaMap *(map[string]*FileMetaData), client *RPCClient) error {

	if len(*serverFileMetaMap) == 0 {
		*serverFileMetaMap = make(map[string]*FileMetaData)
	}
	for k, _ := range *localFileMetaMap {
		log.Println("IN")
		//in local not in remote - new file
		fmt.Println(*serverFileMetaMap)
		fmt.Println(*localFileMetaMap)

		if _, ok := (*serverFileMetaMap)[k]; !ok {
			//updata
			if len((*localFileMetaMap)[k].BlockHashList) == 0 {
				(*serverFileMetaMap)[k] = (*localFileMetaMap)[k]
				(*serverFileMetaMap)[k].BlockHashList = make([]string, 0)
				var v Version
				client.UpdateFile((*serverFileMetaMap)[k], &v.Version)
			} else if (*localFileMetaMap)[k].BlockHashList[0] != "0" {
				(*serverFileMetaMap)[k] = (*localFileMetaMap)[k]
				// (*serverFileMetaMap)[k] = vvv
				var v Version
				client.UpdateFile((*serverFileMetaMap)[k], &v.Version)
				log.Println((*serverFileMetaMap)[k])
				log.Println("update success")
			}
			// for kk,vv:= range (*serverFileMetaMap)[k].BlockHashList {}
			// client.PutBlock()

		} else {
			//both in
			// Version compare

			if (*localFileMetaMap)[k].Version > (*serverFileMetaMap)[k].Version {
				// update
				(*serverFileMetaMap)[k] = (*localFileMetaMap)[k]
				var v Version
				client.UpdateFile((*serverFileMetaMap)[k], &v.Version)
				// client.PutBlock()

			} else if (*localFileMetaMap)[k].Version < (*serverFileMetaMap)[k].Version {
				// download
				(*localFileMetaMap)[k] = (*serverFileMetaMap)[k]

			} else { // v=v
				flag := reflect.DeepEqual((*serverFileMetaMap)[k].BlockHashList, (*localFileMetaMap)[k].BlockHashList)
				if !flag {
					// download
					(*localFileMetaMap)[k] = (*serverFileMetaMap)[k] //differ download
				}
			}
		}
	}

	// in remote but not in local - (DOWNLOAD!)
	for k, _ := range *serverFileMetaMap {
		if _, ok := (*localFileMetaMap)[k]; !ok {

			// if (*serverFileMetaMap)[k].BlockHashList[0] != "0" {
			// delete(localFileMetaMap, key)
			// down load
			// (*serverFileMetaMap)[k].BlockHashList = make([]string, 1)
			// (*serverFileMetaMap)[k].BlockHashList[0] = "0"
			// (*serverFileMetaMap)[k].Version++
			(*localFileMetaMap)[k] = (*serverFileMetaMap)[k]
			var v Version
			client.UpdateFile((*serverFileMetaMap)[k], &v.Version)
			// client.PutBlock()
			// }
		}
	}
	log.Println(*serverFileMetaMap)
	log.Println(*localFileMetaMap)
	return nil
}

func getAllLocalHashes(fileList []string, client RPCClient) (map[string][]byte, []string) {
	localBlockMap := make(map[string][]byte)
	var localBlockHashList []string
	for _, fileName := range fileList {

		filePath := ConcatPath(client.BaseDir, fileName)
		fobj, err := os.Open(filePath)
		if err != nil {
			log.Fatal(err)
		}
		defer fobj.Close()
		reader := bufio.NewReader(fobj)

		for {
			buf := make([]byte, client.BlockSize)
			size, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					log.Fatal(err)
				}
			}
			singleBlock := buf[:size]
			singleBlockHash := GetBlockHashString(singleBlock)
			localBlockMap[singleBlockHash] = singleBlock
			localBlockHashList = append(localBlockHashList, singleBlockHash)
		}
	}
	return localBlockMap, localBlockHashList
}

func intersect(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	for _, v := range slice1 {
		m[v]++
	}

	for _, v := range slice2 {
		times := m[v]
		if times == 1 {
			nn = append(nn, v)
		}
	}
	return nn
}

//求差集 slice1-并集
func difference(slice1, slice2 []string) []string {
	m := make(map[string]int)
	nn := make([]string, 0)
	inter := intersect(slice1, slice2)
	for _, v := range inter {
		m[v]++
	}

	for _, value := range slice1 {
		times := m[value]
		if times == 0 {
			nn = append(nn, value)
		}
	}
	return nn
}

func writeFile(localFileMetaMap map[string]*FileMetaData, client RPCClient, localBlockMap map[string][]byte) error {
	for key, value := range localFileMetaMap {
		if len(localFileMetaMap[key].BlockHashList) > 0 {
			if localFileMetaMap[key].BlockHashList[0] == "0" {
				continue
			}
		}
		fileName := ConcatPath(client.BaseDir, key)
		fwriter, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)

		bw := bufio.NewWriter(fwriter)
		if err != nil {
			return err
		}

		blockhashlist := value.BlockHashList
		for _, hash := range blockhashlist {
			if _, err := bw.Write(localBlockMap[hash]); err != nil {
				return err
			}
			if err := bw.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func deleteFile(localFileMetaMap map[string]*FileMetaData, client RPCClient, fileList []string) {
	for _, fileName := range fileList {
		if _, ok := localFileMetaMap[fileName]; ok {
			if len(localFileMetaMap[fileName].BlockHashList) > 0 {
				if localFileMetaMap[fileName].BlockHashList[0] == "0" {
					os.Remove(ConcatPath(client.BaseDir, fileName))
				}
			}
		}
	}
}

//Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	// panic("todo")
	fmt.Println("use my client sync!!")
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil { //blockStoreAddr is the ADDR
		log.Fatal(err)
	}

	// var localFileMetaMap map[string]*FileMetaData

	localFileMetaMap, err := LoadMetaFromMetaFile(client.BaseDir)

	if err != nil {
		log.Fatal(err)
	}
	//Get all the current base dir file list
	fileList := getFilelist(client.BaseDir)

	//build local hash block list
	localBlockMap, localBlockHashList := getAllLocalHashes(fileList, client)

	//sync base dir localMateMap  localFileMetaMap ?= fileList
	localFileMetaMap = synchroLocalMeta(localFileMetaMap, fileList, client)
	log.Println("===local syn")
	log.Println(localFileMetaMap)
	//sync localMateMap remoteMetaMap
	var serverFileMetaMap map[string]*FileMetaData
	_ = client.GetFileInfoMap(&serverFileMetaMap)

	_ = syncLocalRemote(&localFileMetaMap, &serverFileMetaMap, &client)
	log.Println("after .....")
	log.Println(serverFileMetaMap)
	log.Println(localFileMetaMap)
	log.Println("////////")
	var ss map[string]*FileMetaData
	_ = client.GetFileInfoMap(&ss)
	log.Println(ss)
	PrintMetaMap(ss)
	PrintMetaMap(localFileMetaMap)

	WriteMetaFile(localFileMetaMap, client.BaseDir)

	//sync blocks up and down
	//up
	blockHashesOut := make([]string, 1)
	log.Printf(blockHashesOut[0])
	_ = client.HasBlocks(localBlockHashList, blockStoreAddr, &blockHashesOut)
	upHashlist := difference(localBlockHashList, blockHashesOut)
	if len(blockHashesOut) == 0 {
		blockHashesOut = make([]string, 1)
	}
	log.Printf(blockHashesOut[0])
	// log.Printf(upHashlist[0])
	log.Println(localBlockMap)

	if len(upHashlist) > 0 {
		for _, upHash := range upHashlist {

			succ := false
			upHashBlock := Block{BlockData: localBlockMap[upHash], BlockSize: int32(client.BlockSize)}
			log.Println(upHashBlock)
			_ = client.PutBlock(&upHashBlock, blockStoreAddr, &succ)
		}
		log.Printf("put block success")
	}

	//down
	for _, FMD := range serverFileMetaMap {
		for _, hash := range FMD.BlockHashList {
			if _, ok := localBlockMap[hash]; !ok {
				var block Block
				client.GetBlock(hash, blockStoreAddr, &block)
				localBlockMap[hash] = block.BlockData
				localBlockHashList = append(localBlockHashList, hash)

				log.Printf("download block success")
			}
		}
	}

	//writeback
	WriteMetaFile(localFileMetaMap, client.BaseDir)
	_ = writeFile(localFileMetaMap, client, localBlockMap)
	deleteFile(localFileMetaMap, client, fileList)

	// blockHash := "1234"
	// var block Block
	// if err := client.GetBlock(blockHash, blockStoreAddr, &block); err != nil {
	// 	log.Fatal(err)
	// }
	// log.Print(block.String())
}
