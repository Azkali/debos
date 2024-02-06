/*
Split action

Split a disk image.
Useful for embedded device with fat32 size limitation.

 # Yaml syntax:
 - action: split
   file: disk.img
   name: name
   suffix: 0
   chunk: 4290772992
   output: out/

Mandatory properties:
- file -- Disk image relative to context.Artifactdir.

Optional properties:
- name -- Set the basename for each chunks. Chunk will get a number ranging from [0-9][0-9]
the default value is '<filename>'

- suffix -- Set the destination chunks suffix.
the default value is '0'

- chunk -- Specify the maximum size of each chunk in bytes.
the default value is 4290772992 (FAT32 max file size)

- output -- Output directory relative to context.Scratchdir.
the default value is context.Scratchdir

*/
package actions

import (
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"path"
	"os"
	"strconv"
	"github.com/go-debos/debos"
)

type SplitAction struct {
	debos.BaseAction `yaml:",inline"`
	File		 string
	Name 		 string
	Suffix		 string
	Output		 string
	Chunk	 	 uint64
}

func (pf *SplitAction) Verify(context *debos.DebosContext) error {
	if len(pf.File) == 0 {
		return fmt.Errorf("Missing `file`.")
	}
	return nil
}

func (pf *SplitAction) Run(context *debos.DebosContext) error {
	var File string
	var Name string
	var Suffix string
	var Chunk uint64
	var Output string

	if len(pf.Output) == 0 {
		Output = context.Scratchdir
	} else {
		Output = path.Join(context.Scratchdir, pf.Output + "/")
	}

	if len(pf.Name) == 0 {
		Name = pf.File
	} else {
		Name = pf.Name
	}

	if len(pf.Suffix) == 0 {
		Suffix = "0"
	} else {
		Suffix = pf.Suffix
	}

	if pf.Chunk == 0 {
		Chunk = 4290772992
	} else {
		Chunk = pf.Chunk
	}

	File = path.Join(context.Artifactdir, pf.File)

	Name = Name + "." + Suffix

	if _, err := os.Stat(Output); os.IsNotExist(err) {
	    err := os.MkdirAll(Output, 0644)
	    if err != nil {
		    log.Printf("Path already exists.")
	    }
	}

	file, err := os.Open(File)
	if err != nil {
		return fmt.Errorf("Couldn't open %s. Aborting.", File)
	}

	defer file.Close()
	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()
	chunksNum := uint64(math.Ceil(float64(fileSize) / float64(Chunk)))

	for i := uint64(0); i < chunksNum; i++ {
		pSize := int(math.Min(float64(Chunk), float64(fileSize - int64(i * Chunk))))
		pBuf := make([]byte, pSize)

		file.Read(pBuf)

		chunk := Output + "/" + Name + strconv.FormatUint(i, 10)
		_,  err := os.Create(chunk)
		if err != nil {
			return fmt.Errorf("No space available on disk. Aborting.")
		}

		ioutil.WriteFile(chunk, pBuf, os.ModeAppend)
		log.Printf("Created %s", chunk)
	}

	err = os.Remove(File)
	if err != nil {
		log.Printf("File doesn't exist.")
	}

	return nil
}
