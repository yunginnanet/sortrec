package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dsoprea/go-exif/v3"
)

const unknownDateFolderName = "date-unknown"

type Image struct {
	Time int64
	Path string
}

var (
	maxNumberOfFilesPerFolder int
	splitMonths               bool
	source                    string
	destination               string
	keepFilename              bool
	dateTimeFilename          bool
	minEventDeltaDays         int
)

func getMinimumCreationTime(exifData map[uint16]interface{}) *time.Time {
	var creationTime *time.Time

	if dateTimeTag, ok := exifData[0x9003]; ok {
		if dateTime, err := exif.GetDateTimeOriginal(dateTimeTag); err == nil {
			creationTime = &dateTime
		}
	} else if dateTimeTag, ok := exifData[0x9004]; ok {
		if dateTime, err := exif.GetDateTimeOriginal(dateTimeTag); err == nil {
			creationTime = &dateTime
		}
	} else if dateTimeTag, ok := exifData[0x0132]; ok {
		if dateTime, err := exif.GetDateTimeOriginal(dateTimeTag); err == nil {
			creationTime = &dateTime
		}
	}

	return creationTime
}

func postprocessImage(imagePath string, wg *sync.WaitGroup, imagesChan chan<- Image) {
	defer wg.Done()

	imageFile, err := os.Open(imagePath)
	if err != nil {
		fmt.Printf("Could not open image %s: %v\n", imagePath, err)
		return
	}
	defer imageFile.Close()

	exifData, err := exif.SearchFileAndExtractExif(imageFile)
	if err != nil {
		fmt.Printf("Invalid EXIF tags for %s\n", imagePath)
	}

	creationTime := getMinimumCreationTime(exifData)
	if creationTime == nil {
		fileInfo, err := os.Stat(imagePath)
		if err != nil {
			fmt.Printf("Could not get file info for %s: %v\n", imagePath, err)
			return
		}
		creationTime = &fileInfo.ModTime()
	}

	imagesChan <- Image{creationTime.Unix(), imagePath}
}

func createPath(newPath string) {
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		os.MkdirAll(newPath, os.ModePerm)
	}
}

func createNewFolder(destinationRoot, year, month string, eventNumber int) {
	var newPath string
	if month != "" {
		newPath = filepath.Join(destinationRoot, year, month, strconv.Itoa(eventNumber))
	} else {
		newPath = filepath.Join(destinationRoot, year, strconv.Itoa(eventNumber))
	}
	createPath(newPath)
}

func createUnknownDateFolder(destinationRoot string) {
	path := filepath.Join(destinationRoot, unknownDateFolderName)
	createPath(path)
}

func writeImages(images []Image, destinationRoot string, minEventDeltaDays int, splitByMonth bool) {
	minEventDelta := int64(minEventDeltaDays * 60 * 60 * 24)
	sort.Slice(images, func(i, j int) bool { return images[i].Time < images[j].Time })

	var previousTime int64
	eventNumber := 0
	var previousDestination string
	today := time.Now().Format("02/01/2006")

	for _, image := range images {
		var destination, destinationFilePath string
		t := time.Unix(image.Time, 0)
		year := t.Format("2006")
		var month string
		if splitByMonth {
			month = t.Format("01")
		}
		creationDate := t.Format("02/01/2006")
		fileName := filepath.Base(image.Path)

		if creationDate == today {
			createUnknownDateFolder(destinationRoot)
			destination = filepath.Join(destinationRoot, unknownDateFolderName)
			destinationFilePath = filepath.Join(destination, fileName)
		} else {
			if previousTime == 0 || (previousTime+minEventDelta) < image.Time {
				eventNumber++
				createNewFolder(destinationRoot, year, month, eventNumber)
			}

			previousTime = image.Time

			destComponents := []string{destinationRoot, year, month, strconv.Itoa(eventNumber)}
			validComponents := filterNonEmpty(destComponents)
			destination = filepath.Join(validComponents...)

			if _, err := os.Stat(destination); os.IsNotExist(err) {
				destination = previousDestination
			}

			previousDestination = destination
			destinationFilePath = filepath.Join(destination, fileName)
		}

		if !fileExists(destinationFilePath) {
			os.Rename(image.Path, destinationFilePath)
		} else {
			if fileExists(image.Path) {
				os.Remove(image.Path)
			}
		}
	}
}

func postprocessImages(imageDirectory string, minEventDeltaDays int, splitByMonth bool) {
	var wg sync.WaitGroup
	imagesChan := make(chan Image)

	go func() {
		wg.Wait()
		close(imagesChan)
	}()

	err := filepath.Walk(imageDirectory, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			go postprocessImage(filepath.Join(root, info.Name()), &wg, imagesChan)
		}
		return nil
	})
	if err != nil {
		fmt.Printf("Error walking the path %s: %v\n", imageDirectory, err)
		return
	}

	var images []Image
	for image := range imagesChan {
		images = append(images, image)
	}

	writeImages(images, imageDirectory, minEventDeltaDays, splitByMonth)
}

func getNumberOfFilesInFolderRecursively(startPath string) int {
	numberOfFiles := 0
	filepath.Walk(startPath, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			numberOfFiles++
		}
		return nil
	})
	return numberOfFiles
}

func log(logString string) {
	fmt.Printf("%s: %s\n", time.Now().Format("15:04:05"), logString)
}

func filterNonEmpty(strings []string) []string {
	var result []string
	for _, s := range strings {
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

func copyFile(src, dst string) {
	sourceFile, err := os.Open(src)
	if err != nil {
		fmt.Printf("Could not open source file %s: %v\n", src, err)
		return
	}
	defer sourceFile.Close()

	destinationFile, err := os.Create(dst)
	if err != nil {
		fmt.Printf("Could not create destination file %s: %v\n", dst, err)
		return
	}
	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, sourceFile)
	if err != nil {
		fmt.Printf("Error copying file from %s to %s: %v\n", src, dst, err)
	}
}

func limitFilesPerFolder(folder string, maxNumberOfFilesPerFolder int) {
	filepath.Walk(folder, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			dirPath := filepath.Join(root, info.Name())
			filesInFolder, err := os.ReadDir(dirPath)
			if err != nil {
				fmt.Printf("Could not read directory %s: %v\n", dirPath, err)
				return nil
			}
			if len(filesInFolder) > maxNumberOfFilesPerFolder {
				numberOfSubfolders := (len(filesInFolder)-1)/maxNumberOfFilesPerFolder + 1
				for subFolderNumber := 1; subFolderNumber <= numberOfSubfolders; subFolderNumber++ {
					subFolderPath := filepath.Join(dirPath, strconv.Itoa(subFolderNumber))
					createPath(subFolderPath)
				}
				fileCounter := 1
				for _, file := range filesInFolder {
					source := filepath.Join(dirPath, file.Name())
					if fileInfo, err := file.Info(); err == nil && fileInfo.Mode().IsRegular() {
						destDir := strconv.Itoa((fileCounter-1)/maxNumberOfFilesPerFolder + 1)
						destination := filepath.Join(dirPath, destDir, file.Name())
						os.Rename(source, destination)
						fileCounter++
					}
				}
			}
		}
		return nil
	})
}

func main() {
	flag.StringVar(&source, "src", "", "Source directory with files recovered by Photorec")
	flag.StringVar(&destination, "dest", "", "Destination directory to write sorted files to")
	flag.IntVar(&maxNumberOfFilesPerFolder, "max-per-dir", 500, "Maximum number of files per directory")
	flag.BoolVar(&splitMonths, "split-months", false, "Split JPEG files not only by year but by month as well")
	flag.BoolVar(&keepFilename, "keep_filename", false, "Keeps the original filenames when copying")
	flag.IntVar(&minEventDeltaDays, "min-event-delta", 4, "Minimum delta in days between two events")
	flag.BoolVar(&dateTimeFilename, "date_time_filename", false, "Sets the filename to the exif date and time if possible")

	flag.Parse()

	if source == "" || destination == "" {
		flag.Usage()
		os.Exit(1)
	}

	fmt.Printf("Reading from source '%s', writing to destination '%s' (max %d files per directory, splitting by year %v).\n",
		source, destination, maxNumberOfFilesPerFolder, splitMonths)

	if keepFilename {
		fmt.Println("I will keep your filenames as they are")
	} else if dateTimeFilename {
		fmt.Println("If possible I will rename your files like <Date>_<Time>.jpg - otherwise keep the filenames as they are")
	} else {
		fmt.Println("I will rename your files like '1.jpg'")
	}

	validateDirectory(&source, "Enter a valid source directory\n")
	validateDirectory(&destination, "Enter a valid destination directory\n")

	fileNumber := getNumberOfFilesInFolderRecursively(source)
	onePercentFiles := fileNumber / 100
	if fileNumber <= 100 {
		onePercentFiles = fileNumber
	}
	totalAmountToCopy := strconv.Itoa(fileNumber)
	fmt.Println("Files to copy: " + totalAmountToCopy)

	fileCounter := 0
	var wg sync.WaitGroup

	filepath.Walk(source, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			go processFile(root, info, &fileCounter, totalAmountToCopy, onePercentFiles, &wg)
		}
		return nil
	})

	wg.Wait()
	log("start special file treatment")
	postprocessImages(filepath.Join(destination, "JPG"), minEventDeltaDays, splitMonths)

	log("assure max file per folder number")
	limitFilesPerFolder(destination, maxNumberOfFilesPerFolder)
}

func validateDirectory(dir *string, prompt string) {
	for !isValidDirectory(*dir) {
		fmt.Print(prompt)
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			*dir = scanner.Text()
		}
	}
}

func processFile(root string, info os.FileInfo, fileCounter *int, totalAmountToCopy string, onePercentFiles int, wg *sync.WaitGroup) {
	defer wg.Done()

	extension := strings.ToUpper(filepath.Ext(info.Name())[1:])
	sourcePath := filepath.Join(root, info.Name())
	destinationDirectory := getDestinationDirectory(extension)

	createPath(destinationDirectory)
	fileName := getFileName(info, sourcePath, extension)

	destinationFile := filepath.Join(destinationDirectory, fileName)
	if !fileExists(destinationFile) {
		copyFile(sourcePath, destinationFile)
	}

	*fileCounter++
	if *fileCounter%onePercentFiles == 0 {
		log(fmt.Sprintf("%d / %s processed.", *fileCounter, totalAmountToCopy))
	}
}

func getDestinationDirectory(extension string) string {
	if extension != "" {
		return filepath.Join(destination, extension)
	}
	return filepath.Join(destination, "_NO_EXTENSION")
}

func getFileName(info os.FileInfo, sourcePath, extension string) string {
	if keepFilename {
		return info.Name()
	}
	if dateTimeFilename {
		return getDateTimeFileName(sourcePath, extension, info.Name())
	}
	return strconv.Itoa(fileCounter) + "." + strings.ToLower(extension)
}

func getDateTimeFileName(sourcePath, extension, fallbackName string) string {
	imageFile, err := os.Open(sourcePath)
	if err != nil {
		fmt.Printf("Could not open image %s: %v\n", fallbackName, err)
		return fallbackName
	}
	defer imageFile.Close()

	exifData, err := exif.SearchFileAndExtractExif(imageFile)
	if err == nil {
		creationTime := getMinimumCreationTime(exifData)
		if creationTime != nil {
			creationTimeStr := creationTime.Format("20060102_150405")
			fileName := creationTimeStr + "." + strings.ToLower(extension)
			index := 0
			for fileExists(filepath.Join(destination, extension, fileName)) {
				index++
				fileName = fmt.Sprintf("%s(%d).%s", creationTimeStr, index, strings.ToLower(extension))
			}
			return fileName
		}
	}
	return fallbackName
}
