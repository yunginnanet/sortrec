package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.tcp.direct/kayos/common/entropy"
	"git.tcp.direct/kayos/common/pool"
	"github.com/briandowns/spinner"
	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
	"github.com/go-audio/wav"
	"github.com/h2non/filetype"
	"github.com/panjf2000/ants/v2"
)

var workers, _ = ants.NewMultiPool(5, 25, ants.RoundRobin)

const unknownDateFolderName = "date-unknown"

type Image struct {
	Time int64
	Path string
}

var (
	maxNumberOfFilesPerFolder int
	splitMonths               bool
	source                    string
	destination               = new(string)
	keepFilename              bool
	dateTimeFilename          bool
	minEventDeltaDays         int
	fileCounter               int64
	fileBusy                  = make(map[string]*int64)
	busyIndexLock             sync.RWMutex
)

type lock int64

const (
	unlocked lock = 0
	locked   lock = 1
)

func fileIsBusy(path string) bool {
	busyIndexLock.RLock()
	flock, ok := fileBusy[path]
	busyIndexLock.RUnlock()
	if !ok {
		return false
	}
	return atomic.CompareAndSwapInt64(flock, int64(locked), int64(locked))
}

func lockFile(path string) {
	busyIndexLock.RLock()
	flock, ok := fileBusy[path]
	busyIndexLock.RUnlock()
	if !ok {
		busyIndexLock.Lock()
		fileBusy[path] = new(int64)
		flock = fileBusy[path]
		atomic.StoreInt64(flock, int64(locked))
		busyIndexLock.Unlock()
		return
	}
	// exponential back off on the spinlocc
	multiplier := 0
	for !atomic.CompareAndSwapInt64(flock, int64(unlocked), int64(locked)) {
		multiplier++
		target := 100
		if multiplier > 10 {
			target = target * multiplier / 10
		}
		entropy.RandSleepMS(target)
	}
}

func unlockFile(path string) {
	busyIndexLock.RLock()
	flock, ok := fileBusy[path]
	busyIndexLock.RUnlock()
	if !ok {
		return
	}
	atomic.StoreInt64(flock, int64(unlocked))
}

const (
	red   = "\033[31m"
	reset = "\033[0m"
)

func getMinimumCreationTime(rawExif []byte) (*time.Time, error) {
	var (
		creationTime *time.Time
	)

	fmt.Println("Getting minimum creation time")

	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create IFD mapping: "+reset+"%v\n", err))
		return nil, err
	}

	ti := exif.NewTagIndex()

	visitor := func(ite *exif.IfdTagEntry) (err error) {
		defer func() {
			if state := recover(); state != nil {
				_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"panic recovered, state: "+reset+"%v\n", state))
				return
			}
		}()

		tagId := ite.TagId()
		tagType := ite.TagType()

		switch {
		case (tagType == exifcommon.TypeAscii && (tagId == 0x9003 || tagId == 0x9004 || tagId == 0x0132)) ||
			ite.TagName() == "DateTimeOriginal":
			if v, e := ite.FormatFirst(); e == nil {
				if t, te := exifcommon.ParseExifFullTimestamp(v); te == nil {
					if t.IsZero() {
						return nil
					}
					if creationTime == nil || t.Before(*creationTime) {
						creationTime = &t
					}
				}
			}
		default:
			return nil
		}
		return nil
	}
	if _, _, err = exif.Visit(exifcommon.IfdStandardIfdIdentity, im, ti, rawExif, visitor, nil); err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not extract tags: "+reset+"%v\n", err))
		return nil, err
	}

	if creationTime == nil {
		err = fmt.Errorf("no creation time found")
	}

	if creationTime != nil {
		fmt.Println("Minimum creation time: " + creationTime.Format(time.RFC3339))
	}
	return creationTime, err
}

var bufs = pool.NewBufferFactory()

const kilo = 1024 * 1024

func getRawExif(imagePath string) ([]byte, error) {
	fmt.Println("Getting raw exif for " + imagePath)
	lockFile(imagePath)
	defer unlockFile(imagePath)

	buf := bufs.Get()
	buf.Reset()
	// 64KB
	buf.Grow(kilo * 64)

	f, err := os.Open(imagePath)
	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not open file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}
	size := kilo
	fstat, statErr := f.Stat()
	if statErr == nil && fstat.Size() < int64(kilo) {
		size = int(fstat.Size() / 2)
	}
	lr := io.LimitReader(f, int64(size))
	buf2 := bufs.Get()
	buf2.Reset()
	buf2.Grow(size / 2)
	var n int64
	n, err = io.CopyBuffer(buf, lr, buf2.Bytes()[0:buf2.Cap()])
	f.Close()
	bufs.Put(buf2)

	if err != nil || n == 0 {
		if err == nil {
			err = fmt.Errorf("no data read")
		}
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not read file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	rawExif, err := exif.SearchAndExtractExifWithReader(io.LimitReader(buf, n))
	bufs.Put(buf)
	if len(rawExif) == 0 && err == nil {
		err = fmt.Errorf("no exif data found")
	}
	return rawExif, err
}

func postprocessImage(imagePath string, wg *sync.WaitGroup, imagesChan chan<- Image) {
	defer wg.Done()

	rawExif, err := getRawExif(imagePath)

	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf("Invalid EXIF data for %s: %s\n", imagePath, err.Error()))
		return
	}

	creationTime, err := getMinimumCreationTime(rawExif)
	if creationTime == nil || err != nil {
		fileInfo, err := os.Stat(imagePath)
		if err != nil {
			_, _ = os.Stdout.WriteString(fmt.Sprintf("Could not get file info for %s: %v\n", imagePath, err))
			return
		}
		creationT := fileInfo.ModTime()
		creationTime = &creationT
	} else {
		fmt.Printf("Creation time found for %s: %s", imagePath, creationTime.Format(time.RFC3339))
	}

	imagesChan <- Image{creationTime.Unix(), imagePath}
}

func createPath(newPath string) {
	newPath = strings.TrimPrefix(newPath, ".")
	newPath = filepath.Clean(newPath)
	if newPath == "" {
		return
	}
	lockFile(newPath)
	defer unlockFile(newPath)
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		if err := os.MkdirAll(newPath, 0755); err != nil {
			panic(fmt.Errorf("%s: %w", newPath, err))
		}
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
			lockFile(destinationFilePath)
			lockFile(image.Path)
			os.Rename(image.Path, destinationFilePath)
			unlockFile(image.Path)
			unlockFile(destinationFilePath)
		} else {
			if fileExists(image.Path) {
				lockFile(image.Path)
				os.Remove(image.Path)
				unlockFile(image.Path)
			}
		}
	}
}

func postprocessImages(imageDirectory string, minEventDeltaDays int, splitByMonth bool) {
	var wg = &sync.WaitGroup{}
	imagesChan := make(chan Image, 50)

	spin := spinner.New(spinner.CharSets[34], 50*time.Millisecond)

	spin.Start()

	err := filepath.Walk(imageDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			_ = workers.Submit(func() { postprocessImage(path, wg, imagesChan) })
		}
		return nil
	})

	if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf("Error walking the path %s: %v\n", imageDirectory, err))
		spin.Stop()
		return
	}

	go func() {
		wg.Wait()
		close(imagesChan)
		spin.Stop()
	}()

	var images []Image
	for image := range imagesChan {
		images = append(images, image)
	}

	writeImages(images, imageDirectory, minEventDeltaDays, splitByMonth)
}

func getNumberOfFilesInFolderRecursively(startPath string) int {
	numberOfFiles := 0

	fmt.Println("Counting files in folder")

	s := spinner.New(spinner.CharSets[43], 100*time.Millisecond)

	fmt.Print()

	s.Start()

	filepath.Walk(startPath, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			_, _ = os.Stdout.WriteString(fmt.Sprintf("Could not walk path %s: %v\n", startPath, err))
			return err
		}

		if !info.IsDir() {
			numberOfFiles++
		}

		return nil
	})

	s.Stop()

	return numberOfFiles
}

func log(logString string) {
	_, _ = os.Stdout.WriteString(fmt.Sprintf("%s: %s", time.Now().Format("15:04:05\n"), logString))
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

var errCt int64 = 0

func symLink(src, dst string) {
	if fileIsBusy(src) || fileIsBusy(dst) {
		print(".")
	}
	lockFile(dst)
	lockFile(src)
	defer func() {
		unlockFile(src)
		unlockFile(dst)
	}()

	if err := os.Symlink(src, dst); err != nil {
		if errors.Is(err, os.ErrExist) {
			if err = os.Remove(dst); err != nil {
				_, _ = os.Stdout.WriteString(fmt.Sprintf("Could not remove existing symlink %s: %v\n", dst, err))
				atomic.AddInt64(&errCt, 1)
				return
			}
			if err = os.Symlink(src, dst); err == nil {
				return
			}
			_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create symlink %s"+reset+": %v\n", dst, err))
		} else if errors.Is(err, os.ErrNotExist) {
			if strings.Count(src, filepath.Base(src)) > 1 {
				src = filepath.Dir(src)
				src = strings.TrimSuffix(src, string(filepath.Separator))
				if err = os.Symlink(src, dst); err == nil {
					return
				}
				_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create symlink %s"+reset+": %v\n", dst, err))
			}
		} else {
			_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create symlink %s"+reset+": %v\n", dst, err))
		}

		oldErr := err
		if _, err = os.Stat(filepath.Dir(dst)); err != nil {
			fmt.Println("Creating directory " + filepath.Dir(dst))
			if err = os.MkdirAll(filepath.Dir(dst), 0755); err == nil || errors.Is(err, os.ErrExist) {
				if err = os.Symlink(src, dst); err == nil {
					return
				}
			}
		}

		if err = oldErr; err == nil {
			return
		}

		atomic.AddInt64(&errCt, 1)
		if atomic.LoadInt64(&errCt) > 50 {
			fmt.Println("\n\n" + red + "Last error: " + err.Error() + reset)
			fmt.Println("\n\n" + red + "Too many errors, exiting\n\n" + reset)
			os.Exit(1)
		}
	}
}

func limitFilesPerFolder(folder string, maxNumberOfFilesPerFolder int) {
	filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			filesInFolder, err := os.ReadDir(path)
			if err != nil {
				_, _ = os.Stdout.WriteString(fmt.Sprintf("%sCould not read directory %s%s: %v\n", red, path, err, reset))
				return nil
			}
			if len(filesInFolder) > maxNumberOfFilesPerFolder {
				numberOfSubfolders := (len(filesInFolder)-1)/maxNumberOfFilesPerFolder + 1
				for subFolderNumber := 1; subFolderNumber <= numberOfSubfolders; subFolderNumber++ {
					subFolderPath := filepath.Join(path, strconv.Itoa(subFolderNumber))
					createPath(subFolderPath)
				}
				fileCounterr := 1
				for _, file := range filesInFolder {
					if fileInfo, err := file.Info(); err == nil && fileInfo.Mode().IsRegular() {
						src := filepath.Join(path, filepath.Base(file.Name()))
						dst := strconv.Itoa((fileCounterr-1)/maxNumberOfFilesPerFolder + 1)
						fmt.Println("Moving " + src + " to " + filepath.Join(path, dst, filepath.Base(src)))
						os.Rename(src, filepath.Join(path, dst, filepath.Base(src)))
						fileCounterr++
					}
				}
			}
		}
		return nil
	})
}

func main() {
	var (
		workersFlag int
		poolsFlag   int
	)

	flag.StringVar(&source, "src", "", "Source directory with files recovered by Photorec")
	flag.StringVar(destination, "dest", "", "Destination directory to write sorted files to")
	flag.IntVar(&maxNumberOfFilesPerFolder, "max-per-dir", 500, "Maximum number of files per directory")
	flag.BoolVar(&splitMonths, "split-months", false, "Split JPEG files not only by year but by month as well")
	flag.BoolVar(&keepFilename, "keep_filename", false, "Keeps the original filenames when copying")
	flag.IntVar(&minEventDeltaDays, "min-event-delta", 4, "Minimum delta in days between two events")
	flag.BoolVar(&dateTimeFilename, "date_time_filename", false, "Sets the filename to the exif date and time if possible")
	flag.IntVar(&workersFlag, "workers", 5, "Number of workers to use")
	flag.IntVar(&poolsFlag, "pools", 25, "Number of pools to use")

	flag.Parse()

	if workersFlag > 0 && poolsFlag > 0 {
		workers, _ = ants.NewMultiPool(workersFlag, poolsFlag, ants.RoundRobin)

	}

	if source == "" || *destination == "" {
		flag.Usage()
		os.Exit(1)
	}

	if maxNumberOfFilesPerFolder == 0 {
		fmt.Println("max-per-dir must be greater than 0, setting it to 500 but flags might be fucked")
		maxNumberOfFilesPerFolder = 500
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

	/*	fileNumber := getNumberOfFilesInFolderRecursively(source)
		totalAmountToCopy := strconv.Itoa(fileNumber)
		fmt.Println("Files to copy: " + totalAmountToCopy)*/

	var wg = &sync.WaitGroup{}

	atomic.StoreInt64(&fileCounter, 0)

	spin := spinner.New(spinner.CharSets[43], time.Duration(50)*time.Millisecond)

	spin.Start()

	paths := make(chan string, poolsFlag*workersFlag)

	if err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			paths <- path
			_ = workers.Submit(func() { processFile(paths, wg) })
		}
		return nil
	}); err == nil {
		wg.Wait()
	} else {
		fmt.Println(red + "Error walking the path " + source + ": " + err.Error() + reset)
	}

	spin.Stop()

	log("start special file treatment")
	postprocessImages(filepath.Join(*destination, "JPG"), minEventDeltaDays, splitMonths)

	log("assure max file per folder number")
	limitFilesPerFolder(*destination, maxNumberOfFilesPerFolder)
}

var magicBufs = sync.Pool{
	New: func() interface{} {
		return make([]byte, 261)
	},
}

func processFile(paths chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var path string
	select {
	case path = <-paths:
		if path == "" {
			return
		}
	default:
		return
	}
	fmt.Println("processing: " + path)

	// fmt.Println("extension for: " + path + " is " + extension)

	extension, destinationDirectory, isImage, isWAV := getDestinationDirectory(path)
	/*if isImage || isWAV {
		fmt.Printf("detected: wav=%t image=%t\n", isWAV, isImage)
	}*/
	if !strings.Contains(destinationDirectory, *destination) {
		panic(fmt.Errorf("destination directory %s is not a subdirectory of %s", destinationDirectory, *destination))
		os.Exit(1)
	}
	// fmt.Println("destination directory: " + destinationDirectory)
	createPath(destinationDirectory)

	fname := filepath.Base(path)
	fileName := getFileName(fname, path, extension, isImage, isWAV)

	destinationFile := filepath.Join(destinationDirectory, fileName)
	_, _ = os.Stdout.WriteString(fmt.Sprintf("symlinking %s to %s\n", path, destinationFile))
	symLink(path, destinationFile)
}

var fastMode bool

func getDestinationDirectory(path string) (ext, dest string, isImage, isWAV bool) {
	// so we can skip exif data for non-media files
	// and use this for data recovery that doesn't append the correct extension
	ext = strings.TrimPrefix(strings.ToUpper(filepath.Ext(filepath.Base(path))), ".")
	if fastMode && ext != "" {
		return ext, filepath.Join(*destination, ext), ext == "JPG" || ext == "JPEG", ext == "WAV"
	}

	if fastMode && ext == "" {
		return "UNKNOWN", filepath.Join(*destination, "UNKNOWN"), false, false
	}

	b := magicBufs.Get().([]byte)
	b = b[:0]
	b = b[:261]

	f, err := os.Open(path)
	if err != nil {
		if ext == "" {
			ext = "UNKNOWN"
		}
		return ext, filepath.Join(*destination, ext), ext == "JPG" || ext == "JPEG", ext == "WAV"
	}

	n, rerr := f.Read(b)
	_ = f.Close()
	if rerr == nil && n > 0 {
		kind, kerr := filetype.Match(b)
		if kerr == nil {
			if kind != filetype.Unknown {
				fmt.Printf("File type: %s. MIME: %s\n", kind.Extension, kind.MIME.Value)
				ext = strings.ToUpper(kind.Extension)
				isWAV = kind.MIME.Subtype == "x-wav" || ext == "WAV"
			}
		}
	}

	if ext == "" {
		ext = "UNKNOWN"
	}

	dest = filepath.Join(*destination, ext)
	isImage = filetype.IsImage(b)
	magicBufs.Put(b)
	return ext, dest, isImage || ext == "JPG" || ext == "JPEG", isWAV || ext == "WAV"
}

func readWAV(path string) (string, error) {
	_, ogFilename := filepath.Split(path)

	f, err := os.Open(path)
	if err != nil {
		return ogFilename, fmt.Errorf("couldn't open %s: %s", path, err.Error())
	}
	defer f.Close()

	decoder := wav.NewDecoder(f)

	decoder.ReadMetadata()
	if decoder.Err() != nil {
		return ogFilename, decoder.Err()
	}

	if decoder.Metadata == nil {
		return ogFilename, nil
	}

	var newFilename string

	if decoder.Metadata.Title != "" {
		newFilename = decoder.Metadata.Title
	}

	if decoder.Metadata.Artist != "" {
		if newFilename != "" {
			newFilename = decoder.Metadata.Artist + " - " + newFilename
		} else {
			newFilename = decoder.Metadata.Artist + " - " + ogFilename
		}
	}

	if decoder.Metadata.Software != "" {
		if newFilename == "" {
			newFilename = filepath.Join(decoder.Metadata.Software, ogFilename)
		} else {
			newFilename = filepath.Join(decoder.Metadata.Software, newFilename)
		}
	}

	if newFilename == "" {
		err = fmt.Errorf("no metadata found")
	} else {
		fmt.Printf("got WAV metadata for %s: %s\n", path, newFilename)
	}

	return newFilename, err
}

func getFileName(fname string, sourcePath, extension string, isImage, isWAV bool) string {
	fname = filepath.Base(fname)
	if (keepFilename || fastMode || (!isImage && !isWAV) || !dateTimeFilename) && fname != "" {
		return fname
	}

	fmt.Println("getting filename for: " + sourcePath)

	if dateTimeFilename && !fastMode && !keepFilename && isImage {
		fname = getDateTimeFileName(sourcePath, extension, fname)
	}

	if dateTimeFilename && !fastMode && !keepFilename && isWAV && !isImage {
		newFilename, err := readWAV(sourcePath)
		if err == nil {
			fname = newFilename
		}
	}

	if !isImage && !isWAV {
		return fname
	}

	if fname == "" {
		fstat, err := os.Stat(sourcePath)
		if err == nil {
			fname = fstat.ModTime().Format("2006-01-02_15-04-05")
		} else {
			fname = "unknown"
		}
		fname = fname + "." + strings.ToLower(extension)
	}

	index := 0
	for fileExists(filepath.Join(*destination, extension, fname)) {
		index++
		fbase := strings.TrimSuffix(fname, filepath.Ext(fname))
		if strings.Contains(fbase, "_") {
			fbase = strings.Split(fbase, "_")[0]
		}
		fname = fmt.Sprintf("%s_%d%s", fbase, index, filepath.Ext(fname))
	}

	return fname
}

func getDateTimeFileName(sourcePath, extension, fallbackName string) string {
	rawExifData, err := getRawExif(sourcePath)
	if err == nil {
		creationTime, err := getMinimumCreationTime(rawExifData)
		if creationTime != nil && err == nil {
			creationTimeStr := creationTime.Format("2006-01-02_15-04-05")
			fileName := creationTimeStr + "." + strings.ToLower(extension)

			return fileName
		}
	}
	return fallbackName
}
