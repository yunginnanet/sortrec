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

	"git.tcp.direct/kayos/common/pool"
	"github.com/briandowns/spinner"
	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
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
	destination               string
	keepFilename              bool
	dateTimeFilename          bool
	minEventDeltaDays         int
	fileCounter               int64
	fileBusy                  = make(map[string]*sync.Mutex)
	busyLock                  sync.RWMutex
)

func fileIsBusy(path string) bool {
	busyLock.RLock()
	flock, ok := fileBusy[path]
	if !ok {
		busyLock.RUnlock()
		return false
	}
	if !flock.TryLock() {
		busyLock.RUnlock()
		return true
	}
	flock.Unlock()
	busyLock.RUnlock()
	return false
}

func lockFile(path string) {
	busyLock.RLock()
	if flock, ok := fileBusy[path]; ok {
		busyLock.RUnlock()
		flock.Lock()
		return
	}
	busyLock.RUnlock()
	busyLock.Lock()
	mu := &sync.Mutex{}
	mu.Lock()
	fileBusy[path] = mu
	busyLock.Unlock()
}

func unlockFile(path string) {
	busyLock.RLock()
	flock, ok := fileBusy[path]
	if !ok {
		busyLock.RUnlock()
		return
	}
	busyLock.RUnlock()
	if flock.TryLock() {
		flock.Unlock()
		return
	}
	flock.Unlock()
}

const (
	red   = "\033[31m"
	reset = "\033[0m"
)

func getMinimumCreationTime(rawExif []byte) (*time.Time, error) {
	var (
		creationTime *time.Time
	)

	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create IFD mapping: "+reset+"%v\n", err))
		return nil, err
	}

	ti := exif.NewTagIndex()

	visitor := func(ite *exif.IfdTagEntry) (err error) {
		if creationTime != nil {
			return nil
		}
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
					creationTime = &t
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

	return creationTime, err
}

var bufs = pool.NewBufferFactory()

func getRawExif(imagePath string) ([]byte, error) {
	lockFile(imagePath)
	defer unlockFile(imagePath)

	f, err := os.Open(imagePath)
	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not open image %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	b := bufs.Get()
	b.Reset()
	_ = b.Grow(1024 * 1024)
	pr, pw := io.Pipe()
	doneChan := make(chan struct{})

	go func() {
		defer close(doneChan)

		n, e := io.CopyBuffer(pw, f, b.Bytes()[:b.Cap()])
		if e != nil {
			_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not read image %s: "+reset+"%v\n", imagePath, e))
			return
		}
		if n == 0 {
			_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"empty image %s"+reset+"\n", imagePath))
			return
		}
		_ = pw.Close()
	}()

	go func() {
		<-doneChan
		bufs.MustPut(b)
		_ = f.Close()
	}()

	rawExif, err := exif.SearchAndExtractExifWithReader(pr)
	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not extract exif data for %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}
	return rawExif, nil
}

func postprocessImage(imagePath string, wg *sync.WaitGroup, imagesChan chan<- Image) {
	defer wg.Done()

	rawExif, err := getRawExif(imagePath)

	if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf("Invalid EXIF data for %s: %s\n", imagePath, err.Error()))
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
	}

	imagesChan <- Image{creationTime.Unix(), imagePath}
}

func createPath(newPath string) {
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		if err := os.MkdirAll(newPath, os.ModePerm); err != nil {
			panic(err)
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
	var wg sync.WaitGroup
	imagesChan := make(chan Image)

	spinner := spinner.New(spinner.CharSets[34], 50*time.Millisecond)

	spinner.Start()

	go func() {
		wg.Wait()
		close(imagesChan)
		spinner.Stop()
	}()

	err := filepath.Walk(imageDirectory, func(root string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			fdir, fname := filepath.Split(root)
			if fname != info.Name() {
				fname = filepath.Join(root, info.Name())
				if strings.Count(fdir, info.Name()) > 1 {
					fname = root
				}
			}
			if fileIsBusy(fname) {
				_, _ = os.Stdout.WriteString(fmt.Sprintf("File %s is busy, skipping\n", fname))
				return nil
			}
			wg.Add(1)
			_ = workers.Submit(func() { postprocessImage(fname, &wg, imagesChan) })
		}
		return nil
	})
	if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf("Error walking the path %s: %v\n", imageDirectory, err))
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
		atomic.AddInt64(&errCt, 1)
		if atomic.LoadInt64(&errCt) > 50 {
			fmt.Println("Too many errors, exiting")
			os.Exit(1)
		}
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
				_, _ = os.Stdout.WriteString(fmt.Sprintf("Could not read directory %s: %v\n", dirPath, err))
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
	var (
		workersFlags int
		poolsFlags   int
	)

	flag.StringVar(&source, "src", "", "Source directory with files recovered by Photorec")
	flag.StringVar(&destination, "dest", "", "Destination directory to write sorted files to")
	flag.IntVar(&maxNumberOfFilesPerFolder, "max-per-dir", 500, "Maximum number of files per directory")
	flag.BoolVar(&splitMonths, "split-months", false, "Split JPEG files not only by year but by month as well")
	flag.BoolVar(&keepFilename, "keep_filename", false, "Keeps the original filenames when copying")
	flag.IntVar(&minEventDeltaDays, "min-event-delta", 4, "Minimum delta in days between two events")
	flag.BoolVar(&dateTimeFilename, "date_time_filename", false, "Sets the filename to the exif date and time if possible")
	flag.IntVar(&workersFlags, "workers", 5, "Number of workers to use")
	flag.IntVar(&poolsFlags, "pools", 25, "Number of pools to use")

	flag.Parse()

	if workersFlags > 0 && poolsFlags > 0 {
		workers, _ = ants.NewMultiPool(workersFlags, poolsFlags, ants.RoundRobin)

	}

	if source == "" || destination == "" {
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

	fileNumber := getNumberOfFilesInFolderRecursively(source)
	totalAmountToCopy := strconv.Itoa(fileNumber)
	fmt.Println("Files to copy: " + totalAmountToCopy)

	var wg = &sync.WaitGroup{}

	atomic.StoreInt64(&fileCounter, 0)

	spin := spinner.New(spinner.CharSets[43], time.Duration(50)*time.Millisecond)

	spin.Start()

	filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			_ = workers.Submit(func() { processFile(path, wg) })
		}
		return nil
	})

	wg.Wait()

	spin.Stop()

	log("start special file treatment")
	postprocessImages(filepath.Join(destination, "JPG"), minEventDeltaDays, splitMonths)

	log("assure max file per folder number")
	limitFilesPerFolder(destination, maxNumberOfFilesPerFolder)
}

var magicBufs = sync.Pool{
	New: func() interface{} {
		return make([]byte, 261)
	},
}

var (
	mediaFiles = make([]string, 0)
	mediaLock  = sync.Mutex{}
)

func processFile(path string, wg *sync.WaitGroup) {
	fmt.Println("processing: " + path)

	defer wg.Done()

	var destinationDirectory string
	var extension = strings.TrimPrefix(filepath.Ext(filepath.Base(path)), ".")

	f, err := os.Open(path)
	if err == nil {
		b := magicBufs.Get().([]byte)
		b = b[:0]
		b = b[:261]
		n, rerr := f.Read(b)
		_ = f.Close()
		if rerr == nil && n > 0 {
			kind, kerr := filetype.Match(b)
			if kerr == nil {
				if kind != filetype.Unknown {
					extension = kind.Extension
				}
				if kind.MIME.Type == "image" || kind.MIME.Type == "video" {
					mediaLock.Lock()
					mediaFiles = append(mediaFiles, path)
					mediaLock.Unlock()
				}
			}
		}
		magicBufs.Put(b)
	}

	// fmt.Println("extension for: " + path + " is " + extension)

	destinationDirectory = getDestinationDirectory(extension)
	createPath(destinationDirectory)

	fname := filepath.Base(path)
	fileName := getFileName(fname, path, extension)

	destinationFile := filepath.Join(destinationDirectory, fileName)
	_, _ = os.Stdout.WriteString(fmt.Sprintf("symlinking %s to %s\n", path, destinationFile))
	symLink(path, destinationFile)
}

func getDestinationDirectory(extension string) string {
	if extension != "" {
		return filepath.Join(destination, extension)
	}
	return filepath.Join(destination, "_NO_EXTENSION")
}

func getFileName(fname string, sourcePath, extension string) string {
	if keepFilename {
		return fname
	}
	if dateTimeFilename {
		return getDateTimeFileName(sourcePath, extension, fname)
	}
	return strconv.Itoa(int(atomic.LoadInt64(&fileCounter))) + "." + strings.ToLower(extension)
}

func getDateTimeFileName(sourcePath, extension, fallbackName string) string {
	rawExifData, err := getRawExif(sourcePath)
	if err == nil {
		creationTime, err := getMinimumCreationTime(rawExifData)
		if creationTime != nil && err == nil {
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
