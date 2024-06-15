package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.RUnlock()
		return false
	}
	res := atomic.CompareAndSwapInt64(fileBusy[path], int64(locked), int64(locked))
	busyIndexLock.RUnlock()
	return res
}

func lockFile(path string) {
	busyIndexLock.RLock()
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.RUnlock()
		busyIndexLock.Lock()
		fileBusy[path] = new(int64)
		atomic.StoreInt64(fileBusy[path], int64(locked))
		busyIndexLock.Unlock()
		return
	}
	busyIndexLock.RUnlock()
	// exponential back off on the spinlocc
	multiplier := 0
	for {
		busyIndexLock.RLock()
		if atomic.LoadInt64(fileBusy[path]) == int64(locked) {
			busyIndexLock.RUnlock()
			break
		}

		if atomic.CompareAndSwapInt64(fileBusy[path], int64(unlocked), int64(locked)) {
			busyIndexLock.RUnlock()
			break
		}
		busyIndexLock.RUnlock()
		multiplier++
		target := 100
		if multiplier > 10 {
			target = target * multiplier / 10
		}
		println("spinlock (" + path + "): sleeping for somewhere between 0 and " + strconv.Itoa(target) + " milliseconds...")
		entropy.RandSleepMS(target)
	}
}
func unlockFile(path string) {
	busyIndexLock.Lock()
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.Unlock()
		return
	}
	atomic.StoreInt64(fileBusy[path], int64(unlocked))
	busyIndexLock.Unlock()
}

const (
	colPre = "\033["
	colSuf = "m"
	red    = colPre + "31" + colSuf
	blue   = colPre + "36" + colSuf
	reset  = colPre + "0" + colSuf
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

	return creationTime, err
}

var bufs = pool.NewBufferFactory()

const kilo = 1024 * 1024

func getRawExif(imagePath string) ([]byte, error) {
	// fmt.Println("Getting raw exif for " + imagePath)
	/*	lockFile(imagePath)
		defer unlockFile(imagePath)*/

	buf := bufs.Get()
	buf.Reset()
	// 64KB
	buf.Grow(kilo * 128)

	f, err := os.Open(imagePath)
	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not open file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	n, err := buf.ReadFrom(io.LimitReader(f, kilo*256))

	if err != nil || n == 0 {
		if err == nil {
			err = fmt.Errorf("no data read")
		}
		// _, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not read file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	rawExif, err := exif.SearchAndExtractExifWithReader(buf)
	bufs.Put(buf)
	if len(rawExif) == 0 && err == nil {
		err = fmt.Errorf("no exif data found")
	}
	return rawExif, err
}

func postprocessImage(imagePath string, destinationRoot string, wg *sync.WaitGroup, splitByMonth bool) {
	defer wg.Done()

	rawExif, err := getRawExif(imagePath)

	if err != nil {
		// _, _ = os.Stdout.WriteString(fmt.Sprintf("Invalid EXIF data for %s: %s\n", imagePath, err.Error()))
		return
	}

	creationTime, err := getMinimumCreationTime(rawExif)
	if creationTime == nil || err != nil {
		//		fileInfo, err := os.Stat(imagePath)
		//		if err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf("Could not get file info for %s: %v\n", imagePath, err))
		return
		//		}
		//		creationT := fileInfo.ModTime()
		//		creationTime = &creationT
	}

	fmt.Printf("Creation time found for %s: %s\n", imagePath, creationTime.Format(time.RFC3339))

	writeImage(Image{Time: creationTime.Unix(), Path: imagePath}, destinationRoot)
}

func createPath(newPath string) {
	newPath = strings.TrimPrefix(newPath, ".")
	newPath = filepath.Clean(newPath)
	if newPath == "" {
		return
	}
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		if err := os.MkdirAll(newPath, 0755); err != nil {
			panic(fmt.Errorf("%s: %w", newPath, err))
		}
	}
}

func writeImage(image Image, destinationRoot string) {
	// minEventDelta := int64(minEventDeltaDays * 60 * 60 * 24)

	t := time.Unix(image.Time, 0)
	fileName := filepath.Base(image.Path)
	if t.IsZero() {
		println("skipping zero time for " + fileName)
		return
	}

	destinationDir := ""
	destinationFilePath := ""

	thenYear, thenMonth, thenDay := t.Date()
	nowYear, nowMonth, nowDay := time.Now().Date()

	if thenYear == nowYear && thenMonth == nowMonth && thenDay == nowDay {
		println("skipping date the same as today for " + fileName)
		return
	}

	destinationDir = filepath.Join(
		destinationRoot, strconv.Itoa(thenYear), thenMonth.String(), strconv.Itoa(thenDay),
	)
	_ = os.MkdirAll(destinationDir, 0755)

	destinationFilePath = filepath.Join(destinationDir, fileName)

	if destinationFilePath == "" {
		return
	}

	println("moving " + image.Path + " to " + destinationFilePath)
	os.Rename(image.Path, destinationFilePath)

}

func postprocessImages(imageDirectory string, destinationDirectory string, minEventDeltaDays int, splitByMonth bool) {
	var wg = &sync.WaitGroup{}

	spin := spinner.New(spinner.CharSets[34], 50*time.Millisecond, spinner.WithColor("blue"))

	fmt.Println("\n\n" + blue + "processing images..." + reset)
	fmt.Print("\n")

	spin.Start()

	if err := filepath.Walk(imageDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			_ = workers.Submit(func() { postprocessImage(path, destinationDirectory, wg, splitByMonth) })
		}
		return nil
	}); err != nil {
		_, _ = os.Stdout.WriteString(fmt.Sprintf("Error walking the path %s: %v\n", imageDirectory, err))
		spin.Stop()
		return
	} else {
		wg.Wait()
	}

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
	_, _ = os.Stdout.WriteString(fmt.Sprintf("\n\n%s: %s", time.Now().Format("15:04:05\n\n"), logString))
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

	if atomic.LoadInt64(&errCt) > 50 {
		fmt.Println("\n\n" + red + "Too many errors, exiting" + reset)
		fmt.Print("\n\n")
		os.Exit(1)
	}

	race := false

	if fileIsBusy(src) {
		println("race (src): " + src)
		race = true
	}

	if fileIsBusy(dst) {
		println("race (dst): " + src)
		race = true
	}

	if race {
		return
	}

	lockFile(dst)
	lockFile(src)
	defer func() {
		unlockFile(src)
		unlockFile(dst)
	}()

	handleErr := func() {
		atomic.AddInt64(&errCt, 1)
		time.Sleep(100 * time.Millisecond)
	}

	dstat, err := os.Stat(filepath.Dir(dst))
	switch {
	case err == nil && !dstat.IsDir():
		fmt.Println(red + "destination path (" + filepath.Dir(dst) + ") exists and isn't a directory!" + reset + "\n")
		handleErr()
		return
	case err == nil:
		break
	case err != nil && os.IsNotExist(err):
		if err = os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			if os.IsExist(err) {
				break
			}
			fmt.Println(red + "failed to create directory!" + reset)
			handleErr()
			return
		}
	}

	if err = os.Symlink(src, dst); err == nil {
		return
	}

	switch {
	case strings.Contains(err.Error(), ": file exists"):
		if existingLink, rlerr := os.Readlink(dst); rlerr == nil {
			if existingLink == src {
				return
			}
		}
	default:
		_, _ = os.Stdout.WriteString(fmt.Sprintf(red+"Could not create symlink %s"+reset+": %v\n", dst, err))
		handleErr()
	}
}

func limitFilesPerFolder(folder string, maxNumberOfFilesPerFolder int) {
	if err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if strings.Contains(path, "Processed_Images") {
			return nil
		}
		if info.IsDir() {
			filesInFolder, err := os.ReadDir(path)
			if err != nil {
				_, _ = os.Stdout.WriteString(fmt.Sprintf("%sCould not read directory %s%s: %v\n", red, path, err, reset))
				return err
			}
			adjustedFilesInFolder := []os.DirEntry{}
			for _, de := range filesInFolder {
				if de.IsDir() {
					continue
				}
				adjustedFilesInFolder = append(adjustedFilesInFolder, de)
			}
			filesInFolder = adjustedFilesInFolder
			if len(filesInFolder) <= maxNumberOfFilesPerFolder {
				return nil
			}
			totalLen := len(filesInFolder)
			folderTarget := totalLen / maxNumberOfFilesPerFolder
			for i := 1; i < folderTarget; i++ {
				if i > 1 && len(filesInFolder) >= totalLen {
					panic("files in folder not modified after renames")
				}
				newPath := filepath.Join(path, strconv.Itoa(i))
				println("creating directory " + newPath)
				if err := os.MkdirAll(newPath, 0755); err != nil {
					println(red + err.Error() + reset)
					return err
				}
				if len(filesInFolder) < maxNumberOfFilesPerFolder {
					break
				}
				toMove := len(filesInFolder) / maxNumberOfFilesPerFolder
				for i := 0; i < toMove; i++ {
					target := filepath.Join(path, filesInFolder[i].Name())
					newhome := filepath.Join(newPath, filesInFolder[i].Name())
					println("moving " + target + " to " + newhome)
					if err := os.Rename(target, newhome); err != nil {
						return err
					}
				}
				filesInFolder = filesInFolder[toMove+1:]
			}

		}
		return nil
	}); err != nil {
		if !os.IsNotExist(err) {
			fmt.Println("failed during rename process: " + err.Error())
			os.Exit(1)
		}
	}
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
		source, *destination, maxNumberOfFilesPerFolder, splitMonths)

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

	spin := spinner.New(spinner.CharSets[43], time.Duration(50)*time.Millisecond)

	spin.Start()

	if err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			_ = workers.Submit(func() { processFile(path, wg) })
		}
		return nil
	}); err == nil {
		fmt.Println("\n\n" + blue + "Waiting for all files to be processed" + reset)
		fmt.Print("\n")
		wg.Wait()
	} else {
		fmt.Println(red + "Error walking the path " + source + ": " + err.Error() + reset)
		os.Exit(1)
	}

	spin.Stop()

	log("start special file treatment")
	processPath := filepath.Join(*destination, "Processed_Images")
	_ = os.MkdirAll(processPath, 0755)
	postprocessImages(filepath.Join(*destination, "JPG"), processPath, minEventDeltaDays, splitMonths)

	log("assure max file per folder number")
	limitFilesPerFolder(*destination, maxNumberOfFilesPerFolder)
}

var magicBufs = sync.Pool{
	New: func() interface{} {
		return make([]byte, 261)
	},
}

func processFile(path string, wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Println("processing: " + path)

	// fmt.Println("extension for: " + path + " is " + extension)

	extension, destinationDirectory, isImage, isWAV := getDestinationDirectory(path)
	/*if isImage || isWAV {
		fmt.Printf("detected: wav=%t image=%t\n", isWAV, isImage)
	}*/
	if !strings.Contains(destinationDirectory, *destination) {
		panic(fmt.Errorf("destination directory %s is not a subdirectory of %s", destinationDirectory, *destination))
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

	if dateTimeFilename && !fastMode && !keepFilename && isImage {
		fmt.Println("getting filename for: " + sourcePath)
		fname2 := getDateTimeFileName(sourcePath, extension, fname)
		if fname2 != fname {
			fname = fname2
			fmt.Println("got filename: " + fname)
		}
	}

	if dateTimeFilename && !fastMode && !keepFilename && isWAV {
		newFilename, err := readWAV(sourcePath)
		if err == nil && newFilename != fname {
			fname = newFilename
			fmt.Println("got filename: " + fname)
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
			if creationTimeStr == "" {
				panic("bad time format")
			}
			fileName := creationTimeStr + "." + strings.ToLower(extension)

			return fileName
		}
	}
	return fallbackName
}
